package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeService;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Failure detection based on simple heartbeat protocol. Every member periodically multicasts a heartbeat.
 * Every member also maintains a table of all members (minus itself). When data or a heartbeat from P is received,
 * we reset the timestamp for P to the current time. Periodically, we check for expired members, and suspect those.</p>
 * Reduced number of messages exchanged on suspect event: https://jira.jboss.org/browse/JGRP-1241
 * 
 * @author Bela Ban
 */
@MBean(description="Failure detection based on simple heartbeat protocol")
public class FD_ALL extends Protocol {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Interval at which a HEARTBEAT is sent to the cluster")
    protected long                                   interval=8000;

    @Property(description="Timeout after which a node P is suspected if neither a heartbeat nor data were received from P")
    protected long                                   timeout=40000;

    @Property(description="Interval at which the HEARTBEAT timeouts are checked")
    protected long                                   timeout_check_interval=2000;

    @Property(description="Treat messages received from members as heartbeats. Note that this means we're updating " +
            "a value in a hashmap every time a message is passing up the stack through FD_ALL, which is costly. Default is false")
    protected boolean                                msg_counts_as_heartbeat=false;

    @Property(description="Uses TimeService to get the current time rather than System.currentTimeMillis. Might get " +
      "removed soon, don't use !")
    protected boolean                                use_time_service=true;

    /* ---------------------------------------------   JMX      ------------------------------------------------------ */
    @ManagedAttribute(description="Number of heartbeats sent")
    protected int                                    num_heartbeats_sent;

    @ManagedAttribute(description="Number of heartbeats received")
    protected int                                    num_heartbeats_received;

    @ManagedAttribute(description="Number of suspected events received")
    protected int                                    num_suspect_events;

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    // Map of addresses and timestamps of last updates (ns)
    protected final ConcurrentMap<Address, Long>     timestamps=Util.createConcurrentMap();

    protected Address                                local_addr;
    
    protected final List<Address>                    members=new ArrayList<>();

    protected final Set<Address>                     suspected_mbrs=new HashSet<>();

    @ManagedAttribute(description="Shows whether there are currently any suspected members")
    protected volatile boolean                       has_suspected_mbrs;

    protected TimeScheduler                          timer;

    protected TimeService                            time_service;

    // task which multicasts HEARTBEAT message after 'interval' ms
    @GuardedBy("lock")
    protected Future<?>                              heartbeat_sender_future;

    // task which checks for members exceeding timeout and suspects them
    @GuardedBy("lock")
    protected Future<?>                              timeout_checker_future;

    protected final BoundedList<Tuple<Address,Long>> suspect_history=new BoundedList<>(20);
    
    protected final Lock                             lock=new ReentrantLock();




    public FD_ALL() {}


    @ManagedAttribute(description="Member address")
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute(description="Lists members of a cluster")
    public String getMembers() {return Util.printListWithDelimiter(members, ",");}
    @ManagedAttribute(description="Currently suspected members")
    public synchronized String getSuspectedMembers() {return suspected_mbrs.toString();}
    public int getHeartbeatsSent() {return num_heartbeats_sent;}
    public int getHeartbeatsReceived() {return num_heartbeats_received;}
    public int getSuspectEventsSent() {return num_suspect_events;}
    public long getTimeout() {return timeout;}
    public void setTimeout(long timeout) {this.timeout=timeout;}
    public long getTimeoutCheckInterval() {return timeout_check_interval;}
    public void setTimeoutCheckInterval(long timeout_check_interval) {this.timeout_check_interval=timeout_check_interval;}
    public long getInterval() {return interval;}
    public void setInterval(long interval) {this.interval=interval;}

    @ManagedAttribute(description="Are heartbeat tasks running")
    public boolean isRunning() {
        lock.lock();
        try{
            return isTimeoutCheckerRunning() && isHeartbeatSenderRunning();
        }
        finally{
            lock.unlock();
        }        
    }

    @ManagedOperation(description="Prints suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<Address,Long> tmp: suspect_history) {
            sb.append(new Date(tmp.getVal2())).append(": ").append(tmp.getVal1()).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Prints timestamps")
    public String printTimestamps() {
        return _printTimestamps();
    }

    @ManagedOperation(description="Stops checking for crashed members")
    public void stopFailureDetection() {
        stopTimeoutChecker();
    }

    @ManagedOperation(description="Resumes checking for crashed members")
    public void startFailureDetection() {
        startTimeoutChecker();
    }
  

    public void resetStats() {
        num_heartbeats_sent=num_heartbeats_received=num_suspect_events=0;
        suspect_history.clear();
    }


    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer not set");
        time_service=getTransport().getTimeService();
        if(time_service == null)
            log.warn("%s: time service is not available, using System.currentTimeMillis() instead", local_addr);
        else {
            if(time_service.interval() > timeout) {
                log.warn("%s: interval of time service (%d) is greater than timeout (%d), disabling time service",
                         local_addr, time_service.interval(), timeout);
                use_time_service=false;
            }
        }
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }


    public synchronized void stop() {
        stopHeartbeatSender();
        stopTimeoutChecker();
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address sender=msg.getSrc();

                Header hdr=msg.getHeader(this.id);
                if(hdr != null) {
                    update(sender); // updates the heartbeat entry for 'sender'
                    num_heartbeats_received++;
                    unsuspect(sender);
                    return null; // consume heartbeat message, do not pass to the layer above
                }
                else if(msg_counts_as_heartbeat) {
                    // message did not originate from FD_ALL layer, but still count as heartbeat
                    update(sender); // update when data is received too ? maybe a bit costly
                    if(has_suspected_mbrs)
                        unsuspect(sender);
                }
                break; // pass message to the layer above
        }
        return up_prot.up(evt); // pass up to the layer above us
    }


    public void up(MessageBatch batch) {
        Collection<Message> msgs=batch.getMatchingMessages(id, true);
        if((msgs != null && !msgs.isEmpty()) || msg_counts_as_heartbeat) {
            update(batch.sender());
            num_heartbeats_received++;
            if(has_suspected_mbrs)
                unsuspect(batch.sender());
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                down_prot.down(evt);
                View v=(View)evt.getArg();
                handleViewChange(v);
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.UNSUSPECT:
                Address mbr=(Address)evt.getArg();
                unsuspect(mbr);
                update(mbr);
                break;
        }
        return down_prot.down(evt);
    }

    protected void startTimeoutChecker() {
        lock.lock();
        try {
            if(!isTimeoutCheckerRunning()) {
                timeout_checker_future=timer.scheduleWithFixedDelay(new TimeoutChecker(),timeout_check_interval,timeout_check_interval, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void stopTimeoutChecker() {
         lock.lock();
         try {
             if(timeout_checker_future != null) {
                 timeout_checker_future.cancel(true);
                 timeout_checker_future=null;
             }
         }
         finally {
             lock.unlock();
         }
     }


    protected void startHeartbeatSender() {
        lock.lock();
        try {
            if(!isHeartbeatSenderRunning())
                heartbeat_sender_future=timer.scheduleWithFixedDelay(new HeartbeatSender(), interval, interval, TimeUnit.MILLISECONDS);
        }
        finally {
            lock.unlock();
        }
    }

     protected void stopHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender_future != null) {
                heartbeat_sender_future.cancel(true);
                heartbeat_sender_future=null;
            }
        }
        finally {
            lock.unlock();
        }
    }
     
    protected boolean isTimeoutCheckerRunning() {
        return timeout_checker_future != null && !timeout_checker_future.isDone();
    }
     
    protected boolean isHeartbeatSenderRunning() {
        return heartbeat_sender_future != null && !heartbeat_sender_future.isDone();
    }


    protected void update(Address sender) {
        if(sender != null && !sender.equals(local_addr))
            timestamps.put(sender, getTimestamp());
    }

    protected void addIfAbsent(Address mbr) {
        if(mbr != null && !mbr.equals(local_addr))
            timestamps.putIfAbsent(mbr, getTimestamp());
    }

    protected long getTimestamp() {
        return use_time_service && time_service != null? time_service.timestamp() : System.nanoTime();
    }


    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();

        synchronized(this) {
            members.clear();
            members.addAll(mbrs);
            if(suspected_mbrs.retainAll(mbrs))
                has_suspected_mbrs=!suspected_mbrs.isEmpty();
            timestamps.keySet().retainAll(mbrs);
        }

        for(Address member: mbrs) // JGRP-1856
            addIfAbsent(member);

        if(mbrs.size() > 1) {
            startHeartbeatSender();
            startTimeoutChecker();
        }
        else {
            stopHeartbeatSender();
            stopTimeoutChecker();
        }
    }



    protected String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        long current_time=getTimestamp();
        for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,Long> entry=it.next();
            sb.append(entry.getKey()).append(": ");
            sb.append(TimeUnit.SECONDS.convert (current_time - entry.getValue(), TimeUnit.NANOSECONDS)).append(" secs old\n");
        }
        return sb.toString();
    }

    protected void suspect(List<Address> suspects) {
        if(suspects == null || suspects.isEmpty())
            return;

        num_suspect_events+=suspects.size();

        final List<Address> eligible_mbrs=new ArrayList<>();
        synchronized(this) {
            for(Address suspect: suspects) {
                suspect_history.add(new Tuple<>(suspect, System.currentTimeMillis())); // need wall clock time
                suspected_mbrs.add(suspect);
            }
            eligible_mbrs.addAll(members);
            eligible_mbrs.removeAll(suspected_mbrs);
            has_suspected_mbrs=!suspected_mbrs.isEmpty();
        }

        // Check if we're coord, then send up the stack
        if(local_addr != null && !eligible_mbrs.isEmpty()) {
            Address first=eligible_mbrs.get(0);
            if(local_addr.equals(first)) {
                log.debug("suspecting " + getSuspectedMembers());
                for(Address suspect: suspects) {
                    up_prot.up(new Event(Event.SUSPECT, suspect));
                    down_prot.down(new Event(Event.SUSPECT, suspect));
                }
            }
        }
    }

    /**
     * Removes mbr from suspected_mbrs and sends a UNSUSPECT event up and down the stack
     * @param mbr
     * @return true if the member was removed from suspected_mbrs, otherwise false
     */
    protected boolean unsuspect(Address mbr) {
        if(mbr == null) return false;
        boolean do_unsuspect;
        synchronized(this) {
            do_unsuspect=!suspected_mbrs.isEmpty() && suspected_mbrs.remove(mbr);
            if(do_unsuspect)
                has_suspected_mbrs=!suspected_mbrs.isEmpty();
        }
        if(do_unsuspect) {
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
        }
        return do_unsuspect;
    }


    public static class HeartbeatHeader extends Header {
        public HeartbeatHeader() {}
        public String toString() {return "heartbeat";}
        public int size() {return 0;}
        public void writeTo(DataOutput out) throws Exception {}
        public void readFrom(DataInput in) throws Exception {}
    }


    /**
     * Class which periodically multicasts a HEARTBEAT message to the cluster
     */
    class HeartbeatSender implements Runnable {
        public void run() {
            Message heartbeat=new Message().setFlag(Message.Flag.INTERNAL).putHeader(id, new HeartbeatHeader());
            down_prot.down(new Event(Event.MSG, heartbeat));
            num_heartbeats_sent++;
        }

        public String toString() {
            return FD_ALL.class.getSimpleName() + ": " + getClass().getSimpleName();
        }
    }


    class TimeoutChecker implements Runnable {

        public void run() {                        
            List<Address> suspects=new LinkedList<>();
            long current_time=getTimestamp(), diff;
            for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
                Entry<Address,Long> entry=it.next();
                Address key=entry.getKey();
                Long val=entry.getValue();
                if(val == null) {
                    it.remove();
                    continue;
                }
                diff=TimeUnit.MILLISECONDS.convert(current_time - val, TimeUnit.NANOSECONDS);
                if(diff > timeout) {
                    log.debug("haven't received a heartbeat from " + key + " for " + diff +
                                " ms, adding it to suspect list");
                    suspects.add(key);
                }
            }
            if(!suspects.isEmpty())
                suspect(suspects);
        }

        public String toString() {
            return FD_ALL.class.getSimpleName() + ": " + getClass().getSimpleName() +
              " (interval=" + timeout_check_interval + " ms)";
        }
    }
}

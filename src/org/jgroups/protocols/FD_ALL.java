package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.Map.Entry;
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
    long interval=8000;

    @Property(description="Timeout after which a node P is suspected if neither a heartbeat nor data were received from P")
    long timeout=40000;

    @Property(description="Interval at which the HEARTBEAT timeouts are checked")
    long timeout_check_interval=2000;

    @Property(description="Treat messages received from members as heartbeats. Note that this means we're updating " +
            "a value in a hashmap every time a message is passing up the stack through FD_ALL, which is costly. Default is false")
    boolean msg_counts_as_heartbeat=false;

    /* ---------------------------------------------   JMX      ------------------------------------------------------ */
    @ManagedAttribute(description="Number of heartbeats sent")
    protected int num_heartbeats_sent;

    @ManagedAttribute(description="Number of heartbeats received")
    protected int num_heartbeats_received=0;

    @ManagedAttribute(description="Number of suspected events received")
    protected int num_suspect_events=0;

    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    // Map of addresses and timestamps of last updates
    protected final ConcurrentMap<Address, Long>     timestamps=Util.createConcurrentMap();

    private Address local_addr=null;
    
    private final List<Address> members=new ArrayList<Address>();

    protected final Set<Address> suspected_mbrs=new HashSet<Address>();

    @ManagedAttribute(description="Shows whether there are currently any suspected members")
    protected volatile boolean has_suspected_mbrs;

    private TimeScheduler timer=null;

    // task which multicasts HEARTBEAT message after 'interval' ms
    @GuardedBy("lock")
    private Future<?> heartbeat_sender_future=null;

    // task which checks for members exceeding timeout and suspects them
    @GuardedBy("lock")
    private Future<?> timeout_checker_future=null;    

    private final BoundedList<Tuple<Address,Long>> suspect_history=new BoundedList<Tuple<Address,Long>>(20);
    
    private final Lock lock=new ReentrantLock();




    public FD_ALL() {}


    @ManagedAttribute(description="Member address")
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute(description="Lists members of a cluster")
    public String getMembers() {return members.toString();}
    @ManagedAttribute(description="Currently suspected members")
    public String getSuspectedMembers() {return suspected_mbrs.toString();}
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
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }


    public void stop() {
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

    private void startTimeoutChecker() {
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

    private void stopTimeoutChecker() {
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


    private void startHeartbeatSender() {
        lock.lock();
        try {
            if(!isHeartbeatSenderRunning()) {
                heartbeat_sender_future=timer.scheduleWithFixedDelay(new HeartbeatSender(), interval, interval, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            lock.unlock();
        }
    }

     private void stopHeartbeatSender() {
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
     
    private boolean isTimeoutCheckerRunning() {
        return timeout_checker_future != null && !timeout_checker_future.isDone();
    }
     
    private boolean isHeartbeatSenderRunning() {
        return heartbeat_sender_future != null && !heartbeat_sender_future.isDone();
    }


    private void update(Address sender) {
        if(sender != null && !sender.equals(local_addr))
            timestamps.put(sender, getTimestamp());
    }

    protected void addIfAbsent(Address mbr) {
        if(mbr != null && !mbr.equals(local_addr))
            timestamps.putIfAbsent(mbr, getTimestamp());
    }

    protected static long getTimestamp() {
        return System.currentTimeMillis();
    }


    private void handleViewChange(View v) {
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



    private String _printTimestamps() {
        StringBuilder sb=new StringBuilder();
        long current_time=System.currentTimeMillis();
        for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
            Entry<Address,Long> entry=it.next();
            sb.append(entry.getKey()).append(": ");
            sb.append(current_time - entry.getValue()).append(" ms old\n");
        }
        return sb.toString();
    }

    protected void suspect(List<Address> suspects) {
        if(suspects == null || suspects.isEmpty())
            return;

        num_suspect_events+=suspects.size();

        final List<Address> eligible_mbrs=new ArrayList<Address>();
        synchronized(this) {
            for(Address suspect: suspects) {
                suspect_history.add(new Tuple<Address,Long>(suspect, System.currentTimeMillis()));
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
                if(log.isDebugEnabled())
                    log.debug("suspecting " + suspected_mbrs);
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
            List<Address> suspects=new LinkedList<Address>();
            long current_time=System.currentTimeMillis(), diff;
            for(Iterator<Entry<Address,Long>> it=timestamps.entrySet().iterator(); it.hasNext();) {
                Entry<Address,Long> entry=it.next();
                Address key=entry.getKey();
                Long val=entry.getValue();
                if(val == null) {
                    it.remove();
                    continue;
                }
                diff=current_time - val;
                if(diff > timeout) {
                    if(log.isDebugEnabled())
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

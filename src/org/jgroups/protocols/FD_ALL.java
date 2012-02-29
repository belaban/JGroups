package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
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
@DeprecatedProperty(names={"shun"})
public class FD_ALL extends Protocol {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Interval in which a HEARTBEAT is sent to the cluster")
    long interval=3000;

    @Property(description="Timeout after which a node P is suspected if neither a heartbeat nor data were received from P")
    long timeout=10000;

    @Property(description="Interval in which HEARTBEAT timeout is checked")
    long interval_checker = 2000;
    
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
    private final Map<Address, Long> timestamps=Util.createConcurrentMap();

    private Address local_addr=null;
    
    private final List<Address> members=new ArrayList<Address>();

    protected final Set<Address> suspected_mbrs=new HashSet<Address>();

    private TimeScheduler timer=null;

    // task which multicasts HEARTBEAT message after 'interval' ms
    @GuardedBy("lock")
    private Future<?> heartbeat_sender_future=null;

    // task which checks for members exceeding timeout and suspects them
    @GuardedBy("lock")
    private Future<?> timeout_checker_future=null;    

    private final BoundedList<Address> suspect_history=new BoundedList<Address>(20);
    
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
    public long getIntervalChecker() {return interval_checker;}
    public void setIntervalChecker(long interval_checker) {this.interval_checker=interval_checker;}
    public long getInterval() {return interval;}
    public void setInterval(long interval) {this.interval=interval;}
    @Deprecated
    public static boolean isShun() {return false;}
    @Deprecated
    public void setShun(boolean flag) {}
    
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
        for(Address tmp: suspect_history) {
            sb.append(new Date()).append(": ").append(tmp).append("\n");
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
    }


    public void stop() {
        stopHeartbeatSender();
        stopTimeoutChecker();
        suspected_mbrs.clear();
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
                    return null; // consume heartbeat message, do not pass to the layer above
                } else if(msg_counts_as_heartbeat) {
                    // message did not originate from FD_ALL layer, but still count as heartbeat
                    update(sender); // update when data is received too ? maybe a bit costly
                }
                break; // pass message to the layer above
        }
        return up_prot.up(evt); // pass up to the layer above us
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
        }
        return down_prot.down(evt);
    }

    private void startTimeoutChecker() {
        lock.lock();
        try {
            if(!isTimeoutCheckerRunning()) {
                timeout_checker_future=timer.scheduleWithFixedDelay(new TimeoutChecker(), interval_checker, interval_checker, TimeUnit.MILLISECONDS);
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
            timestamps.put(sender, System.currentTimeMillis());
    }


    private void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();

        synchronized(this) {
            members.clear();
            members.addAll(mbrs);
            suspected_mbrs.retainAll(mbrs);
            timestamps.keySet().retainAll(mbrs);
        }

        for(Address member: mbrs)
            update(member);

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
            sb.append(current_time - entry.getValue().longValue()).append(" ms old\n");
        }
        return sb.toString();
    }

    void suspect(List<Address> suspects) {
        if(suspects == null)
            return;

        num_suspect_events+=suspects.size();

        final List<Address> eligible_mbrs=new ArrayList<Address>();
        synchronized(this) {
            for(Address suspect: suspects) {
                suspect_history.add(suspect);
                suspected_mbrs.add(suspect);
            }
            eligible_mbrs.addAll(members);
            eligible_mbrs.removeAll(suspected_mbrs);
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


    public static class HeartbeatHeader extends Header {
        public HeartbeatHeader() {}
        public String toString() {return "heartbeat";}
        public int size() {return 0;}
        public void writeTo(DataOutputStream out) throws IOException {}
        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {}
    }


    /**
     * Class which periodically multicasts a HEARTBEAT message to the cluster
     */
    class HeartbeatSender implements Runnable {
        public void run() {
            Message heartbeat=new Message(); // send to all
            heartbeat.setFlag(Message.OOB);
            heartbeat.putHeader(id, new HeartbeatHeader());
            down_prot.down(new Event(Event.MSG, heartbeat));
            num_heartbeats_sent++;
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
                diff=current_time - val.longValue();
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
    }
}

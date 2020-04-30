package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Bela Ban
 * @since  5.0.0
 */
@MBean(description="Heartbeat-based failure detection protocol")
public abstract class FailureDetection extends Protocol {

    @Property(description="Timeout after which a node is suspected if neither a heartbeat nor data have been received from it",
      type=AttributeType.TIME)
    protected long                                   timeout=40000;

    @Property(description="Interval at which a HEARTBEAT is sent to the cluster",type=AttributeType.TIME)
    protected long                                   interval=8000;

    @ManagedAttribute(description="Number of heartbeats sent",type=AttributeType.SCALAR)
    protected int                                    num_heartbeats_sent;

    @ManagedAttribute(description="Number of heartbeats received",type=AttributeType.SCALAR)
    protected int                                    num_heartbeats_received;

    @ManagedAttribute(description="Number of suspected events received",type=AttributeType.SCALAR)
    protected int                                    num_suspect_events;

    @ManagedAttribute(description="Shows whether there are currently any suspected members")
    protected volatile boolean                       has_suspected_mbrs;

    protected       Address                          local_addr;
    protected final List<Address>                    members=new ArrayList<>();
    protected final Set<Address>                     suspected_mbrs=new HashSet<>();
    protected final BoundedList<Tuple<Address,Long>> suspect_history=new BoundedList<>(20);
    protected final Lock                             lock=new ReentrantLock();
    protected       TimeScheduler                    timer;
    protected final Predicate<Message>               HAS_HEADER=msg -> msg != null && msg.getHeader(this.id) != null;

    // task which multicasts HEARTBEAT message after 'interval' ms
    @GuardedBy("lock") protected Future<?>           heartbeat_sender;

    // task which checks for members exceeding timeout and suspects them
    @GuardedBy("lock") protected Future<?>           timeout_checker;

    protected final AtomicBoolean                    mcast_sent=new AtomicBoolean(false);


    protected abstract Map<Address,?>     getTimestamps();

    protected abstract long               getTimeoutCheckInterval();
    protected abstract String             getTimeoutCheckerInfo();

    protected abstract void               update(Address sender, boolean log_msg, boolean skip_if_exists);
    protected abstract <T> boolean        needsToBeSuspected(Address mbr, T value);


    public long                           getTimeout()                   {return timeout;}
    public <T extends FailureDetection> T setTimeout(long t)             {this.timeout=t; return (T)this;}
    public long                           getInterval()                  {return interval;}
    public <T extends FailureDetection> T setInterval(long i)            {this.interval=i; return (T)this;}
    public int                            getHeartbeatsSent()            {return num_heartbeats_sent;}
    public int                            getHeartbeatsReceived()        {return num_heartbeats_received;}
    public int                            getSuspectEventsSent()         {return num_suspect_events;}
    protected void                        retainKeys(List<Address> mbrs) {getTimestamps().keySet().retainAll(mbrs);}
    protected Runnable                    createTimeoutChecker()         {return new TimeoutChecker();}

    @ManagedAttribute(description="This member's address")
    public String getLocalAddress() {return String.format("%s", local_addr);}

    @ManagedAttribute(description="The members of the cluster")
    public String getMembers() {return Util.printListWithDelimiter(members, ",");}

    @ManagedAttribute(description="Currently suspected members")
    public synchronized String getSuspectedMembers() {return suspected_mbrs.toString();}

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

    @ManagedAttribute(description="Is the timeout checker task running")
    public boolean isTimeoutCheckerRunning() {
        return timeout_checker != null && !timeout_checker.isDone();
    }

    @ManagedAttribute(description="Is the heartbeat sender task running")
    public boolean isHeartbeatSenderRunning() {
        return heartbeat_sender != null && !heartbeat_sender.isDone();
    }

    @ManagedOperation(description="Resumes checking for crashed members")
    public void startFailureDetection() {
        startTimeoutChecker();
    }

    @ManagedOperation(description="Stops checking for crashed members")
    public void stopFailureDetection() {
        stopTimeoutChecker();
    }

    @ManagedOperation(description="Prints suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Tuple<Address,Long> tmp: suspect_history) {
            sb.append(new Date(tmp.getVal2())).append(": ").append(tmp.getVal1()).append("\n");
        }
        return sb.toString();
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

    public synchronized void stop() {
        stopHeartbeatSender();
        stopTimeoutChecker();
        suspected_mbrs.clear();
        has_suspected_mbrs=false;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                down_prot.down(evt);
                View v=evt.getArg();
                handleViewChange(v);
                return null;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.UNSUSPECT:
                Address mbr=evt.getArg();
                unsuspect(mbr);
                update(mbr, false, false);
                break;
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        if(msg.getDest() == null)
            mcast_sent.compareAndSet(false, true);
        return down_prot.down(msg);
    }

    public Object up(Message msg) {
        Address sender=msg.getSrc();
        Header hdr=msg.getHeader(this.id);
        if(hdr != null) {
            update(sender, true, false); // updates the heartbeat entry for 'sender'
            num_heartbeats_received++;
            unsuspect(sender);
            return null; // consume heartbeat message, do not pass to the layer above
        }
        update(sender, false, false);
        if(has_suspected_mbrs)
            unsuspect(sender);
        return up_prot.up(msg); // pass up to the layer above us
    }

    public void up(MessageBatch batch) {
        int matched_msgs=batch.replaceIf(HAS_HEADER, null, true);
        update(batch.sender(), matched_msgs > 0, false);
        if(matched_msgs > 0)
            num_heartbeats_received+=matched_msgs;
        if(has_suspected_mbrs)
            unsuspect(batch.sender());
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        synchronized(this) {
            members.clear();
            members.addAll(mbrs);
            if(suspected_mbrs.retainAll(mbrs))
                has_suspected_mbrs=!suspected_mbrs.isEmpty();
            retainKeys(mbrs);
        }
        mbrs.forEach(m -> update(m, false, true));
        if(mbrs.size() > 1) {
            startHeartbeatSender();
            startTimeoutChecker();
        }
        else {
            stopHeartbeatSender();
            stopTimeoutChecker();
        }
    }

    protected void suspect(List<Address> suspects) {
        if(suspects == null || suspects.isEmpty())
            return;
        num_suspect_events+=suspects.size();
        final List<Address> eligible_mbrs;
        synchronized(this) {
            for(Address suspect: suspects) {
                suspect_history.add(new Tuple<>(suspect, System.currentTimeMillis())); // need wall clock time
                suspected_mbrs.add(suspect);
            }
            eligible_mbrs=new ArrayList<>(members);
            eligible_mbrs.removeAll(suspected_mbrs);
            has_suspected_mbrs=!suspected_mbrs.isEmpty();
        }
        // Check if we're coord, then send up the stack
        if(local_addr != null && !eligible_mbrs.isEmpty() && local_addr.equals(eligible_mbrs.get(0))) {
            log.debug("%s: suspecting %s", local_addr, suspects);
            up_prot.up(new Event(Event.SUSPECT, suspects));
            down_prot.down(new Event(Event.SUSPECT, suspects));
        }
    }


    /**
     * Removes mbr from suspected_mbrs and sends a UNSUSPECT event up and down the stack
     * @param mbr The member to be unsuspected
     * @return True if the member was removed from suspected_mbrs, otherwise false
     */
    protected boolean unsuspect(Address mbr) {
        if(mbr == null) return false;
        boolean do_unsuspect;
        synchronized(this) {
            do_unsuspect=!suspected_mbrs.isEmpty() && suspected_mbrs.remove(mbr);
            if(do_unsuspect) {
                has_suspected_mbrs=!suspected_mbrs.isEmpty();
                log.debug("%s: unsuspecting %s", local_addr, mbr);
            }
        }
        if(do_unsuspect) {
            up_prot.up(new Event(Event.UNSUSPECT, mbr));
            down_prot.down(new Event(Event.UNSUSPECT, mbr));
        }
        return do_unsuspect;
    }



    protected void startHeartbeatSender() {
        lock.lock();
        try {
            if(!isHeartbeatSenderRunning()) {
                heartbeat_sender=timer.scheduleWithFixedDelay(new HeartbeatSender(this), 0, interval, TimeUnit.MILLISECONDS,
                                                              getTransport() instanceof TCP);
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void stopHeartbeatSender() {
        lock.lock();
        try {
            if(heartbeat_sender != null) {
                heartbeat_sender.cancel(true);
                heartbeat_sender=null;
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void startTimeoutChecker() {
        lock.lock();
        try {
            if(!isTimeoutCheckerRunning()) {
                timeout_checker=timer.scheduleWithFixedDelay(createTimeoutChecker(), getTimeoutCheckInterval(),
                                                             getTimeoutCheckInterval(), TimeUnit.MILLISECONDS, false);
            }
        }
        finally {
            lock.unlock();
        }
    }

    protected void stopTimeoutChecker() {
        lock.lock();
        try {
            if(timeout_checker != null) {
                timeout_checker.cancel(true);
                timeout_checker=null;
            }
        }
        finally {
            lock.unlock();
        }
    }





    public static class HeartbeatHeader extends Header {
        public HeartbeatHeader() {}
        public short getMagicId() {return 62;}
        public Supplier<? extends Header> create() {return HeartbeatHeader::new;}
        public String toString() {return "heartbeat";}
        @Override public int  serializedSize()        {return 0;}
        @Override public void writeTo(DataOutput out) {}
        @Override public void readFrom(DataInput in)  {}
    }

    /** Class which periodically multicasts a HEARTBEAT message to the cluster */
    class HeartbeatSender implements Runnable {
        protected final FailureDetection enclosing;

        HeartbeatSender(FailureDetection enclosing) {
            this.enclosing=enclosing;
        }

        public void run() {
            if(mcast_sent.compareAndSet(true, false))
                ; // suppress sending of heartbeat
            else {
                Message heartbeat=new EmptyMessage().setFlag(Message.Flag.INTERNAL).setFlag(Message.TransientFlag.DONT_LOOPBACK)
                  .putHeader(id, new HeartbeatHeader());
                down_prot.down(heartbeat);
                num_heartbeats_sent++;
                log.trace("%s: sent heartbeat", local_addr);
            }
        }

        public String toString() {
            return String.format("%s: %s", enclosing.getClass().getSimpleName(), getClass().getSimpleName());
        }
    }

    class TimeoutChecker implements Runnable {

        public void run() {
            synchronized(this) {
                retainKeys(members); // remove all non-members (// https://issues.jboss.org/browse/JGRP-2387)
            }
            List<Address> suspects=new LinkedList<>();
            for(Iterator<? extends Map.Entry<Address,?>> it=getTimestamps().entrySet().iterator(); it.hasNext();) {
                Map.Entry<Address,?> entry=it.next();
                Address key=entry.getKey();
                Object val=entry.getValue();
                if(val == null) {
                    it.remove();
                    continue;
                }
                if(needsToBeSuspected(key, val))
                    suspects.add(key);
            }
            if(!suspects.isEmpty())
                suspect(suspects);
        }

        public String toString() {return getTimeoutCheckerInfo();}
    }
}

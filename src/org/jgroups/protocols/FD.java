
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Failure detection based on simple heartbeat protocol. Regularly polls members
 * for liveness. Multicasts SUSPECT messages when a member is not reachable. The
 * simple algorithms works as follows: the membership is known and ordered. Each
 * HB protocol periodically sends an 'are-you-alive' message to its *neighbor*.
 * A neighbor is the next in rank in the membership list, which is recomputed
 * upon a view change. When a response hasn't been received for n milliseconds
 * and m tries, the corresponding member is suspected (and eventually excluded
 * if faulty).
 * <p>
 * FD starts when it detects (in a view change notification) that there are at
 * least 2 members in the group. It stops running when the membership drops
 * below 2.
 * <p>
 * When a message is received from the monitored neighbor member, it causes the
 * pinger thread to 'skip' sending the next are-you-alive message. Thus, traffic
 * is reduced.
 *
 * @author Bela Ban
 */
@MBean(description="Failure detection based on simple heartbeat protocol")
public class FD extends Protocol {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    @Property(description="Timeout to suspect a node P if neither a heartbeat nor data were received from P.")
    protected long                       timeout=3000;

    @Property(description="Number of times to send an are-you-alive message")
    protected int                        max_tries=5;


    @Property(description="Treat messages received from members as heartbeats. Note that this means we're updating " +
      "a value in a hashmap every time a message is passing up the stack through FD, which is costly.")
    boolean                              msg_counts_as_heartbeat=true;

    
    /* ---------------------------------------------   JMX      ------------------------------------------------------ */

    protected int                        num_heartbeats;
    
    protected int                        num_suspect_events;

    protected final BoundedList<Address> suspect_history=new BoundedList<Address>(20);


    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    protected Address                    local_addr;
    
    protected volatile long              last_ack=System.currentTimeMillis();
    
    protected final AtomicInteger        num_tries=new AtomicInteger(1);

    protected final Lock                 lock=new ReentrantLock();

    @GuardedBy("lock")
    protected volatile Address           ping_dest;

    @GuardedBy("lock")
    protected final List<Address>        members=new ArrayList<Address>();

    /** Members from which we select ping_dest. Copy of {@link #members} minus the suspected members */
    @GuardedBy("lock")
    protected final List<Address>        pingable_mbrs=new ArrayList<Address>();
   
    protected TimeScheduler              timer;

    // task that performs the actual monitoring for failure detection
    @GuardedBy("lock")
    protected Future<?>                  monitor_future=null;
    
    /** Transmits SUSPECT message until view change or UNSUSPECT is received */
    protected final Broadcaster          bcast_task=new Broadcaster();
    
    @ManagedAttribute(description="Member address")
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute(description="List of cluster members")
    public String getMembers() {return members != null? members.toString() : "null";}
    @ManagedAttribute(description="List of pingable members of a cluster")
    public String getPingableMembers() {return pingable_mbrs != null? pingable_mbrs.toString() : "null";}
    @ManagedAttribute(description="Ping destination")
    public String getPingDest() {return ping_dest != null? ping_dest.toString() : "null";}
    @ManagedAttribute(description="Number of heartbeats sent")    
    public int getNumberOfHeartbeatsSent() {return num_heartbeats;}
    @ManagedAttribute(description="Number of suspect events received")
    public int getNumSuspectEventsGenerated() {return num_suspect_events;}
    public long getTimeout() {return timeout;}
    public void setTimeout(long timeout) {this.timeout=timeout;}
    public int getMaxTries() {return max_tries;}
    public void setMaxTries(int max_tries) {this.max_tries=max_tries;}
    public int getCurrentNumTries() {return num_tries.get();}
    @ManagedOperation(description="Print suspect history")
    public String printSuspectHistory() {
        StringBuilder sb=new StringBuilder();
        for(Address addr: suspect_history) {
            sb.append(new Date()).append(": ").append(addr).append("\n");
        }
        return sb.toString();
    }   

    public void resetStats() {
        num_heartbeats=num_suspect_events=0;
        suspect_history.clear();
    }


    public void init() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");
    }


    public void stop() {
        lock.lock();
        try {
            ping_dest=null;
            stopMonitor();
        }
        finally {
            lock.unlock();
        }
    }


    protected Address getPingDest(List<Address> mbrs) {
        Address tmp, retval=null;

        if(mbrs == null || mbrs.size() < 2 || local_addr == null)
            return null;
        for(int i=0; i < mbrs.size(); i++) {
            tmp=mbrs.get(i);
            if(local_addr.equals(tmp)) {
                if(i + 1 >= mbrs.size())
                    retval=mbrs.get(0);
                else
                    retval=mbrs.get(i + 1);
                break;
            }
        }
        return retval;
    }



    @ManagedOperation(description="Stops checking for crashed members")
    public void stopFailureDetection() {
        stopMonitor();
    }

    @ManagedOperation(description="Resumes checking for crashed members")
    public void startFailureDetection() {
        startMonitor();
    }

    /** Requires lock to held by caller */
    @GuardedBy("lock")
    protected void startMonitor() {
        if(monitor_future == null || monitor_future.isDone()) {
            last_ack=System.currentTimeMillis();  // start from scratch
            monitor_future=timer.scheduleWithFixedDelay(new Monitor(), timeout, timeout, TimeUnit.MILLISECONDS);
            num_tries.set(1);
        }
    }

    /** Requires lock to be held by caller */
    @GuardedBy("lock")
    protected void stopMonitor() {
        if(monitor_future != null) {
            monitor_future.cancel(true);
            monitor_future=null;
        }
    }

    @ManagedAttribute(description="Whether the failure detection monitor is running",writable=false)
    public boolean isMonitorRunning() {return monitor_future != null && !monitor_future.isDone();}



    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FdHeader hdr=(FdHeader)msg.getHeader(this.id);
                if(hdr == null) {
                    if(msg_counts_as_heartbeat)
                        updateTimestamp(msg.getSrc());
                    break;  // message did not originate from FD layer, just pass up
                }

                switch(hdr.type) {
                    case FdHeader.HEARTBEAT:                       // heartbeat request; send heartbeat ack
                        Address hb_sender=msg.getSrc();
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": received are-you-alive from " + hb_sender + ", sending response");
                        sendHeartbeatResponse(hb_sender);
                        break;                                     // don't pass up !

                    case FdHeader.HEARTBEAT_ACK:                   // heartbeat ack
                        updateTimestamp(hdr.from);
                        break;

                    case FdHeader.SUSPECT:
                        if(hdr.mbrs == null)
                            return null;
                        if(log.isTraceEnabled()) log.trace(local_addr + ": received suspect message: " + hdr);
                        for(Address mbr: hdr.mbrs) {
                            if(local_addr != null && mbr.equals(local_addr)) {
                                if(log.isWarnEnabled())
                                    log.warn(local_addr + ": I was suspected by " + msg.getSrc() +
                                               "; ignoring the SUSPECT message and sending back a HEARTBEAT_ACK");
                                sendHeartbeatResponse(msg.getSrc());
                                continue;
                            }
                            lock.lock();
                            try {
                                computePingDest(mbr);
                            }
                            finally {
                                lock.unlock();
                            }
                            up_prot.up(new Event(Event.SUSPECT, mbr));
                            down_prot.down(new Event(Event.SUSPECT, mbr));
                        }
                        break;
                }
                return null;
        }
        return up_prot.up(evt); // pass up to the layer above us
    }


    public void up(MessageBatch batch) {
        Collection<Message> msgs=batch.getMatchingMessages(id, true);
        boolean updated=false;
        if(msgs != null) {
            for(Message msg: msgs) {
                FdHeader hdr=(FdHeader)msg.getHeader(id); // header is not null at this point
                if(hdr.type == FdHeader.HEARTBEAT_ACK)
                    updated=true;
                else
                    up(new Event(Event.MSG, msg)); // SUSPECT and HEARTBEAT
            }
        }
        if(updated || (msg_counts_as_heartbeat && batch.sender() != null))
            updateTimestamp(batch.sender());
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=down_prot.down(evt);
                View view=(View)evt.getArg();

                lock.lock();
                try {
                    members.clear();
                    members.addAll(view.getMembers());
                    bcast_task.adjustSuspectedMembers(members);
                    computePingDest(null);
                    if(view.size() <= 1)
                        stopMonitor();
                    else if(!isMonitorRunning())
                        startMonitor();
                }
                finally {
                    lock.unlock();
                }
                return retval;

            case Event.UNSUSPECT:
                unsuspect((Address)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    protected void sendHeartbeatResponse(Address dest) {
        Message hb_ack=new Message(dest).setFlag(Message.Flag.INTERNAL);
        FdHeader tmp_hdr=new FdHeader(FdHeader.HEARTBEAT_ACK);
        tmp_hdr.from=local_addr;
        hb_ack.putHeader(this.id, tmp_hdr);
        down_prot.down(new Event(Event.MSG, hb_ack));
    }

    @GuardedBy("lock")
    protected void unsuspect(Address mbr) {
        lock.lock();
        try {
            bcast_task.removeSuspectedMember(mbr);
            computePingDest(null);
        }
        finally {
            lock.unlock();
        }
    }

    protected void updateTimestamp(Address sender) {
        if(sender != null && sender.equals(ping_dest)) {
            last_ack=System.currentTimeMillis();
            num_tries.set(1);
        }
    }


    /**
     * Computes pingable_mbrs (based on the current membership and the suspected members) and ping_dest
     * @param remove The member to be removed from pingable_mbrs
     */
    @GuardedBy("lock")
    protected void computePingDest(Address remove) {
        if(remove != null)
            pingable_mbrs.remove(remove);
        else {
            pingable_mbrs.clear();
            pingable_mbrs.addAll(members);
            pingable_mbrs.removeAll(bcast_task.getSuspectedMembers());
        }

        Address old_ping_dest=ping_dest;
        ping_dest=getPingDest(pingable_mbrs);
        if(Util.different(old_ping_dest, ping_dest)) {
            num_tries.set(1);
            last_ack=System.currentTimeMillis();
        }
    }



    public static class FdHeader extends Header {
        public static final byte HEARTBEAT=0;
        public static final byte HEARTBEAT_ACK=1;
        public static final byte SUSPECT=2;


        protected byte                 type=HEARTBEAT;
        protected Collection<Address>  mbrs;
        protected Address              from;  // member who detected that suspected_mbr has failed


        public FdHeader() {
        }

        public FdHeader(byte type) {
            this.type=type;
        }

        public FdHeader(byte type, Collection<Address> mbrs, Address from) {
            this(type);
            this.mbrs=mbrs;
            this.from=from;
        }


        public String toString() {
            switch(type) {
                case HEARTBEAT:
                    return "heartbeat";
                case HEARTBEAT_ACK:
                    return "heartbeat ack";
                case SUSPECT:
                    return "SUSPECT (suspected_mbrs=" + mbrs + ", from=" + from + ")";
                default:
                    return "unknown type (" + type + ")";
            }
        }


        public int size() {
            int retval=Global.BYTE_SIZE; // type
            retval+=Util.size(mbrs);
            retval+=Util.size(from);
            return retval;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeAddresses(mbrs, out);
            Util.writeAddress(from, out);
        }


        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            mbrs=(Collection<Address>)Util.readAddresses(in, ArrayList.class);
            from=Util.readAddress(in);
        }

    }


    /** Task which periodically checks of the last_ack from ping_dest exceeded timeout and - if yes - broadcasts
     * a SUSPECT message */
    protected class Monitor implements Runnable {

        public void run() {
            Address dest=ping_dest;
            if(dest == null) {
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": ping_dest is null, skipping timeout check: members=" + members +
                                ", pingable_mbrs=" + pingable_mbrs + ", local_addr=" + local_addr);
                return;
            }

            // 1. send heartbeat request
            Message hb_req=new Message(dest).setFlag(Message.Flag.INTERNAL).putHeader(id, new FdHeader(FdHeader.HEARTBEAT));
            if(log.isDebugEnabled())
                log.debug(local_addr + ": sending are-you-alive msg to " + dest);
            down_prot.down(new Event(Event.MSG, hb_req));
            num_heartbeats++;

            // 2. If the time of the last heartbeat is > timeout and max_tries heartbeat messages have not been
            //    received, then broadcast a SUSPECT message. Will be handled by coordinator, which may install
            //    a new view
            long not_heard_from=System.currentTimeMillis() - last_ack; // time in msecs we haven't heard from ping_dest
            // quick & dirty fix: increase timeout by 500 ms to allow for latency (bela June 27 2003)
            if(not_heard_from > timeout + 500) { // no heartbeat ack for more than timeout msecs
                int tmp_tries=num_tries.get();
                if(tmp_tries >= max_tries) {
                    if(!dest.equals(ping_dest)) // ping_dest was changed meanwhile...
                        return;
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": received no heartbeat from " + dest + " for " + tmp_tries +
                                    " times (" + (tmp_tries * timeout) + " milliseconds), suspecting it");
                    // broadcast a SUSPECT message to all members - loop until unsuspect or view change is received
                    bcast_task.addSuspectedMember(dest);
                    num_tries.set(1);
                    if(stats) {
                        num_suspect_events++;
                        suspect_history.add(dest);
                    }
                }
                else {
                    if(log.isDebugEnabled())
                        log.debug(local_addr + ": heartbeat missing from " + dest + " (number=" + tmp_tries + ')');
                    num_tries.incrementAndGet();
                }
            }
        }

        public String toString() {
            return FD.class.getSimpleName() + ": Monitor (timeout=" + timeout + "ms)";
        }
    }


    /**
     * Task that periodically broadcasts a list of suspected members to the group. Goal is not to lose
     * a SUSPECT message: since these are bcast unreliably, they might get dropped. The BroadcastTask makes
     * sure they are retransmitted until a view has been received which doesn't contain the suspected members
     * any longer. Then the task terminates.
     */
    protected final class Broadcaster {
        protected final List<Address> suspected_mbrs=new ArrayList<Address>(7);
        protected final Lock          bcast_lock=new ReentrantLock();
        @GuardedBy("bcast_lock")
        protected Future<?>           bcast_future=null;
        @GuardedBy("bcast_lock")
        protected BroadcastTask       task;


        protected List<Address> getSuspectedMembers() {
            return suspected_mbrs;
        }

        /**
         * Starts a new task, or - if already running - adds the argument to the running task.
         * @param suspect
         */
        protected void startBroadcastTask(Address suspect) {
            bcast_lock.lock();
            try {
                if(bcast_future == null || bcast_future.isDone()) {
                    task=new BroadcastTask(suspected_mbrs);
                    task.addSuspectedMember(suspect);
                    bcast_future=timer.scheduleWithFixedDelay(task,
                                                              0, // run immediately the first time
                                                              timeout, // then every timeout milliseconds, until cancelled
                                                              TimeUnit.MILLISECONDS);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": BroadcastTask started");
                }
                else {
                    task.addSuspectedMember(suspect);
                }
            }
            finally {
                bcast_lock.unlock();
            }
        }

        protected void stopBroadcastTask() {
            bcast_lock.lock();
            try {
                if(bcast_future != null) {
                    bcast_future.cancel(true);
                    bcast_future=null;
                    task=null;
                }
            }
            finally {
                bcast_lock.unlock();
            }
        }

        /** Adds a suspected member. Starts the task if not yet running */
        protected void addSuspectedMember(Address mbr) {
            if(mbr == null) return;
            if(!members.contains(mbr)) return;
            synchronized(suspected_mbrs) {
                if(!suspected_mbrs.contains(mbr)) {
                    suspected_mbrs.add(mbr);
                    startBroadcastTask(mbr);
                }
            }
        }

        void removeSuspectedMember(Address suspected_mbr) {
            if(suspected_mbr == null) return;
            if(log.isDebugEnabled()) log.debug(local_addr + ": member is " + suspected_mbr);
            synchronized(suspected_mbrs) {
                suspected_mbrs.remove(suspected_mbr);
                if(suspected_mbrs.isEmpty())
                    stopBroadcastTask();
            }
        }


        /** Removes all elements from suspected_mbrs that are <em>not</em> in the new membership */
        void adjustSuspectedMembers(List<Address> new_mbrship) {
            if(new_mbrship == null || new_mbrship.isEmpty()) return;
            synchronized(suspected_mbrs) {
                suspected_mbrs.retainAll(new_mbrship);
                if(suspected_mbrs.isEmpty())
                    stopBroadcastTask();
            }
        }
    }


    protected final class BroadcastTask implements Runnable {
        protected final List<Address> suspected_members=new ArrayList<Address>();


        BroadcastTask(List<Address> suspected_members) {
            this.suspected_members.addAll(suspected_members);
        }

        public void stop() {
            suspected_members.clear();
            if(log.isTraceEnabled())
                log.trace(local_addr + ": BroadcastTask stopped");
        }


        public void run() {
            FD.FdHeader hdr;
            synchronized(suspected_members) {
                if(suspected_members.isEmpty()) {
                    stop();
                    return;
                }

                hdr=new FdHeader(FdHeader.SUSPECT);
                hdr.mbrs=new ArrayList<Address>(suspected_members);
                hdr.from=local_addr;
            }
            Message suspect_msg=new Message().setFlag(Message.Flag.INTERNAL).putHeader(id, hdr);
            if(log.isDebugEnabled())
                log.debug(local_addr + ": broadcasting SUSPECT message (suspects=" + suspected_members + ")");
            down_prot.down(new Event(Event.MSG, suspect_msg));
        }

        public void addSuspectedMember(Address suspect) {
            if(suspect != null && !suspected_members.contains(suspect)) {
                suspected_members.add(suspect);
            }
        }

        public String toString() {
            return "BroadcastTask (" + suspected_members.size() + " suspected mbrs)";
        }
    }

}

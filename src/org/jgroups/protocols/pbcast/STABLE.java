package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Computes the broadcast messages that are stable; i.e., have been delivered by all members. Sends STABLE events down
 * the stack when this is the case. This allows NAKACK{2,3} to garbage collect messages that have been seen by all members.
 * <p>
 * Works as follows: periodically (desired_avg_gossip) or when having received a number of bytes (max_bytes), every
 * member sends its digest (highest seqno delivered, received) to the cluster (send_stable_msgs_to_coord_only=false)
 * or the current coordinator (send_stable_msgs_to_coord_only=true).<p/>
 * The recipient updates a stability vector, which maintains the highest seqno delivered/receive for each member
 * and initially contains no data, when such a message is received. <p/>
 * When messages from all members have been received, a stability message is mcast, which causes all
 * members to send a STABLE event down the stack (triggering garbage collection in the NAKACK{2,3} layer).
 * <p>
 * When send_stable_msgs_to_coord_only is true, far fewer messages are exchanged, as members don't multicast
 * STABLE messages, but instead send them only to the coordinator.
 * @author Bela Ban
 */
@MBean(description="Computes the broadcast messages that are stable")
public class STABLE extends Protocol {
    protected static final long MAX_SUSPEND_TIME=200000;

    /* ------------------------------------------ Properties  ------------------------------------------ */

    /**
     * Sends a STABLE gossip every 20 seconds on average. 0 disables gossiping of STABLE messages
     */
    @Property(description="Average time to send a STABLE message")
    protected long   desired_avg_gossip=20000;

    /**
     * delay before we send STABILITY msg (give others a change to send first).
     * This should be set to a very small number (> 0 !) if <code>max_bytes</code> is used
     */
    @Property(description="Delay before stability message is sent")
    protected long   stability_delay=6000;

    /**
     * Total amount of bytes from incoming messages (default = 0 = disabled).
     * When exceeded, a STABLE message will be broadcast and
     * <code>num_bytes_received</code> reset to 0 . If this is > 0, then
     * ideally <code>stability_delay</code> should be set to a low number as
     * well
     */
    @Property(description="Maximum number of bytes received in all messages before sending a STABLE message is triggered")
    protected long   max_bytes=2000000;

    @Property(description="Max percentage of the max heap (-Xmx) to be used for max_bytes. " +
      "Only used if ergonomics is enabled. 0 disables setting max_bytes dynamically.",deprecatedMessage="will be ignored")
    @Deprecated
    protected double cap=0.10; // 10% of the max heap by default


    @Property(description="Wether or not to send the STABLE messages to all members of the cluster, or to the " +
      "current coordinator only. The latter reduces the number of STABLE messages, but also generates more work " +
      "on the coordinator")
    protected boolean send_stable_msgs_to_coord_only=true;

    
    /* --------------------------------------------- JMX  ---------------------------------------------- */

    protected int    num_stable_msgs_sent;
    protected int    num_stable_msgs_received;
    protected int    num_stability_msgs_sent;
    protected int    num_stability_msgs_received;

    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    protected Address             local_addr;
    protected final Set<Address>  mbrs=new LinkedHashSet<Address>(); // we don't need ordering here

    @GuardedBy("lock")
    protected final MutableDigest digest=new MutableDigest(10); // keeps track of the highest seqnos from all members

    /**
     * Keeps track of who we already heard from (STABLE_GOSSIP msgs). This is cleared initially, and we add the sender
     * when a STABLE message is received. When the list is full (responses from all members), we send a STABILITY message
     */
    @GuardedBy("lock")
    protected final Set<Address>  votes=new HashSet<Address>();

    protected final Lock          lock=new ReentrantLock();

    @GuardedBy("stability_lock")
    protected Future<?>           stability_task_future;
    protected final Lock          stability_lock=new ReentrantLock(); // to synchronize on stability_task

    @GuardedBy("stable_task_lock")
    protected Future<?>           stable_task_future=null; // bcasts periodic STABLE message (added to timer below)
    protected final Lock          stable_task_lock=new ReentrantLock(); // to sync on stable_task

    protected TimeScheduler       timer; // to send periodic STABLE msgs (and STABILITY messages)

    /** The total number of bytes received from unicast and multicast messages */
    @GuardedBy("received")
    @ManagedAttribute(description="Bytes accumulated so far")
    protected long                num_bytes_received=0;

    protected final Lock          received=new ReentrantLock();

    /**
     * When true, don't take part in garbage collection: neither send STABLE messages nor handle STABILITY messages
     */
    @ManagedAttribute
    protected volatile boolean    suspended=false;

    protected boolean             initialized=false;

    protected Future<?>           resume_task_future;
    protected final Object        resume_task_mutex=new Object();

    protected volatile Address    coordinator;

    
    
    public STABLE() {             
    }

    public long getDesiredAverageGossip() {
        return desired_avg_gossip;
    }

    public void setDesiredAverageGossip(long gossip_interval) {
        desired_avg_gossip=gossip_interval;
    }

    public long getMaxBytes() {
        return max_bytes;
    }

    public void setMaxBytes(long max_bytes) {
        this.max_bytes=max_bytes;
    }

    @ManagedAttribute(name="bytes_received")
    public long getBytes() {return num_bytes_received;}
    @ManagedAttribute
    public int getStableSent() {return num_stable_msgs_sent;}
    @ManagedAttribute
    public int getStableReceived() {return num_stable_msgs_received;}
    @ManagedAttribute
    public int getStabilitySent() {return num_stability_msgs_sent;}
    @ManagedAttribute
    public int getStabilityReceived() {return num_stability_msgs_received;}

    @ManagedAttribute
    public boolean getStableTaskRunning() {
        stable_task_lock.lock();
        try {
            return stable_task_future != null && !stable_task_future.isDone() && !stable_task_future.isCancelled();
        }
        finally {
            stable_task_lock.unlock();
        }
    }

    public void resetStats() {
        super.resetStats();
        num_stability_msgs_received=num_stability_msgs_sent=num_stable_msgs_sent=num_stable_msgs_received=0;
    }


    public List<Integer> requiredDownServices() {
        List<Integer> retval=new ArrayList<Integer>();
        retval.add(Event.GET_DIGEST);  // from the NAKACK layer
        return retval;
    }

    protected void suspend(long timeout) {
        if(!suspended) {
            suspended=true;
            if(log.isDebugEnabled())
                log.debug("suspending message garbage collection");
        }
        startResumeTask(timeout); // will not start task if already running
    }

    protected void resume() {
        lock.lock();
        try {
            resetDigest(); // start from scratch
            suspended=false;
        }
        finally {
            lock.unlock();
        }

        if(log.isDebugEnabled())
            log.debug("resuming message garbage collection");
        stopResumeTask();
    }
    
    public void init() throws Exception {
        super.init();
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer cannot be retrieved");
        if(desired_avg_gossip > 0)
            startStableTask();
    }

    public void stop() {
        stopStableTask();
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                StableHeader hdr=(StableHeader)msg.getHeader(this.id);
                if(hdr == null) {
                    handleRegularMessage(msg);
                    return up_prot.up(evt);
                }

                handleUpEvent(hdr, msg.getSrc());
                return null;  // don't pass STABLE or STABILITY messages up the stack

            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                View view=(View)evt.getArg();
                handleViewChange(view);
                return retval;
        }
        return up_prot.up(evt);
    }

    protected void handleUpEvent(StableHeader hdr, Address sender) {
        switch(hdr.type) {
            case StableHeader.STABLE_GOSSIP:
                handleStableMessage(hdr.stableDigest, sender);
                break;
            case StableHeader.STABILITY:
                handleStabilityMessage(hdr.stableDigest, sender);
                break;
            default:
                if(log.isErrorEnabled()) log.error("StableHeader type " + hdr.type + " not known");
        }
    }


    public void up(MessageBatch batch) {
        StableHeader hdr;

        for(Message msg: batch) { // remove and handle messages with flow control headers (STABLE_GOSSIP, STABILITY)
            if((hdr=(StableHeader)msg.getHeader(id)) != null) {
                batch.remove(msg);
                handleUpEvent(hdr, batch.sender());
            }
        }

        // only if message counting is on, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes > 0 && batch.dest() == null && !batch.isEmpty()) {
            boolean send_stable_msg=false;
            received.lock();
            try {
                num_bytes_received+=batch.length();
                if(num_bytes_received >= max_bytes) {
                    if(log.isTraceEnabled())
                        log.trace(new StringBuilder("max_bytes has been reached (").append(max_bytes).
                          append(", bytes received=").append(num_bytes_received).append("): triggers stable msg"));
                    num_bytes_received=0;
                    send_stable_msg=true;
                }
            }
            finally {
                received.unlock();
            }

            if(send_stable_msg) {
                Digest my_digest=getDigest();  // asks the NAKACK protocol for the current digest,
                if(log.isTraceEnabled())
                    log.trace("setting latest_local_digest from NAKACK: " + my_digest.printHighestDeliveredSeqnos());
                sendStableMessage(my_digest);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void handleRegularMessage(Message msg) {
        // only if bytes counting is enabled, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes <= 0)
            return;
        Address dest=msg.getDest();
        if(dest == null) {
            boolean send_stable_msg=false;
            received.lock();
            try {
                num_bytes_received+=msg.getLength();
                if(num_bytes_received >= max_bytes) {
                    if(log.isTraceEnabled()) {
                        log.trace(new StringBuilder("max_bytes has been reached (").append(max_bytes).
                                append(", bytes received=").append(num_bytes_received).append("): triggers stable msg"));
                    }
                    num_bytes_received=0;
                    send_stable_msg=true;
                }
            }
            finally {
                received.unlock();
            }

            if(send_stable_msg) {
                Digest my_digest=getDigest();  // asks the NAKACK protocol for the current digest,
                if(log.isTraceEnabled())
                    log.trace("setting latest_local_digest from NAKACK: " + my_digest.printHighestDeliveredSeqnos());
                sendStableMessage(my_digest);
            }
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=down_prot.down(evt);
                View v=(View)evt.getArg();
                handleViewChange(v);
                return retval;

            case Event.SUSPEND_STABLE:
                long timeout=MAX_SUSPEND_TIME;
                Object t=evt.getArg();
                if(t != null && t instanceof Long)
                    timeout=(Long)t;
                suspend(timeout);
                break;

            case Event.RESUME_STABLE:
                resume();
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }


    @ManagedOperation
    public void runMessageGarbageCollection() {
        Digest copy=getDigest();
        sendStableMessage(copy);
    }

    @ManagedOperation(description="Sends a STABLE message; when every member has received a STABLE message " +
      "from everybody else, a STABILITY message will be sent")
    public void gc() {runMessageGarbageCollection();}



    /* --------------------------------------- Private Methods ---------------------------------------- */


    protected void handleViewChange(View v) {
        List<Address> tmp=v.getMembers();
        synchronized(mbrs) {
            mbrs.clear();
            mbrs.addAll(tmp);
        }
        lock.lock();
        try {
            coordinator=tmp.get(0);
            resetDigest();
            if(!initialized)
                initialized=true;
        }
        finally {
            lock.unlock();
        }
    }




    /** Update my own digest from a digest received by somebody else. Returns whether the update was successful.
     *  Needs to be called with a lock on digest */
    @GuardedBy("lock")
    protected boolean updateLocalDigest(Digest d, Address sender) {
        if(d == null || d.size() == 0)
            return false;

        if(!initialized) {
            if(log.isTraceEnabled())
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return false;
        }

        if(!digest.sameSenders(d)) {
            // to avoid sending incorrect stability/stable msgs, we simply reset our votes list, see DESIGN
            resetDigest();
            return false;
        }

        StringBuilder sb=null;
        if(log.isTraceEnabled()) {
            sb=new StringBuilder().append(local_addr).append(": handling digest from ").append(sender).append(" (").
                    append(votes.size()).append(" votes):\nmine:   ").append(digest.printHighestDeliveredSeqnos())
                    .append("\nother:  ").append(d.printHighestDeliveredSeqnos());
        }

        for(Digest.DigestEntry entry: d) {
            Address mbr=entry.getMember();
            long highest_delivered=entry.getHighestDeliveredSeqno();
            long highest_received=entry.getHighestReceivedSeqno();

            // compute the minimum of the highest seqnos deliverable (for garbage collection)
            long[] seqnos=digest.get(mbr);
            if(seqnos == null)
                continue;
            long my_highest_delivered=seqnos[0];
            // compute the maximum of the highest seqnos seen (for retransmission of last missing message)
            long my_highest_received=seqnos[1];

            long new_highest_delivered=Math.min(my_highest_delivered, highest_delivered);
            long new_highest_received=Math.max(my_highest_received, highest_received);
            digest.setHighestDeliveredAndSeenSeqnos(mbr, new_highest_delivered, new_highest_received);
        }
        if(sb != null) { // implies log.isTraceEnabled() == true
            sb.append("\nresult: ").append(digest.printHighestDeliveredSeqnos()).append("\n");
            log.trace(sb);
        }
        return true;
    }


    @GuardedBy("lock")
    protected void resetDigest() {
        Digest tmp=getDigest();
        digest.replace(tmp);
        if(log.isTraceEnabled())
            log.trace(local_addr + ": resetting digest from NAKACK: " + digest.printHighestDeliveredSeqnos());
        votes.clear();
    }

    /**
     * Adds mbr to votes and returns true if we have all the votes, otherwise false.
     * @param mbr
     */
    @GuardedBy("lock")
    protected boolean addVote(Address mbr) {
        boolean added=votes.add(mbr);
        return added && allVotesReceived(votes);
    }

    /** Votes is already locked and guaranteed to be non-null */
    protected boolean allVotesReceived(Set<Address> votes) {
        synchronized(mbrs) {
            return votes.equals(mbrs); // compares identity, size and element-wise (if needed)
        }
    }


    protected void startStableTask() {
        stable_task_lock.lock();
        try {
            if(stable_task_future == null || stable_task_future.isDone()) {
                StableTask stable_task=new StableTask();
                stable_task_future=timer.scheduleWithDynamicInterval(stable_task);
                if(log.isTraceEnabled())
                    log.trace("stable task started");
            }
        }
        finally {
            stable_task_lock.unlock();
        }
    }


    protected void stopStableTask() {
        stable_task_lock.lock();
        try {
            if(stable_task_future != null) {
                stable_task_future.cancel(false);
                stable_task_future=null;
            }
        }
        finally {
            stable_task_lock.unlock();
        }
    }


    protected void startResumeTask(long max_suspend_time) {
        max_suspend_time=(long)(max_suspend_time * 1.1); // little slack
        if(max_suspend_time <= 0)
            max_suspend_time=MAX_SUSPEND_TIME;

        synchronized(resume_task_mutex) {
            if(resume_task_future == null || resume_task_future.isDone()) {
                ResumeTask resume_task=new ResumeTask();
                resume_task_future=timer.schedule(resume_task, max_suspend_time, TimeUnit.MILLISECONDS);
                if(log.isDebugEnabled())
                    log.debug("resume task started, max_suspend_time=" + max_suspend_time);
            }
        }

    }


    protected void stopResumeTask() {
        synchronized(resume_task_mutex) {
            if(resume_task_future != null) {
                resume_task_future.cancel(false);
                resume_task_future=null;
            }
        }
    }


    protected void startStabilityTask(Digest d, long delay) {
        stability_lock.lock();
        try {
            if(stability_task_future == null || stability_task_future.isDone()) {
                StabilitySendTask stability_task=new StabilitySendTask(d); // runs only once
                stability_task_future=timer.schedule(stability_task, delay, TimeUnit.MILLISECONDS);
            }
        }
        finally {
            stability_lock.unlock();
        }
    }


    protected void stopStabilityTask() {
        stability_lock.lock();
        try {
            if(stability_task_future != null) {
                stability_task_future.cancel(false);
                stability_task_future=null;
            }
        }
        finally {
            stability_lock.unlock();
        }
    }


    /**
     Digest d contains (a) the highest seqnos <em>deliverable</em> for each sender and (b) the highest seqnos
     <em>seen</em> for each member. (Difference: with 1,2,4,5, the highest seqno seen is 5, whereas the highest
     seqno deliverable is 2). The minimum of all highest seqnos deliverable will be taken to send a stability
     message, which results in garbage collection of messages lower than the ones in the stability vector. The
     maximum of all seqnos will be taken to trigger possible retransmission of last missing seqno (see DESIGN
     for details).
     */
    protected void handleStableMessage(Digest d, Address sender) {
        if(d == null || sender == null) {
            if(log.isErrorEnabled()) log.error("digest or sender is null");
            return;
        }

        if(!initialized) {
            if(log.isTraceEnabled())
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return;
        }

        if(suspended) {
            if(log.isTraceEnabled())
                log.trace("STABLE message will not be handled as I'm suspended");
            return;
        }

        Digest copy=null;
        lock.lock();
        try {
            if(votes.contains(sender))  // already received gossip from sender; discard it
                return;
            num_stable_msgs_received++;
            boolean success=updateLocalDigest(d, sender);
            if(!success) // we can only add the sender to votes if *all* elements of my digest were updated
                return;

            boolean all_votes_received=addVote(sender);
            if(all_votes_received)
                copy=digest.copy();
        }
        finally {
            lock.unlock();
        }

        // we don't yet reset digest: new STABLE messages will be discarded anyway as we have already
        // received votes from their senders
        if(copy != null) {
            sendStabilityMessage(copy);
        }
    }


    protected void handleStabilityMessage(Digest stable_digest, Address sender) {
        if(stable_digest == null) {
            if(log.isErrorEnabled()) log.error("stability digest is null");
            return;
        }

         if(!initialized) {
             if(log.isTraceEnabled())
                 log.trace("STABLE message will not be handled as I'm not yet initialized");
             return;
         }

         if(suspended) {
             if(log.isDebugEnabled()) {
                 log.debug("stability message will not be handled as I'm suspended");
             }
             return;
         }

         if(log.isTraceEnabled())
             log.trace(new StringBuilder(local_addr + ": received stability msg from ").append(sender).append(": ").append(stable_digest.printHighestDeliveredSeqnos()));
         stopStabilityTask();

        lock.lock();
        try {
            // we won't handle the gossip d, if d's members don't match the membership in my own digest,
            // this is part of the fix for the NAKACK problem (bugs #943480 and #938584)
            if(!this.digest.sameSenders(stable_digest)) {
                if(log.isDebugEnabled()) {
                    log.debug(local_addr + ": received digest from " + sender + " (digest=" + stable_digest + ") which does not match my own digest ("+
                            this.digest + "): ignoring digest and re-initializing own digest");
                }
                resetDigest();
                return;
            }
            num_stability_msgs_received++;
            resetDigest();
        }
        finally {
            lock.unlock();
        }

        // pass STABLE event down the stack, so NAKACK can garbage collect old messages
        down_prot.down(new Event(Event.STABLE, stable_digest));
        num_bytes_received=0; // reset, so all members have more or less the same value
    }




    /**
     * Bcasts a STABLE message of the current digest to all members. Message contains highest seqnos of all members
     * seen by this member. Highest seqnos are retrieved from the NAKACK layer below.
     * @param d A <em>copy</em> of this.digest
     */
    protected void sendStableMessage(Digest d) {
        if(suspended) {
            if(log.isTraceEnabled())
                log.trace("will not send STABLE message as I'm suspended");
            return;
        }

        if(d != null && d.size() > 0) {
            Address dest=send_stable_msgs_to_coord_only? coordinator : null;
            if(log.isTraceEnabled())
                log.trace(local_addr + ": sending stable msg to " + (send_stable_msgs_to_coord_only? coordinator : "cluster") +
                            ": " + d.printHighestDeliveredSeqnos());
            num_stable_msgs_sent++;
            final Message msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.NO_RELIABILITY)
              .putHeader(this.id,new StableHeader(StableHeader.STABLE_GOSSIP,d));

            Runnable r=new Runnable() {
                public void run() {
                    down_prot.down(new Event(Event.MSG, msg));
                }

                public String toString() {return STABLE.class.getSimpleName() + ": STABLE-GOSSIP";}
            };

            // Run in a separate thread so we don't potentially block (http://jira.jboss.com/jira/browse/JGRP-532)
            timer.execute(r);
        }
    }



    /**
     Schedules a stability message to be mcast after a random number of milliseconds (range 1-5 secs).
     The reason for waiting a random amount of time is that, in the worst case, all members receive a
     STABLE_GOSSIP message from the last outstanding member at the same time and would therefore mcast the
     STABILITY message at the same time too. To avoid this, each member waits random N msecs. If, before N
     elapses, some other member sent the STABILITY message, we just cancel our own message. If, during
     waiting for N msecs to send STABILITY message S1, another STABILITY message S2 is to be sent, we just
     discard S2.
     @param tmp A copy of te stability digest, so we don't need to copy it again
     */
    protected void sendStabilityMessage(Digest tmp) {
        long delay;

        if(suspended) {
            if(log.isTraceEnabled())
                log.trace("STABILITY message will not be sent as I'm suspended");
            return;
        }

        // give other members a chance to mcast STABILITY message. if we receive STABILITY by the end of our random
        // sleep, we will not send the STABILITY msg. this prevents that all mbrs mcast a STABILITY msg at the same time
        delay=Util.random(stability_delay);
        if(log.isTraceEnabled()) log.trace(local_addr + ": sending stability msg (in " + delay + " ms) " + tmp.printHighestDeliveredSeqnos());
        startStabilityTask(tmp, delay);
    }


    protected Digest getDigest() {
        return (Digest)down_prot.down(Event.GET_DIGEST_EVT);
    }


    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class StableHeader extends Header {
        public static final int STABLE_GOSSIP=1;
        public static final int STABILITY=2;

        protected int    type;
        protected Digest stableDigest; // changed by Bela April 4 2004

        public StableHeader() {
        }


        public StableHeader(int type, Digest digest) {
            this.type=type;
            this.stableDigest=digest;
        }


        static String type2String(int t) {
            switch(t) {
                case STABLE_GOSSIP:
                    return "STABLE_GOSSIP";
                case STABILITY:
                    return "STABILITY";
                default:
                    return "<unknown>";
            }
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append('[');
            sb.append(type2String(type));
            sb.append("]: digest is ");
            sb.append(stableDigest);
            return sb.toString();
        }

        public int size() {
            int retval=Global.INT_SIZE + Global.BYTE_SIZE; // type + presence for digest
            if(stableDigest != null)
                retval+=stableDigest.serializedSize();
            return retval;
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeInt(type);
            Util.writeStreamable(stableDigest, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readInt();
            stableDigest=(Digest)Util.readStreamable(Digest.class, in);
        }
    }




    /**
     Mcast periodic STABLE message. Interval between sends varies.
     */
    protected class StableTask implements TimeScheduler.Task {

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return desired_avg_gossip / 2;
            else
                return interval;
        }


        public void run() {
            if(suspended) {
                if(log.isTraceEnabled())
                    log.trace("stable task will not run as suspended=" + suspended);
                return;
            }

            Digest my_digest=getDigest(); // asks the NAKACK protocol for the current digest
            if(my_digest == null) {
                if(log.isWarnEnabled())
                    log.warn("received null digest, skipped sending of stable message");
                return;
            }
            if(log.isTraceEnabled())
                log.trace(local_addr + ": setting latest_local_digest from NAKACK: " + my_digest.printHighestDeliveredSeqnos());
            sendStableMessage(my_digest);
        }

        public String toString() {return STABLE.class.getSimpleName() + ": StableTask";}

        long computeSleepTime() {
            return getRandom((desired_avg_gossip * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }





    /**
     * Multicasts a STABILITY message.
     */
    protected class StabilitySendTask implements Runnable {
        Digest stability_digest=null;

        StabilitySendTask(Digest d) {
            this.stability_digest=d;
        }

        public void run() {
            if(suspended) {
                if(log.isDebugEnabled()) {
                    log.debug("STABILITY message will not be sent as suspended=" + suspended);
                }
                return;
            }

            if(stability_digest != null) {
                Message msg=new Message().setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
                StableHeader hdr=new StableHeader(StableHeader.STABILITY, stability_digest);
                msg.putHeader(id, hdr);
                if(log.isTraceEnabled()) log.trace(local_addr + ": sending stability msg " + stability_digest.printHighestDeliveredSeqnos());
                num_stability_msgs_sent++;
                down_prot.down(new Event(Event.MSG, msg));
            }
        }

        public String toString() {return STABLE.class.getSimpleName() + ": StabilityTask";}
    }


    protected class ResumeTask implements Runnable {

        public void run() {
            if(suspended)
                log.warn("ResumeTask resumed message garbage collection - this should be done by a RESUME_STABLE event; " +
                           "check why this event was not received (or increase max_suspend_time for large state transfers)");
            resume();
        }

        public String toString() {return STABLE.class.getSimpleName() + ": ResumeTask";}
    }


}

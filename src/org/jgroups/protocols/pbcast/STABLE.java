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
    protected volatile View       view;

    @GuardedBy("lock")
    protected volatile MutableDigest digest; // keeps track of the highest seqnos from all members

    /**
     * Keeps track of who we already heard from (STABLE_GOSSIP msgs). This is all 0's, and we set the sender
     * when a STABLE message is received. When the bitset is all 1's (responses from all members), we send a STABILITY message
     */
    @GuardedBy("lock")
    protected FixedSizeBitSet     votes;

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

    // @ManagedAttribute(name="bytes_received")
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
        return Arrays.asList(Event.GET_DIGEST);
    }

    protected void suspend(long timeout) {
        if(!suspended) {
            suspended=true;
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

        // we're the only one who sends out STABILITY messages; no need to wait for others to send it (as they won't)
        if(send_stable_msgs_to_coord_only)
            stability_delay=0;
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

                handleUpEvent(hdr, msg.getSrc(), readDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength()));
                return null;  // don't pass STABLE or STABILITY messages up the stack

            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange((View)evt.getArg());
                return retval;
        }
        return up_prot.up(evt);
    }

    protected void handleUpEvent(StableHeader hdr, Address sender, Digest digest) {
        switch(hdr.type) {
            case StableHeader.STABLE_GOSSIP:
                handleStableMessage(digest, sender, hdr.view_id);
                break;
            case StableHeader.STABILITY:
                handleStabilityMessage(digest, sender, hdr.view_id);
                break;
            default:
                log.error("%s: StableHeader type %s not known", local_addr, hdr.type);
        }
    }


    public void up(MessageBatch batch) {
        StableHeader hdr;

        for(Message msg: batch) { // remove and handle messages with flow control headers (STABLE_GOSSIP, STABILITY)
            if((hdr=(StableHeader)msg.getHeader(id)) != null) {
                batch.remove(msg);
                handleUpEvent(hdr, batch.sender(), readDigest(msg.getRawBuffer(), msg.getOffset(), msg.getLength()));
            }
        }

        // only if message counting is on, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes > 0 && batch.dest() == null && !batch.isEmpty()) {
            boolean send_stable_msg=false;
            received.lock();
            try {
                num_bytes_received+=batch.length();
                if(num_bytes_received >= max_bytes) {
                    log.trace("max_bytes has been reached (%s, bytes received=%s): triggers stable msg",
                              max_bytes, num_bytes_received);
                    num_bytes_received=0;
                    send_stable_msg=true;
                }
            }
            finally {
                received.unlock();
            }

            if(send_stable_msg)
                sendStableMessage(true);
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void handleRegularMessage(Message msg) {
        // only if bytes counting is enabled, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes <= 0)
            return;
        if(msg.getDest() == null) {
            boolean send_stable_msg=false;
            received.lock();
            try {
                num_bytes_received+=msg.getLength();
                if(num_bytes_received >= max_bytes) {
                    log.trace("max_bytes has been reached (%s, bytes received=%s): triggers stable msg",
                              max_bytes, num_bytes_received);
                    num_bytes_received=0;
                    send_stable_msg=true;
                }
            }
            finally {
                received.unlock();
            }

            if(send_stable_msg)
                sendStableMessage(true);
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=down_prot.down(evt);
                handleViewChange((View)evt.getArg());
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


    @ManagedOperation(description="Sends a STABLE message; when every member has received a STABLE message " +
      "from everybody else, a STABILITY message will be sent")
    public void gc() {
        sendStableMessage(false);
    }


    /* --------------------------------------- Private Methods ---------------------------------------- */


    protected void handleViewChange(View v) {
        lock.lock();
        try {
            this.view=v;
            coordinator=v.getMembers().get(0);
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
    protected void updateLocalDigest(Digest d, Address sender) {
        StringBuilder sb=null;
        if(log.isTraceEnabled())
            sb=new StringBuilder().append(local_addr).append(": handling digest from ").append(sender).append(":\nmine:   ")
              .append(printDigest(digest)).append("\nother:  ").append(printDigest(d));

        for(Digest.Entry entry: d) {
            Address mbr=entry.getMember();
            long hd=entry.getHighestDeliveredSeqno(), hr=entry.getHighestReceivedSeqno();

            // compute the minimum of the highest seqnos deliverable (for garbage collection)
            long[] seqnos=digest.get(mbr);
            if(seqnos == null)
                continue;
            long my_hd=seqnos[0];
            long my_hr=seqnos[1]; // (for retransmission of last missing message)

            if(my_hd == -1) // -1 means the seqno hasn't been set yet
                my_hd=hd;

            long new_hd=Math.min(my_hd, hd);
            long new_hr=Math.max(my_hr, hr);
            digest.set(mbr, new_hd, new_hr);
        }
        if(sb != null) // implies log.isTraceEnabled() == true
            log.trace(sb.append("\nresult: ").append(printDigest(digest)).append("\n"));
    }


    @GuardedBy("lock")
    protected void resetDigest() {
        if(view == null)
            return;
        digest=new MutableDigest(view.getMembersRaw()); // .set(getDigest());
        log.trace("%s: reset digest to %s", local_addr, printDigest(digest));
        votes=new FixedSizeBitSet(view.size()); // all 0's initially
    }

    /**
     * Adds mbr to votes and returns true if we have all the votes, otherwise false.
     * @param rank
     */
    @GuardedBy("lock")
    protected boolean addVote(int rank) {
        try {
            return votes.set(rank) && allVotesReceived(votes);
        }
        catch(Throwable t) {
            return false;
        }
    }

    /** Votes is already locked and guaranteed to be non-null */
    @GuardedBy("lock")
    protected static boolean allVotesReceived(FixedSizeBitSet votes) {
        return votes.cardinality() == votes.size();
    }

    protected static int getRank(Address member, View v) {
        if(v == null || member == null)
            return -1;
        Address[] members=v.getMembersRaw();
        for(int i=0; i < members.length; i++)
            if(member.equals(members[i]))
                return i;
        return -1;
    }

    protected void startStableTask() {
        stable_task_lock.lock();
        try {
            if(stable_task_future == null || stable_task_future.isDone()) {
                StableTask stable_task=new StableTask();
                stable_task_future=timer.scheduleWithDynamicInterval(stable_task);
                log.trace("%s: stable task started", local_addr);
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
                log.debug("%s: resume task started, max_suspend_time=%d", local_addr, max_suspend_time);
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


    protected void startStabilityTask(Digest d, ViewId view_id, long delay) {
        stability_lock.lock();
        try {
            if(stability_task_future == null || stability_task_future.isDone()) {
                StabilitySendTask stability_task=new StabilitySendTask(d, view_id); // runs only once
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
    protected void handleStableMessage(final Digest d, final Address sender, final ViewId view_id) {
        if(d == null || sender == null) {
            if(log.isErrorEnabled()) log.error("digest or sender is null");
            return;
        }

        if(!initialized || suspended) {
            log.trace("%s: STABLE message is ignored: initialized=%b, suspended=%b", local_addr, initialized, suspended);
            return;
        }

        // Check if STABLE message is from the same view
        if(!view_id.equals(view.getViewId())) {
            log.trace("%s: discarded STABLE message with different view-id %s (my view-id=%s)",
                      local_addr, view_id, view.getViewId());
            return;
        }

        Digest stable_digest=null;
        ViewId stable_view_id=null;
        lock.lock();
        try {
            int rank=getRank(sender, view);
            if(rank < 0 || votes.get(rank))  // already received gossip from sender; discard it
                return;
            num_stable_msgs_received++;
            updateLocalDigest(d, sender);
            if(addVote(rank)) {       // votes from all members have been received
                stable_digest=digest; // no need to copy, as digest (although mutable) is reassigned below
                stable_view_id=view.getViewId();
                resetDigest();        // sets digest
            }
        }
        catch(Throwable t) {
            return;
        }
        finally {
            lock.unlock();
        }

        // we don't yet reset digest: new STABLE messages will be discarded anyway as we have already
        // received votes from their senders
        if(stable_digest != null) {
            resetNumBytes();
            sendStabilityMessage(stable_digest, stable_view_id);
            // we discard our own STABILITY message: pass it down now, so NAKACK can purge old messages
            down_prot.down(new Event(Event.STABLE, stable_digest));
        }
    }

    protected void resetNumBytes() {
        received.lock();
        try {
            num_bytes_received=0; // reset, so all members have more or less the same value
        }
        finally {
            received.unlock();
        }
    }


    protected void handleStabilityMessage(final Digest stable_digest, final Address sender, final ViewId view_id) {
        if(stable_digest == null) {
            if(log.isErrorEnabled()) log.error("stability digest is null");
            return;
        }

        if(!initialized || suspended) {
            log.trace("%s: STABLE message is ignored: initialized=%b, suspended=%b", local_addr, initialized, suspended);
            return;
        }

        // received my own STABILITY message - no need to handle it as I already reset my digest before I sent the msg
        if(local_addr != null && local_addr.equals(sender)) {
            num_stability_msgs_received++;
            return;
        }

        stopStabilityTask();

        lock.lock();
        try {
            // we won't handle the stable_digest, if its members don't match the membership in my own digest,
            // this is part of the fix for the NAKACK problem (bugs #943480 and #938584)
            if(!view_id.equals(this.view.getViewId())) {
                log.trace("%s: discarded STABILITY message with different view-id %s (my view-id=%s)",
                          local_addr, view_id, view);
                // resetDigest();
                return;
            }
            log.trace("%s: received stability msg from %s: %s", local_addr, sender, printDigest(stable_digest));

            num_stability_msgs_received++;
            resetDigest();
        }
        finally {
            lock.unlock();
        }

        resetNumBytes();
        down_prot.down(new Event(Event.STABLE, stable_digest)); // pass STABLE down, so NAKACK{2} can purge stable messages
    }

    /**
     * Broadcasts a STABLE message of the current digest to all members (or the coordinator only). The message contains
     * the highest seqno delivered and received for all members. The seqnos are retrieved from the NAKACK layer below.
     */
    protected void sendStableMessage(boolean send_in_background) {
        if(suspended || view == null)
            return;

        final View          current_view=view;
        final MutableDigest d=new MutableDigest(current_view.getMembersRaw()).set(getDigest());
        Address dest=send_stable_msgs_to_coord_only? coordinator : null;

        if(d.allSet() || d.set(getDigest()).allSet()) // try once more if the first digest didn't match
            log.trace("%s: sending stable msg to %s: %s",
                      local_addr, (send_stable_msgs_to_coord_only? coordinator : "cluster"), printDigest(d));
        else {
            log.trace("%s: could not find matching digest for view %s, missing members: %s", local_addr, current_view, d.getNonSetMembers());
            return;
        }

        final Message msg=new Message(dest)
          .setFlag(Message.Flag.OOB,Message.Flag.INTERNAL,Message.Flag.NO_RELIABILITY)
          .putHeader(this.id, new StableHeader(StableHeader.STABLE_GOSSIP, current_view.getViewId()))
          .setBuffer(marshal(d));
        try {
            if(!send_in_background) {
                down_prot.down(new Event(Event.MSG, msg));
                return;
            }
            Runnable r=new Runnable() {
                public void run() {
                    down_prot.down(new Event(Event.MSG, msg));
                    num_stable_msgs_sent++;
                }
                public String toString() {return STABLE.class.getSimpleName() + ": STABLE-GOSSIP";}
            };

            // Run in a separate thread so we don't potentially block (http://jira.jboss.com/jira/browse/JGRP-532)
            timer.execute(r);
        }
        catch(Throwable t) {
            log.warn("failed sending STABLE message", t);
        }
    }


    public static Buffer marshal(Digest digest) {
        return Util.streamableToBuffer(digest);
    }

    protected Digest readDigest(byte[] buffer, int offset, int length) {
        try {
            return buffer != null? Util.streamableFromBuffer(Digest.class, buffer, offset, length) : null;
        }
        catch(Exception ex) {
            log.error("%s: failed reading Digest from message: %s", local_addr, ex);
            return null;
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
    protected void sendStabilityMessage(Digest tmp, final ViewId view_id) {
        // give other members a chance to mcast STABILITY message. if we receive STABILITY by the end of our random
        // sleep, we will not send the STABILITY msg. this prevents that all mbrs mcast a STABILITY msg at the same time
        startStabilityTask(tmp, view_id, Util.random(stability_delay));
    }


    protected Digest getDigest() {
        return (Digest)down_prot.down(Event.GET_DIGEST_EVT);
    }

    protected String printDigest(final Digest digest) {
        if(digest == null)
            return null;
        return view != null? digest.toString(view.getMembersRaw(), false) : digest.toString();
    }

    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class StableHeader extends Header {
        public static final byte STABLE_GOSSIP=1;
        public static final byte STABILITY=2;

        protected byte   type;
        protected ViewId view_id;

        public StableHeader() {
        }

        public StableHeader(byte type, ViewId view_id) {
            this.type=type;
            this.view_id=view_id;
        }

        static String type2String(byte t) {
            switch(t) {
                case STABLE_GOSSIP: return "STABLE_GOSSIP";
                case STABILITY:     return "STABILITY";
                default:            return "<unknown>";
            }
        }

        public String toString() {
            return String.format("[%s] view-id= %s", type2String(type), view_id);
        }

        public int size() {
            return Global.BYTE_SIZE // type
              + Util.size(view_id);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeViewId(view_id, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            view_id=Util.readViewId(in);
        }
    }




    /**
     * Mcast periodic STABLE message. Interval between sends varies.
     */
    protected class StableTask implements TimeScheduler.Task {

        public long nextInterval() {
            long interval=computeSleepTime();
            return interval <= 0? desired_avg_gossip / 2 : interval;
        }

        public void run() {
            if(suspended) {
                log.trace("%s: stable task will not run as suspended=true", local_addr);
                return;
            }
            sendStableMessage(false);
        }

        public String toString() {return STABLE.class.getSimpleName() + ": StableTask";}

        long computeSleepTime() {
            return getRandom((desired_avg_gossip * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }





    /** Multicasts a STABILITY message */
    protected class StabilitySendTask implements Runnable {
        protected final Digest stability_digest;
        protected final ViewId view_id; // ViewId at the time the STABILITY message was created


        protected StabilitySendTask(Digest d, ViewId view_id) {
            this.stability_digest=d;
            this.view_id=view_id;
        }

        public void run() {
            if(suspended) {
                log.debug("STABILITY message will not be sent as suspended=%s", suspended);
                return;
            }

            // https://issues.jboss.org/browse/JGRP-1638: we reverted to sending the STABILITY message *unreliably*,
            // but clear votes *before* sending it
            try {
                Message msg=new Message().setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.NO_RELIABILITY)
                  .putHeader(id, new StableHeader(StableHeader.STABILITY, view_id))
                  .setBuffer(marshal(stability_digest));
                log.trace("%s: sending stability msg %s", local_addr, printDigest(stability_digest));
                num_stability_msgs_sent++;
                down_prot.down(new Event(Event.MSG, msg));
            }
            catch(Exception e) {
                log.warn("failed sending STABILITY message", e);
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

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.TCP;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.jgroups.Message.Flag.NO_RELIABILITY;
import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;


/**
 * Computes the broadcast messages that are stable; i.e., have been delivered by all members. Sends STABLE events down
 * the stack when this is the case. This allows NAKACK{2,3} to garbage collect messages that have been seen by all members.
 * <p>
 * Works as follows: periodically (desired_avg_gossip) or when having received a number of bytes (max_bytes), every
 * member sends its digest (highest seqno delivered, received) to the current coordinator<br/>
 * The coordinator updates a stability vector, which maintains the highest seqno delivered/receive for each member
 * and initially contains no data, when such a message is received. <br/>
 * When messages from all members have been received, a stability message is mcast, which causes all
 * members to send a STABLE event down the stack (triggering garbage collection in the NAKACK{2,3} layer).
 * @author Bela Ban
 */
@MBean(description="Computes the broadcast messages that are stable")
public class STABLE extends Protocol {
    protected static final long MAX_SUSPEND_TIME=200000;

    /* ------------------------------------------ Properties  ------------------------------------------ */

    /**
     * Sends a STABLE gossip every 20 seconds on average. 0 disables gossiping of STABLE messages
     */
    @Property(description="Average time to send a STABLE message",type=AttributeType.TIME)
    protected long   desired_avg_gossip=20000;

    /**
     * Total amount of bytes from incoming messages (default = 0 = disabled). When exceeded, a STABLE message will
     * be broadcast and{@code num_bytes_received} reset to 0 . If this is > 0, then ideally {@code stability_delay}
     * should be set to a low number as well
     */
    @Property(description="Maximum number of bytes received in all messages before sending a STABLE message is triggered",
      type=AttributeType.BYTES)
    protected long   max_bytes=2000000;


    /* --------------------------------------------- JMX  ---------------------------------------------- */
    protected int    num_stable_msgs_sent;
    protected int    num_stable_msgs_received;
    protected int    num_stability_msgs_sent;
    protected int    num_stability_msgs_received;

    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected volatile View          view;

    @GuardedBy("lock")
    protected volatile MutableDigest digest; // keeps track of the highest seqnos from all members

    /**
     * Keeps track of who we already heard from (STABLE_GOSSIP msgs). This is all 0's, and we set the sender
     * when a STABLE message is received. When the bitset is all 1's (responses from all members), we send a STABILITY message
     */
    @GuardedBy("lock")
    protected FixedSizeBitSet     votes;

    protected final Lock          lock=new ReentrantLock();

    @GuardedBy("stable_task_lock")
    protected Future<?>           stable_task_future; // bcasts periodic STABLE message (added to timer below)
    protected final Lock          stable_task_lock=new ReentrantLock(); // to sync on stable_task

    protected TimeScheduler       timer; // to send periodic STABLE msgs (and STABILITY messages)

    /** The total number of bytes received from unicast and multicast messages */
    @GuardedBy("received")
    @ManagedAttribute(description="Bytes accumulated so far",type=AttributeType.BYTES)
    protected long                num_bytes_received;

    protected final Lock          received=new ReentrantLock();

    /**
     * When true, don't take part in garbage collection: neither send STABLE messages nor handle STABILITY messages
     */
    @ManagedAttribute
    protected volatile boolean    suspended;

    protected boolean             initialized;

    protected Future<?>           resume_task_future;
    protected final Object        resume_task_mutex=new Object();

    @ManagedAttribute(description="The coordinator")
    protected volatile Address    coordinator;

    
    
    public STABLE() {             
    }

    public long   getDesiredAverageGossip()       {return desired_avg_gossip;}
    public STABLE setDesiredAverageGossip(long g) {desired_avg_gossip=g; return this;}
    public long   getMaxBytes()                   {return max_bytes;}
    public STABLE setMaxBytes(long m)             {this.max_bytes=m; return this;}

    // @ManagedAttribute(name="bytes_received")
    public long getBytes() {return num_bytes_received;}
    @ManagedAttribute(type=AttributeType.SCALAR)
    public int getStableSent() {return num_stable_msgs_sent;}
    @ManagedAttribute(type=AttributeType.SCALAR)
    public int getStableReceived() {return num_stable_msgs_received;}
    @ManagedAttribute(type=AttributeType.SCALAR)
    public int getStabilitySent() {return num_stability_msgs_sent;}
    @ManagedAttribute(type=AttributeType.SCALAR)
    public int getStabilityReceived() {return num_stability_msgs_received;}
    @ManagedAttribute(description="The number of votes for the current digest")
    public int getNumVotes() {return votes != null? votes.cardinality() : 0;}

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


    @ManagedOperation(description="Sends a STABLE message; when every member has received a STABLE message " +
      "from everybody else, a STABILITY message will be sent")
    public void gc() {
        sendStableMessage(false);
    }

    @ManagedOperation(description="Prints the current digest")
    public String printDigest() {
        return printDigest(digest);
    }

    @ManagedOperation(description="Prints the current votes")
    public String printVotes() {
        return votes != null? votes.toString() : "n/a";
    }

    public void resetStats() {
        super.resetStats();
        num_stability_msgs_received=num_stability_msgs_sent=num_stable_msgs_sent=num_stable_msgs_received=0;
    }


    public List<Integer> requiredDownServices() {
        return Collections.singletonList(Event.GET_DIGEST);
    }

    protected void suspend(long timeout) {
        if(!suspended) {
            suspended=true;
            log.debug("%s: suspending message garbage collection", local_addr);
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

        log.debug("%s: resuming message garbage collection", local_addr);
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
            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange(evt.getArg());
                return retval;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        StableHeader hdr=msg.getHeader(this.id);
        if(hdr == null) {
            handleRegularMessage(msg);
            return up_prot.up(msg);
        }
        return handle(hdr, msg.getSrc(), msg.getObject()); // don't pass STABLE or STABILITY messages up the stack
    }

    public void up(MessageBatch batch) {
        StableHeader hdr;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) { // remove / handle msgs with headers STABLE_GOSSIP, STABILITY
            Message msg=it.next();
            if((hdr=msg.getHeader(id)) != null) {
                it.remove();
                handle(hdr, batch.sender(), msg.getObject());
            }
        }

        // only if message counting is on, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes > 0 && batch.dest() == null && !batch.isEmpty() && maxBytesExceeded(batch.length()))
            sendStableMessage(true);

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public Object down(Message msg) {
        boolean send_stable_msg=max_bytes > 0 && msg.getDest() == null && msg.isFlagSet(DONT_LOOPBACK) && maxBytesExceeded(msg.getLength());
        Object retval=down_prot.down(msg);
        if(send_stable_msg)
            sendStableMessage(true);
        return retval;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                Object retval=down_prot.down(evt);
                handleViewChange(evt.getArg());
                return retval;

            case Event.SUSPEND_STABLE:
                long timeout=MAX_SUSPEND_TIME;
                Object t=evt.getArg();
                if(t instanceof Long)
                    timeout=(Long)t;
                suspend(timeout);
                break;

            case Event.RESUME_STABLE:
                resume();
                break;
        }
        return down_prot.down(evt);
    }


    protected Object handle(StableHeader hdr, Address sender, Digest digest) {
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
        return null;
    }

    protected void handleRegularMessage(Message msg) {
        // only if bytes counting is enabled, and only for multicast messages (http://jira.jboss.com/jira/browse/JGRP-233)
        if(max_bytes > 0 && msg.getDest() == null &&  maxBytesExceeded(msg.getLength()))
            sendStableMessage(true);
    }

    protected boolean maxBytesExceeded(int len) {
        received.lock();
        try {
            num_bytes_received+=len;
            if(num_bytes_received >= max_bytes) {
                log.trace("%s: max_bytes (%d) has been exceeded; bytes received=%d: triggers stable msg",
                          local_addr, max_bytes, num_bytes_received);
                num_bytes_received=0;
                return true;
            }
            return false;
        }
        finally {
            received.unlock();
        }
    }


    protected void handleViewChange(View v) {
        lock.lock();
        try {
            this.view=v;
            coordinator=v.getCoord();
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
              .append(printDigest(digest)).append("\nsender: ").append(printDigest(d));

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
                stable_task_future=timer.scheduleWithDynamicInterval(stable_task, getTransport() instanceof TCP);
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
                resume_task_future=timer.schedule(resume_task, max_suspend_time, TimeUnit.MILLISECONDS, false);
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
            if(log.isErrorEnabled()) log.error(Util.getMessage("DigestOrSenderIsNull"));
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
            if(log.isErrorEnabled()) log.error(Util.getMessage("StabilityDigestIsNull"));
            return;
        }
        if(!initialized || suspended) {
            log.trace("%s: STABLE message is ignored: initialized=%b, suspended=%b", local_addr, initialized, suspended);
            return;
        }

        lock.lock();
        try {
            // we won't handle the stable_digest, if its members don't match the membership in my own digest,
            // this is part of the fix for the NAKACK problem (bugs #943480 and #938584)
            if(!view_id.equals(this.view.getViewId())) {
                log.trace("%s: discarded STABILITY message with different view-id %s (my view-id=%s)",
                          local_addr, view_id, view);
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
        View          current_view=view;
        Address       dest=coordinator;
        boolean       is_coord=Objects.equals(local_addr, coordinator);
        MutableDigest d=new MutableDigest(current_view.getMembersRaw()).set(getDigest());
        boolean       all_set=d.allSet() || d.set(getDigest()).allSet();

        if(!all_set) {
            log.trace("%s: could not find matching digest for view %s, missing members: %s",
                      local_addr, current_view, d.getNonSetMembers());
            return;
        }
        // don't send a STABLE message to self when coord, but instead update the digest directly
        if(is_coord) {
            log.trace("%s: updating the local digest with a stable message (coordinator): %s", local_addr, d);
            num_stable_msgs_sent++;
            handleStableMessage(d, local_addr, current_view.getViewId());
            return;
        }
        log.trace("%s: sending stable msg to %s: %s", local_addr, dest, printDigest(d));
        final Message msg=new ObjectMessage(dest, d).setFlag(OOB, NO_RELIABILITY)
          .putHeader(this.id, new StableHeader(StableHeader.STABLE_GOSSIP, current_view.getViewId()));
        try {
            if(!send_in_background) {
                num_stable_msgs_sent++;
                down_prot.down(msg);
                return;
            }
            Runnable r=new Runnable() {
                public void run() {
                    down_prot.down(msg);
                    num_stable_msgs_sent++;
                }

                public String toString() {
                    return STABLE.class.getSimpleName() + ": STABLE-GOSSIP";
                }
            };

            // Run in a separate thread so we don't potentially block (http://jira.jboss.com/jira/browse/JGRP-532)
            timer.execute(r, getTransport() instanceof TCP);
        }
        catch(Throwable t) {
            log.warn("failed sending STABLE message", t);
        }
    }



    /**
     Sends a stability message to all members except self.
     @param d A copy of the stability digest, so we don't need to copy it again
     */
    protected void sendStabilityMessage(Digest d, final ViewId view_id) {
        if(suspended) {
            log.debug("%s: STABILITY message will not be sent as suspended=%b", local_addr, suspended);
            return;
        }

        // https://issues.jboss.org/browse/JGRP-1638: we reverted to sending the STABILITY message *unreliably*,
        // but clear votes *before* sending it
        try {
            Message msg=new ObjectMessage(null, d).setFlag(OOB, NO_RELIABILITY).setFlag(DONT_LOOPBACK)
              .putHeader(id, new StableHeader(StableHeader.STABILITY, view_id));
            log.trace("%s: sending stability msg %s", local_addr, printDigest(d));
            num_stability_msgs_sent++;
            num_stability_msgs_received++; // since we don't receive this message
            down_prot.down(msg);
        }
        catch(Exception e) {
            log.warn("%s: failed sending STABILITY message: %s", local_addr, e);
        }
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

        public short getMagicId() {return 56;}

        public Supplier<? extends Header> create() {return StableHeader::new;}

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

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE // type
              + Util.size(view_id);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Util.writeViewId(view_id, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
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
            return Util.random((desired_avg_gossip * 2));
        }
    }




    protected class ResumeTask implements Runnable {
        public void run() {
            if(suspended)
                log.warn("%s: ResumeTask resumed message garbage collection - this should be done by a RESUME_STABLE event; " +
                           "check why this event was not received (or increase max_suspend_time for large state transfers)", local_addr);
            resume();
        }
        public String toString() {return STABLE.class.getSimpleName() + ": ResumeTask";}
    }


}

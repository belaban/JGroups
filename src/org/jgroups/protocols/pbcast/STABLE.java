package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Computes the broadcast messages that are stable; i.e., have been received by all members. Sends
 * STABLE events up the stack when this is the case. This allows NAKACK to garbage collect messages that
 * have been seen by all members.<p>
 * Works as follows: periodically we mcast our highest seqnos (seen for each member) to the group.
 * A stability vector, which maintains the highest seqno for each member and initially contains no data,
 * is updated when such a message is received. The entry for a member P is computed set to
 * min(entry[P], digest[P]). When messages from all members have been received, a stability
 * message is mcast, which causes all members to send a STABLE event up the stack (triggering garbage collection
 * in the NAKACK layer).<p>
 * New: when <code>max_bytes</code> is exceeded (unless disabled by setting it to 0),
 * a STABLE task will be started (unless it is already running).
 * @author Bela Ban
 * @version $Id: STABLE.java,v 1.77 2007/05/09 23:50:26 belaban Exp $
 */
public class STABLE extends Protocol {
    private Address               local_addr=null;
    private final Vector<Address> mbrs=new Vector<Address>();


    private final MutableDigest digest=new MutableDigest(10);        // keeps track of the highest seqnos from all members
    private final MutableDigest   latest_local_digest=new MutableDigest(10); // keeps track of the latest digests received from NAKACK
    private final Vector<Address> heard_from=new Vector<Address>();      // keeps track of who we already heard from (STABLE_GOSSIP msgs)

    /** Sends a STABLE gossip every 20 seconds on average. 0 disables gossipping of STABLE messages */
    private long                  desired_avg_gossip=20000;

    /** delay before we send STABILITY msg (give others a change to send first). This should be set to a very
     * small number (> 0 !) if <code>max_bytes</code> is used */
    private long                  stability_delay=6000;

    @GuardedBy("stability_lock")
    private Future                stability_task_future=null;
    private final Lock            stability_lock=new ReentrantLock();   // to synchronize on stability_task

    @GuardedBy("stable_task_lock")
    private Future                stable_task_future=null;               // bcasts periodic STABLE message (added to timer below)
    private final Lock            stable_task_lock=new ReentrantLock(); // to sync on stable_task


    private TimeScheduler         timer=null;                     // to send periodic STABLE msgs (and STABILITY messages)
    private static final String   name="STABLE";

    /** Total amount of bytes from incoming messages (default = 0 = disabled). When exceeded, a STABLE
     * message will be broadcast and <code>num_bytes_received</code> reset to 0 . If this is > 0, then ideally
     * <code>stability_delay</code> should be set to a low number as well */
    private long                  max_bytes=0;

    /** The total number of bytes received from unicast and multicast messages */
    @GuardedBy("received")
    private long                  num_bytes_received=0;

    private Lock                  received=new ReentrantLock();

    /** When true, don't take part in garbage collection protocol: neither send STABLE messages nor
     * handle STABILITY messages */
    private boolean               suspended=false;

    private boolean               initialized=false;

    private Future                resume_task_future=null;
    private final Object          resume_task_mutex=new Object();

    private int num_stable_msgs_sent=0;
    private int num_stable_msgs_received=0;
    private int num_stability_msgs_sent=0;
    private int num_stability_msgs_received=0;
    
    private static final long MAX_SUSPEND_TIME=200000;


    public String getName() {
        return name;
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

    public long getBytes() {return num_bytes_received;}
    public int getStableSent() {return num_stable_msgs_sent;}
    public int getStableReceived() {return num_stable_msgs_received;}
    public int getStabilitySent() {return num_stability_msgs_sent;}
    public int getStabilityReceived() {return num_stability_msgs_received;}


    public void resetStats() {
        super.resetStats();
        num_stability_msgs_received=num_stability_msgs_sent=num_stable_msgs_sent=num_stable_msgs_received=0;
    }


    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval=new Vector<Integer>();
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));  // NAKACK layer
        return retval;
    }

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("digest_timeout");
        if(str != null) {
            props.remove("digest_timeout");
            log.error("digest_timeout has been deprecated; it will be ignored");
        }

        str=props.getProperty("desired_avg_gossip");
        if(str != null) {
            desired_avg_gossip=Long.parseLong(str);
            props.remove("desired_avg_gossip");
        }

        str=props.getProperty("stability_delay");
        if(str != null) {
            stability_delay=Long.parseLong(str);
            props.remove("stability_delay");
        }

        str=props.getProperty("max_gossip_runs");
        if(str != null) {
            props.remove("max_gossip_runs");
            log.error("max_gossip_runs has been deprecated and will be ignored");
        }

        str=props.getProperty("max_bytes");
        if(str != null) {
            max_bytes=Long.parseLong(str);
            props.remove("max_bytes");
        }

        str=props.getProperty("max_suspend_time");
        if(str != null) {
            log.error("max_suspend_time is not supported any longer; please remove it (ignoring it)");
            props.remove("max_suspend_time");
        }

        Util.checkBufferSize("STABLE.max_bytes", max_bytes);

        if(!props.isEmpty()) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    private void suspend(long timeout) {
        if(!suspended) {
            suspended=true;
            if(log.isDebugEnabled())
                log.debug("suspending message garbage collection");
        }
        startResumeTask(timeout); // will not start task if already running
    }

    private void resume() {
        resetDigest(mbrs); // start from scratch
        suspended=false;
        if(log.isDebugEnabled())
            log.debug("resuming message garbage collection");
        stopResumeTask();
    }

    public void start() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("timer cannot be retrieved from protocol stack");
        if(desired_avg_gossip > 0)
            startStableTask();
    }

    public void stop() {
        stopStableTask();
        clearDigest();
    }


    public Object up(Event evt) {
        Message msg;
        StableHeader hdr;
        int type=evt.getType();

        switch(type) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            hdr=(StableHeader)msg.getHeader(name);
            if(hdr == null) {
                handleRegularMessage(msg);
                return up_prot.up(evt);
            }

            switch(hdr.type) {
            case StableHeader.STABLE_GOSSIP:
                handleStableMessage(msg.getSrc(), hdr.stableDigest);
                break;
            case StableHeader.STABILITY:
                handleStabilityMessage(hdr.stableDigest, msg.getSrc());
                break;
            default:
                if(log.isErrorEnabled()) log.error("StableHeader type " + hdr.type + " not known");
            }
            return null;  // don't pass STABLE or STABILITY messages up the stack

        case Event.VIEW_CHANGE:
            Object retval=up_prot.up(evt);
            View view=(View)evt.getArg();
            handleViewChange(view);
            return retval;

        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
        }
        return up_prot.up(evt);
    }



    private void handleRegularMessage(Message msg) {
        // only if message counting is enabled, and only for multicast messages
        // fixes http://jira.jboss.com/jira/browse/JGRP-233
        if(max_bytes <= 0)
            return;
        Address dest=msg.getDest();
        if(dest == null || dest.isMulticastAddress()) {
            received.lock();
            boolean locked=true;
            try {
                num_bytes_received+=(long)msg.getLength();
                if(num_bytes_received >= max_bytes) {
                    if(log.isTraceEnabled()) {
                        log.trace(new StringBuilder("max_bytes has been reached (").append(max_bytes).
                                append(", bytes received=").append(num_bytes_received).append("): triggers stable msg"));
                    }
                    num_bytes_received=0;
                    received.unlock();
                    locked=false;

                    // asks the NAKACK protocol for the current digest,
                    Digest my_digest=(Digest)down_prot.down(Event.GET_DIGEST_STABLE_EVT);
                    synchronized(latest_local_digest) {
                        latest_local_digest.replace(my_digest);
                    }
                    if(log.isTraceEnabled())
                        log.trace("setting latest_local_digest from NAKACK: " + my_digest.printHighestDeliveredSeqnos());
                    sendStableMessage(my_digest);
                }
            }
            finally {
                if(locked)
                    received.unlock();
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
            long timeout=0;
            Object t=evt.getArg();
            if(t != null && t instanceof Long)
                timeout=((Long)t).longValue();
            suspend(timeout);
            break;

        case Event.RESUME_STABLE:
            resume();
            break;
        }
        return down_prot.down(evt);
    }


    public void runMessageGarbageCollection() {
        Digest copy;
        synchronized(digest) {
            copy=digest.copy();
        }
        sendStableMessage(copy);
    }



    /* --------------------------------------- Private Methods ---------------------------------------- */


    private void handleViewChange(View v) {
        Vector<Address> tmp=v.getMembers();
        mbrs.clear();
        mbrs.addAll(tmp);
        adjustSenders(digest, tmp);
        adjustSenders(latest_local_digest, tmp);

        // asks the NAKACK protocol for the current digest
        Digest my_digest=(Digest)down_prot.down(Event.GET_DIGEST_STABLE_EVT);
        if(my_digest != null) {
            synchronized(latest_local_digest) {
                latest_local_digest.replace(my_digest);
            }
        }

        resetDigest(tmp);
        if(!initialized)
            initialized=true;
    }


    /** Digest and members are guaranteed to be non-null */
    private static void adjustSenders(MutableDigest digest, Vector members) {
        synchronized(digest) {
            // 1. remove all members from digest who are not in the view
            Iterator<Address> it=digest.getSenders().keySet().iterator();
            Address mbr;
            while(it.hasNext()) {
                mbr=it.next();
                if(!members.contains(mbr))
                    it.remove();
            }
            // 2. add members to digest which are in the new view but not in the digest
            for(int i=0; i < members.size(); i++) {
                mbr=(Address)members.get(i);
                if(!digest.contains(mbr))
                    digest.add(mbr, -1, -1);
            }
        }
    }


    private void clearDigest() {
        synchronized(digest) {
            digest.clear();
        }
    }



    /** Update my own digest from a digest received by somebody else. Returns whether the update was successful.
     *  Needs to be called with a lock on digest */
    private boolean updateLocalDigest(Digest d, Address sender) {
        if(d == null || d.size() == 0)
            return false;

        if(!initialized) {
            if(log.isTraceEnabled())
                log.trace("STABLE message will not be handled as I'm not yet initialized");
            return false;
        }

        if(!digest.sameSenders(d)) {
//            if(log.isTraceEnabled())
//                log.trace(new StringBuffer("received a digest ").append(d.printHighSeqnos()).append(" from ").
//                          append(sender).append(" which has different members than mine (").
//                          append(digest.printHighSeqnos()).append("), discarding it and resetting heard_from list"));
            // to avoid sending incorrect stability/stable msgs, we simply reset our heard_from list, see DESIGN
            resetDigest(mbrs);
            return false;
        }

        StringBuffer sb=null;
        if(log.isTraceEnabled()) {
            sb=new StringBuffer("[").append(local_addr).append("] handling digest from ").append(sender).append(":\n");
            sb.append("mine:   ").append(digest.printHighestDeliveredSeqnos()).append("\nother:  ").append(d.printHighestDeliveredSeqnos());
        }
        Address mbr;
        long highest_seqno, my_highest_seqno, new_highest_seqno, my_low, low, new_low;
        long highest_seen_seqno, my_highest_seen_seqno, new_highest_seen_seqno;
        Map.Entry<Address,Digest.Entry> entry;
        Digest.Entry val;
        for(Iterator<Map.Entry<Address, Digest.Entry>> it=d.getSenders().entrySet().iterator(); it.hasNext();) {
            entry=it.next();
            mbr=entry.getKey();
            val=entry.getValue();
            low=val.getLow();
            highest_seqno=val.getHighestDeliveredSeqno();      // highest *delivered* seqno
            highest_seen_seqno=val.getHighestReceivedSeqno();  // highest *received* seqno

            my_low=digest.lowSeqnoAt(mbr);
            new_low=Math.min(my_low, low);

            // compute the minimum of the highest seqnos deliverable (for garbage collection)
            my_highest_seqno=digest.highestDeliveredSeqnoAt(mbr);
            // compute the maximum of the highest seqnos seen (for retransmission of last missing message)
            my_highest_seen_seqno=digest.highestReceivedSeqnoAt(mbr);

            new_highest_seqno=Math.min(my_highest_seqno, highest_seqno);
            new_highest_seen_seqno=Math.max(my_highest_seen_seqno, highest_seen_seqno);
            digest.setHighestDeliveredAndSeenSeqnos(mbr, new_low, new_highest_seqno, new_highest_seen_seqno);
        }
        if(log.isTraceEnabled()) {
            sb.append("\nresult: ").append(digest.printHighestDeliveredSeqnos()).append("\n");
            log.trace(sb);
        }
        return true;
    }



    private void resetDigest(Vector<Address> new_members) {
        if(new_members == null || new_members.isEmpty())
            return;
        synchronized(heard_from) {
            heard_from.clear();
            heard_from.addAll(new_members);
        }

        Digest copy_of_latest;
        synchronized(latest_local_digest) {
            copy_of_latest=latest_local_digest.copy();
        }
        synchronized(digest) {
            digest.replace(copy_of_latest);
            if(log.isTraceEnabled())
                log.trace("resetting digest from NAKACK: " + copy_of_latest.printHighestDeliveredSeqnos());
        }
    }

    /**
     * Removes mbr from heard_from and returns true if this was the last member, otherwise false.
     * Resets the heard_from list (populates with membership)
     * @param mbr
     */
    private boolean removeFromHeardFromList(Address mbr) {
        synchronized(heard_from) {
            heard_from.remove(mbr);
            if(heard_from.isEmpty()) {
                resetDigest(this.mbrs);
                return true;
            }
        }
        return false;
    }


    private void startStableTask() {
        stable_task_lock.lock();
        try {
            if(stable_task_future == null || stable_task_future.isDone()) {
                StableTask stable_task=new StableTask();
                stable_task_future=timer.scheduleWithDynamicInterval(stable_task, true);
                if(log.isTraceEnabled())
                    log.trace("stable task started");
            }
        }
        finally {
            stable_task_lock.unlock();
        }
    }


    private void stopStableTask() {
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


    private void startResumeTask(long max_suspend_time) {
        max_suspend_time=(long)(max_suspend_time * 1.1); // little slack
        if(max_suspend_time <= 0)
            max_suspend_time=MAX_SUSPEND_TIME;

        synchronized(resume_task_mutex) {
            if(resume_task_future == null || resume_task_future.isDone()) {
                ResumeTask resume_task=new ResumeTask();
                resume_task_future=timer.schedule(resume_task, max_suspend_time, TimeUnit.MILLISECONDS); // fixed-rate scheduling
                if(log.isDebugEnabled())
                    log.debug("resume task started, max_suspend_time=" + max_suspend_time);
            }
        }

    }


    private void stopResumeTask() {
        synchronized(resume_task_mutex) {
            if(resume_task_future != null) {
                resume_task_future.cancel(false);
                resume_task_future=null;
            }
        }
    }


    private void startStabilityTask(Digest d, long delay) {
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


    private void stopStabilityTask() {
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
    private void handleStableMessage(Address sender, Digest d) {
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

        if(!heard_from.contains(sender)) {  // already received gossip from sender; discard it
            return;
        }

        num_stable_msgs_received++;

        Digest copy;
        synchronized(digest) {
            boolean success=updateLocalDigest(d, sender);
            if(!success) // we can only remove the sender from heard_from if *all* elements of my digest were updated
                return;
            copy=digest.copy();
        }

        boolean was_last=removeFromHeardFromList(sender);
        if(was_last) {
            sendStabilityMessage(copy);
        }
    }


    /**
     * Bcasts a STABLE message of the current digest to all members. Message contains highest seqnos of all members
     * seen by this member. Highest seqnos are retrieved from the NAKACK layer below.
     * @param d A <em>copy</em> of this.digest
     */
    private void sendStableMessage(Digest d) {
        if(suspended) {
            if(log.isTraceEnabled())
                log.trace("will not send STABLE message as I'm suspended");
            return;
        }

        if(d != null && d.size() > 0) {
            if(log.isTraceEnabled())
                log.trace("sending stable msg " + d.printHighestDeliveredSeqnos());
            num_stable_msgs_sent++;
            Message msg=new Message(); // mcast message
            msg.setFlag(Message.OOB);
            StableHeader hdr=new StableHeader(StableHeader.STABLE_GOSSIP, d);
            msg.putHeader(name, hdr);
            down_prot.down(new Event(Event.MSG, msg));
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
    private void sendStabilityMessage(Digest tmp) {
        long delay;

        if(suspended) {
            if(log.isTraceEnabled())
                log.trace("STABILITY message will not be sent as I'm suspended");
            return;
        }

        // give other members a chance to mcast STABILITY message. if we receive STABILITY by the end of
        // our random sleep, we will not send the STABILITY msg. this prevents that all mbrs mcast a
        // STABILITY msg at the same time
        delay=Util.random(stability_delay);
        startStabilityTask(tmp, delay);
    }


    private void handleStabilityMessage(Digest d, Address sender) {
        if(d == null) {
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
            log.trace(new StringBuffer("received stability msg from ").append(sender).append(": ").append(d.printHighestDeliveredSeqnos()));
        stopStabilityTask();

        // we won't handle the gossip d, if d's members don't match the membership in my own digest,
        // this is part of the fix for the NAKACK problem (bugs #943480 and #938584)
        if(!this.digest.sameSenders(d)) {
            if(log.isDebugEnabled()) {
                log.debug("received digest (digest=" + d + ") which does not match my own digest ("+
                        this.digest + "): ignoring digest and re-initializing own digest");
            }
            return;
        }

        num_stability_msgs_received++;

        resetDigest(mbrs);

        // pass STABLE event down the stack, so NAKACK can garbage collect old messages
        down_prot.down(new Event(Event.STABLE, d));
    }



    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class StableHeader extends Header implements Streamable {
        public static final int STABLE_GOSSIP=1;
        public static final int STABILITY=2;

        int type=0;
        // Digest digest=new Digest();  // used for both STABLE_GOSSIP and STABILITY message
        Digest stableDigest=null; // changed by Bela April 4 2004

        public StableHeader() {
        } // used for externalizable


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


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            if(stableDigest == null) {
                out.writeBoolean(false);
                return;
            }
            out.writeBoolean(true);
            stableDigest.writeExternal(out);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            boolean digest_not_null=in.readBoolean();
            if(digest_not_null) {
                stableDigest=new Digest();
                stableDigest.readExternal(in);
            }
        }

        public int size() {
            int retval=Global.INT_SIZE + Global.BYTE_SIZE; // type + presence for digest
            if(stableDigest != null)
                retval+=stableDigest.serializedSize();
            return retval;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(type);
            Util.writeStreamable(stableDigest, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readInt();
            stableDigest=(Digest)Util.readStreamable(Digest.class, in);
        }


    }




    /**
     Mcast periodic STABLE message. Interval between sends varies.
     */
    private class StableTask implements TimeScheduler.Task {

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return 10000;
            else
                return interval;
        }


        public void run() {
            if(suspended) {
                if(log.isTraceEnabled())
                    log.trace("stable task will not run as suspended=" + suspended);
                return;
            }

            // asks the NAKACK protocol for the current digest
            Digest my_digest=(Digest)down_prot.down(Event.GET_DIGEST_STABLE_EVT);
            if(my_digest == null) {
                if(log.isWarnEnabled())
                    log.warn("received null digest, skipped sending of stable message");
                return;
            }
            synchronized(latest_local_digest) {
                latest_local_digest.replace(my_digest);
            }
            if(log.isTraceEnabled())
                log.trace("setting latest_local_digest from NAKACK: " + my_digest.printHighestDeliveredSeqnos());
            sendStableMessage(my_digest);
        }

        long computeSleepTime() {
            return getRandom((mbrs.size() * desired_avg_gossip * 2));
        }

        long getRandom(long range) {
            return (long)((Math.random() * range) % range);
        }
    }





    /**
     * Multicasts a STABILITY message.
     */
    private class StabilitySendTask implements Runnable {
        Digest   d=null;

        StabilitySendTask(Digest d) {
            this.d=d;
        }

        public void run() {
            Message msg;
            StableHeader hdr;

            if(suspended) {
                if(log.isDebugEnabled()) {
                    log.debug("STABILITY message will not be sent as suspended=" + suspended);
                }
                return;
            }

            if(d != null) {
                msg=new Message();
                msg.setFlag(Message.OOB);
                hdr=new StableHeader(StableHeader.STABILITY, d);
                msg.putHeader(STABLE.name, hdr);
                if(log.isTraceEnabled()) log.trace("sending stability msg " + d.printHighestDeliveredSeqnos());
                num_stability_msgs_sent++;
                down_prot.down(new Event(Event.MSG, msg));
                d=null;
            }
        }
    }


    private class ResumeTask implements Runnable {
        ResumeTask() {
        }

        public void run() {
            if(suspended)
                log.warn("ResumeTask resumed message garbage collection - this should be done by a RESUME_STABLE event; " +
                         "check why this event was not received (or increase max_suspend_time for large state transfers)");
            resume();
        }
    }


}

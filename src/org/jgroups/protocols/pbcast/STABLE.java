// $Id: STABLE.java,v 1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols.pbcast;


import org.jgroups.*;
import org.jgroups.log.Trace;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Vector;




/**
 * Computes the broadcast messages that are stable, i.e. have been received by all members. Sends
 * STABLE events up the stack when this is the case. This allows NAKACK to garbage collect messages that
 * have been seen by all members.<p>
 * Works as follows: periodically we mcast our highest seqnos (seen for each member) to the group.
 * A stability vector, which maintains the highest seqno for each member and initially contains no data,
 * is updated when such a message is received. The entry for a member P is computed set to
 * min(entry[P], digest[P]). When messages from all members have been received, a stability
 * message is mcast, which causes all members to send a STABLE event up the stack (triggering garbage collection
 * in the NAKACK layer).<p>
 * The stable task now terminates after max_num_gossips if no messages or view changes have been sent or received
 * in the meantime. It will resume when messages are received. This effectively suspends sending superfluous
 * STABLE messages in the face of no activity.<br/>
 * New: when <code>max_bytes</code> is exceeded (unless disabled by setting it to 0),
 * a STABLE task will be started (unless it is already running).
 * @author Bela Ban
 */
public class STABLE extends Protocol {
    Address             local_addr=null;
    Vector              mbrs=new Vector();
    Digest              digest=new Digest();          // keeps track of the highest seqnos from all members
    Promise             digest_promise=new Promise(); // for fetching digest (from NAKACK layer)
    Vector              heard_from=new Vector();      // keeps track of who we already heard from (STABLE_GOSSIP msgs)
    long                digest_timeout=10000;         // time to wait until digest is received (from NAKACK)

    /** Sends a STABLE gossip every 20 seconds on average. 0 disables gossipping of STABLE messages */
    long                desired_avg_gossip=20000;

    /** delay before we send STABILITY msg (give others a change to send first). This should be set to a very
     * small number (> 0 !) if <code>max_bytes</code> is used */
    long                stability_delay=6000;
    StabilitySendTask   stability_task=null;
    Object              stability_mutex=new Object(); // to synchronize on stability_task
    StableTask          stable_task=null;             // bcasts periodic STABLE message (added to timer below)
    TimeScheduler       timer=null;                   // to send periodic STABLE msgs (and STABILITY messages)
    int                 max_gossip_runs=3;            // max. number of times the StableTask runs before terminating
    int                 num_gossip_runs=3;            // this number is decremented (max_gossip_runs doesn't change)
    static final String name="STABLE";

    /** Total amount of bytes from incoming messages (default = 0 = disabled). When exceeded, a STABLE
     * message will be broadcast and <code>num_bytes_received</code> reset to 0 . If this is > 0, then ideally
     * <code>stability_delay</code> should be set to a low number as well */
    long                max_bytes=0;

    /** The total number of bytes received from unicast and multicast messages */
    long                num_bytes_received=0;


    public String getName() {
        return name;
    }


    public Vector requiredDownServices() {
        Vector retval=new Vector();
        retval.addElement(new Integer(Event.GET_DIGEST_STABLE));  // NAKACK layer
        return retval;
    }

    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("digest_timeout");
        if(str != null) {
            digest_timeout=new Long(str).longValue();
            props.remove("digest_timeout");
        }

        str=props.getProperty("desired_avg_gossip");
        if(str != null) {
            desired_avg_gossip=new Long(str).longValue();
            props.remove("desired_avg_gossip");
        }

        str=props.getProperty("stability_delay");
        if(str != null) {
            stability_delay=new Long(str).longValue();
            props.remove("stability_delay");
        }

        str=props.getProperty("max_gossip_runs");
        if(str != null) {
            max_gossip_runs=new Integer(str).intValue();
            num_gossip_runs=max_gossip_runs;
            props.remove("max_gossip_runs");
        }

        str=props.getProperty("max_bytes");
        if(str != null) {
            max_bytes=new Long(str).longValue();
            props.remove("max_bytes");
        }

        if(props.size() > 0) {
            System.err.println("STABLE.setProperties(): these properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }

    public void start() throws Exception {
        if(stack != null && stack.timer != null)
            timer=stack.timer;
        else
            throw new Exception("STABLE.up(): timer cannot be retrieved from protocol stack");
    }

    public void stop() {
        stopStableTask();
    }


    public void up(Event evt) {
        Message msg;
        StableHeader hdr;
        Header obj;
        int type=evt.getType();

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();

                if(max_bytes > 0) {  // message counting is enabled
                    long size=msg.getBuffer() != null? msg.getBuffer().length : 24;
                    num_bytes_received+=size;
                    if(Trace.debug)
                        Trace.info("STABLE.up()", "received message of " + size +
                                " bytes, total bytes received=" + num_bytes_received);
                    if(num_bytes_received >= max_bytes) {
                        if(Trace.trace)
                            Trace.info("STABLE.up()", "max_bytes has been exceeded (max_bytes=" + max_bytes +
                                    ", number of bytes received=" + num_bytes_received + "): sending STABLE message");

                        new Thread() {
                            public void run() {
                                initialize();
                                sendStableMessage();
                            }
                        }.start();
                        num_bytes_received=0;
                    }
                }

                obj=msg.getHeader(getName());
                if(obj == null || !(obj instanceof StableHeader))
                    break;
                hdr=(StableHeader)msg.removeHeader(getName());
                switch(hdr.type) {
                    case StableHeader.STABLE_GOSSIP:
                        handleStableGossip(msg.getSrc(), hdr.digest);
                        break;
                    case StableHeader.STABILITY:
                        handleStabilityMessage(hdr.digest);
                        break;
                    default:
                        Trace.error("STABLE.up()", "StableHeader type " + hdr.type + " not known");
                }
                return;  // don't pass STABLE or STABILITY messages up the stack

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        passUp(evt);
        if(desired_avg_gossip > 0) {
            if(type == Event.VIEW_CHANGE || type == Event.MSG)
                startStableTask(); // only start if not yet running
        }
    }


    /**
     * We need to receive this event out-of-band, otherwise we would block. The use case is
     * <ol>
     * <li>To send a STABLE_GOSSIP message we need the digest (from NAKACK below)
     * <li>We send a GET_DIGEST_STABLE event down <em>from the up() method</em>
     * <li>NAKACK sends the GET_DIGEST_STABLE_OK backup. <em>However, we may have other messages in the
     * up queue ahead of this event !</em> Therefore the event cannot be processed until all messages ahead of
     * the event have been processed. These can't be processed, however, because the up() call waits for
     * GET_DIGEST_STABLE_OK ! The up() call would always run into the timeout.<be/>
     * Having out-of-band reception of just this one event eliminates the problem.
     * </ol>
     * @param evt
     */
    protected void receiveUpEvent(Event evt) {
        if(evt.getType() == Event.GET_DIGEST_STABLE_OK) {
            digest_promise.setResult(evt.getArg());
            return;
        }
        super.receiveUpEvent(evt);
    }


    public void down(Event evt) {
        int type=evt.getType();

        if(desired_avg_gossip > 0) {
            if(type == Event.VIEW_CHANGE || type == Event.MSG)
                startStableTask(); // only start if not yet running
        }

        switch(evt.getType()) {

            case Event.VIEW_CHANGE:
                View v=(View)evt.getArg();
                Vector tmp=v.getMembers();
                mbrs.removeAllElements();
                mbrs.addAll(tmp);
                heard_from.retainAll(tmp);     // removes all elements from heard_from that are not in new view
                break;
        }

        passDown(evt);
    }





    /* --------------------------------------- Private Methods ---------------------------------------- */

    void initialize() {
        synchronized(digest) {
            digest.reset(mbrs.size());
            for(int i=0; i < mbrs.size(); i++)
                digest.add((Address)mbrs.elementAt(i), -1, -1);
            heard_from.removeAllElements();
            heard_from.addAll(mbrs);
        }
    }


    void startStableTask() {
        num_gossip_runs=max_gossip_runs;
        if(stable_task != null && !stable_task.cancelled()) {
            return;  // already running
        }
        stable_task=new StableTask();
        timer.add(stable_task, true); // fixed-rate scheduling
        if(Trace.trace)
            Trace.info("STABLE.startStableTask()", "stable task started; num_gossip_runs=" + num_gossip_runs +
                    ", max_gossip_runs=" + max_gossip_runs);
    }


    void stopStableTask() {
        if(stable_task != null) {
            stable_task.stop();
            stable_task=null;
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
    void handleStableGossip(Address sender, Digest d) {
        Address mbr;
        long highest_seqno, my_highest_seqno;
        long highest_seen_seqno, my_highest_seen_seqno;

        if(d == null || sender == null) {
            if(Trace.trace)
                Trace.error("STABLE.handleStableGossip()", "digest or sender is null");
            return;
        }

        if(Trace.trace)
            Trace.info("STABLE.handleStableGossip()", "received digest " + printStabilityDigest(d) +
                    " from " + sender);

        if(!heard_from.contains(sender)) {  // already received gossip from sender; discard it
            if(Trace.trace)
                Trace.info("STABLE.handleStableGossip()", "already received gossip from " + sender);
            return;
        }

        for(int i=0; i < d.size(); i++) {
            mbr=d.senderAt(i);
            highest_seqno=d.highSeqnoAt(i);
            highest_seen_seqno=d.highSeqnoSeenAt(i);
            if(digest.getIndex(mbr) == -1) {
                if(Trace.trace)
                    Trace.info("STABLE.handleStableGossip()", "sender " + mbr + " not found in stability vector");
                continue;
            }

            // compute the minimum of the highest seqnos deliverable (for garbage collection)
            my_highest_seqno=digest.highSeqnoAt(mbr);
            if(my_highest_seqno < 0) {
                if(highest_seqno >= 0)
                    digest.setHighSeqnoAt(mbr, highest_seqno);
            }
            else {
                digest.setHighSeqnoAt(mbr, Math.min(my_highest_seqno, highest_seqno));
            }

            // compute the maximum of the highest seqnos seen (for retransmission of last missing message)
            my_highest_seen_seqno=digest.highSeqnoSeenAt(mbr);
            if(my_highest_seen_seqno < 0) {
                if(highest_seen_seqno >= 0)
                    digest.setHighSeqnoSeenAt(mbr, highest_seen_seqno);
            }
            else {
                digest.setHighSeqnoSeenAt(mbr, Math.max(my_highest_seen_seqno, highest_seen_seqno));
            }
        }

        heard_from.removeElement(sender);
        if(heard_from.size() == 0) {
            if(Trace.trace)
                Trace.info("STABLE.handleStableGossip()", "sending stability msg " + printStabilityDigest(digest));
            sendStabilityMessage(digest.copy());
            initialize();
        }
    }


    /**
     * Bcasts a STABLE message to all group members. Message contains highest seqnos of all members
     * seen by this member. Highest seqnos are retrieved from the NAKACK layer above.
     */
    synchronized void sendStableMessage() {
        Digest d=null;
        Message msg=new Message(); // mcast message
        StableHeader hdr;

        d=getDigest();
        if(d != null && d.size() > 0) {
            if(Trace.trace)
                Trace.info("STABLE.sendStableMessage()", "mcasting digest " + d +
                        " (num_gossip_runs=" + num_gossip_runs + ", max_gossip_runs=" + max_gossip_runs + ")");
            hdr=new StableHeader(StableHeader.STABLE_GOSSIP, d);
            msg.putHeader(getName(), hdr);
            passDown(new Event(Event.MSG, msg));
        }
    }



    Digest getDigest() {
        Digest ret=null;
        passDown(new Event(Event.GET_DIGEST_STABLE));
        ret=(Digest)digest_promise.getResult(digest_timeout);
        if(ret == null)
            Trace.error("STABLE.getDigest()", "digest could not be fetched from below " +
                    "(timeout was " + digest_timeout + " msecs)");
        return ret;
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
    void sendStabilityMessage(Digest tmp) {
        long delay;

        if(timer == null) {
            if(Trace.trace)
                Trace.error("STABLE.sendStabilityMessage()", "timer is null, cannot schedule " +
                        "stability message to be sent");
            timer=stack != null ? stack.timer : null;
            return;
        }

        // give other members a chance to mcast STABILITY message. if we receive STABILITY by the end of
        // our random sleep, we will not send the STABILITY msg. this prevents that all mbrs mcast a
        // STABILITY msg at the same time
        delay=Util.random(stability_delay);
        if(Trace.trace)
            Trace.info("STABLE.sendStabilityMessage()", "stability_task=" + stability_task +
                    ", delay is " + delay);
        synchronized(stability_mutex) {
            if(stability_task != null && !stability_task.cancelled())  // schedule only if not yet running
                return;
            stability_task=new StabilitySendTask(this, tmp, delay);
            timer.add(stability_task, true); // run it 1x after delay msecs. use fixed-rate scheduling
        }
    }


    void handleStabilityMessage(Digest d) {
        if(d == null) {
            if(Trace.trace)
                Trace.error("STABLE.handleStabilityMessage()", "stability vector is null");
            return;
        }

        if(Trace.trace)
            Trace.info("STABLE.handleStabilityMessage()", "stability vector is " + d.printHighSeqnos());

        synchronized(stability_mutex) {
            if(stability_task != null) {
                if(Trace.trace)
                    Trace.info("STABLE.handleStabilityMessage()", "cancelling stability task (running=" +
                            !stability_task.cancelled() + ")");
                stability_task.stop();
                stability_task=null;
            }
        }

        // pass STABLE event down the stack, so NAKACK can garbage collect old messages
        passDown(new Event(Event.STABLE, d));
    }


    String printStabilityDigest(Digest d) {
        StringBuffer sb=new StringBuffer();
        boolean first=true;

        if(d != null) {
            for(int i=0; i < d.size(); i++) {
                if(!first)
                    sb.append(", ");
                else
                    first=false;
                sb.append(d.senderAt(i) + "#" + d.highSeqnoAt(i) + " (" + d.highSeqnoSeenAt(i) + ")");
            }
        }
        return sb.toString();
    }

    /* ------------------------------------End of Private Methods ------------------------------------- */







    public static class StableHeader extends Header {
        static final int STABLE_GOSSIP=1;
        static final int STABILITY=2;

        int type=0;
        Digest digest=new Digest();  // used for both STABLE_GOSSIP and STABILITY message

        public StableHeader() {
        } // used for externalizable


        StableHeader(int type, Digest digest) {
            this.type=type;
            this.digest=digest;
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
            StringBuffer sb=new StringBuffer();
            sb.append("[");
            sb.append(type2String(type));
            sb.append("]: digest is ");
            sb.append(digest);
            return sb.toString();
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            digest.writeExternal(out);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readInt();
            digest=new Digest();
            digest.readExternal(in);
        }


    }




    /**
     Mcast periodic STABLE message. Interval between sends varies. Terminates after num_gossip_runs is 0.
     However, UP or DOWN messages will reset num_gossip_runs to max_gossip_runs. This has the effect that the
     stable_send task terminates only after a period of time within which no messages were either sent
     or received
     */
    private class StableTask implements TimeScheduler.Task {
        boolean stopped=false;

        public void reset() {
            stopped=false;
        }

        public void stop() {
            stopped=true;
        }

        public boolean cancelled() {
            return stopped;
        }

        public long nextInterval() {
            long interval=computeSleepTime();
            if(interval <= 0)
                return 10000;
            else
                return interval;
        }

        public void run() {
            initialize();
            sendStableMessage();
            num_gossip_runs--;
            if(num_gossip_runs <= 0) {
                if(Trace.trace)
                    Trace.info("STABLE.StableTask.run()", "stable task terminating (num_gossip_runs=" +
                            num_gossip_runs + ", max_gossip_runs=" + max_gossip_runs + ")");
                stop();
            }
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
    private static class StabilitySendTask implements TimeScheduler.Task {
        Digest   d=null;
        Protocol stable_prot=null;
        boolean  stopped=false;
        long     delay=2000;


        public StabilitySendTask(Protocol stable_prot, Digest d, long delay) {
            this.stable_prot=stable_prot;
            this.d=d;
            this.delay=delay;
        }

        public void stop() {
            stopped=true;
        }

        public boolean cancelled() {
            return stopped;
        }


        /** wait a random number of msecs (to give other a chance to send the STABILITY msg first) */
        public long nextInterval() {
            return delay;
        }


        public void run() {
            Message msg;
            StableHeader hdr;

            if(d != null && !stopped) {
                msg=new Message();
                hdr=new StableHeader(StableHeader.STABILITY, d);
                msg.putHeader(STABLE.name, hdr);
                stable_prot.passDown(new Event(Event.MSG, msg));
                d=null;
            }
            stopped=true; // run only once
        }


    }


}

// $Id: STABLE.java,v 1.9 2005/07/17 11:36:15 chrislott Exp $

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.stack.RpcProtocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;


/**
 * Computes the broadcast messages that are stable; i.e., that have been received
 * by all members. Sends STABLE events up the stack when this is the case.
 * Uses a probabilistic scheme to do so, as described in:<br>
 * GSGC: An Efficient Gossip-Style Garbage Collection Scheme for Scalable
 * Reliable Multicast, K. Guo et al., 1997.
 * <p>
 * The only difference is that instead of using counters for an estimation of
 * messages received from each member, we retrieve this actual information
 * from the NAKACK layer (which must be present for the STABLE protocol to
 * work).
 * <p>
 * Note: the the <tt>Event.MSG</tt> call path path must be as lightweight as
 * possible. It should not request any lock for which there is a high
 * contention and/or long delay.
 * <p>
 * <pre>
 * Changes(igeorg - 2.VI.2001):
 * i. Thread-safety (in RPC calls most notably on the lines of Gianluca
 * Collot's bugfix)
 * ii. All slow calls (RPCs, seqnos requests, etc.) placed outside locks
 * iii. Removed redundant initialization in adaptation to a higher round
 * iv. heard_from[this meber] is always set to true on every new round
 * (i.e. on every stability bcast).
 * v. Replaced gossip thread with <tt>TimeScheduler.Task</tt>
 * </pre>
 * <p>
 * [[[ TODO(igeorg - 2.VI.2001)
 * i. Faster stability convergence by better selection of gossip subsets
 * (replace Util.pickSubset()).
 * ii. Special mutex on the <tt>Event.MSG</tt> call path. I.e. remove
 * <tt>synchronized(this)</t>> with e.g. <tt>synchronized(msg_mutex)</tt>.
 * ]] TODO
 */
public class STABLE extends RpcProtocol {
    /** The protocol name */
    private static final String PROT_NAME="STABLE";

    /** Default subgroup size for gossiping expressed as percentage overthe group's size */
    private static final double SUBSET_SIZE=0.1;

    /** Default max number of msgs to wait for before sending gossip */
    private static final int GOSSIP_MSG_INTERVAL=100;

    /** Default max time to wait before sending gossip (ms) */
    private static final int GOSSIP_INTERVAL=10000;

    private Address local_addr=null;
    private ViewId vid=null;
    private final Vector mbrs=new Vector(11);

    /** gossip round */
    private long round=1;

    /** highest seqno received for each member (corresponds to membership) */
    private long[] seqnos=new long[0];

    /** Array of members from which we have received a gossip in the current round */
    private boolean[] heard_from=new boolean[0];

    /** Percentage of members to which gossip is sent (parameterizable by user) */
    private double subset=SUBSET_SIZE;

    /** The gossiping task scheduler */
    private TimeScheduler sched=null;

    private Task gossip_task;

    /** wait for n messages until sending gossip ... */
    private int max_msgs=GOSSIP_MSG_INTERVAL;

    /** ... or until max_wait_time has elapsed, whichever comes first */
    private long max_wait_time=GOSSIP_INTERVAL;

    /** Current number of msgs left to be received before sending a gossip */
    private long num_msgs=max_msgs;

    /** mutex for interacting with NAKACK layer (GET_MSGS_RECVD) */
    private final Object highest_seqnos_mutex=new Object();
    
    /** Time to wait for a reply from NAKACK layer (GET_MSGS_RECVD) */
    private long highest_seqnos_timeout=4000;


    /**
     * @return this protocol name
     */
    public String getName() {
        return (PROT_NAME);
    }


    /**
     * The events expected to be handled from some layer above:
     * <ul>
     * <li>
     * GET_MSGS_RECEIVED: NAKACK layer
     * </li>
     * </ul>
     * @return a list of events expected by to be handled from some layer
     * above
     */
    public Vector requiredUpServices() {
        Vector retval=new Vector(1);
        retval.addElement(new Integer(Event.GET_MSGS_RECEIVED));
        return retval;
    }

    /**
     * Set the parameters for this layer.
     *
     * <ul>
     * <li>
     * <i>subset</i>: the percentage of the group'size to which the
     * msgs_seen_so_far gossip is sent periodically.</li>
     * <li>
     * <i>max_msgs</i>: the max number of msgs to wait for between two
     * consecutive gossipings.</li>
     * <li>
     * <i>max_wait_time</i>: the max time to wait for between two consecutive
     * gossipings.</li>
     * <li>
     * <i>highest_seqno_timeout</i>: time to wait to receive from NAKACK
     * the array of highest deliverable seqnos
     * </li>
     * </ul>
     *
     * @param props the list of parameters
     */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("subset");
        if(str != null) {
            subset=Float.parseFloat(str);
            props.remove("subset");
        }

        str=props.getProperty("max_msgs");
        if(str != null) {
            num_msgs=max_msgs=Integer.parseInt(str);
            if(max_msgs <= 1) {
                if(log.isFatalEnabled()) log.fatal("value for 'max_msgs' must be greater than 1 !");
                return false;
            }
            props.remove("max_msgs");
        }

        str=props.getProperty("max_wait_time");
        if(str != null) {
            max_wait_time=Long.parseLong(str);
            props.remove("max_wait_time");
        }

        str=props.getProperty("highest_seqnos_timeout");
        if(str != null) {
            highest_seqnos_timeout=Long.parseLong(str);
            props.remove("highest_seqnos_timeout");
        }

        if(props.size() > 0) {
            log.error("STABLE.setProperties(): these properties are not recognized: " + props);

            return false;
        }
        return true;
    }


    /**
     * Start the layer:
     * i. Set the gossip task scheduler
     * ii. Reset the layer's state.
     * iii. Start the gossiping task
     */
    public void start() throws Exception {
        TimeScheduler timer;

        super.start();
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("STABLE.start(): timer is null");

        sched=timer;

        // we use only asynchronous method invocations...
        if(_corr != null)
            _corr.setDeadlockDetection(false);
        initialize();
        startGossip();
    }


    /**
     * Stop scheduling the gossip task
     */
    public void stop() {
        super.stop();
        synchronized(this) {
            if(gossip_task != null)
                gossip_task.cancel();
            gossip_task=null;
        }
    }


    /* ------------------------- Request handler methods ------------------ */

    /**
     * Contains the highest sequence numbers as seen by <code>sender</code>
     *
     * @param view_id The view ID in which the gossip was sent. Must be the
     * same as ours, otherwise it is discarded
     *
     * @param gossip_round The round in which the gossip was sent
     *
     * @param gossip_seqnos A vector with the highest sequence numbers as
     * seen by <code>sender</code>
     *
     * @param heard The sender's <code>heard_from</code> array. This allows
     * us to minimize the gossip msgs for a given round as a member does not
     * have to receive gossip msgs from each member, but members pass gossips
     * they've received from others on in their own gossips. E.g. when a
     * member P (of group {P,Q,R}) receives a gossip from R, its own gossip
     * to Q might be {R,P}. Q, who hasn't received a gossip from R, will not
     * need to receive it anymore as it is already sent by P. This simple
     * scheme reduces the number of gossip messages needed.
     *
     * @param sender The sender of the gossip message (obviously :-))
     */
    public void gossip(ViewId view_id, long gossip_round,
                       long[] gossip_seqnos, boolean[] heard, Object sender) {
        Object[] params;
        MethodCall call;

        synchronized(this) {

                if(log.isInfoEnabled()) log.info("sender=" + sender + ", round=" + gossip_round + ", seqnos=" +
                        Util.array2String(gossip_seqnos) + ", heard=" +
                        Util.array2String(heard));
            if(vid == null || view_id == null || !vid.equals(view_id)) {

                    if(log.isInfoEnabled()) log.info("view ID s are different (" + vid + " != " + view_id +
                            "). Discarding gossip received");
                return;
            }
            if(gossip_round < this.round) {

                    if(log.isInfoEnabled()) log.info("received a gossip from a previous round (" +
                            gossip_round + "); my round is " + round +
                            ". Discarding gossip");
                return;
            }
            if(gossip_seqnos == null || seqnos == null ||
                    seqnos.length != gossip_seqnos.length) {

                    if(log.isWarnEnabled()) log.warn("size of seqnos and gossip_seqnos are not equal ! " +
                            "Discarding gossip");
                return;
            }

            // (1) If round greater than local round:
            // i. Adjust the local to the received round
            //
            // (2)
            // i. local_seqnos = arrayMin(local_seqnos, gossip_seqnos)
            // ii. local_heard = arrayMax(local_heard, gossip_heard)
            // iii. If heard from all, bcast our seqnos (stability vector)
            if(round == gossip_round) {
                update(sender, gossip_seqnos, heard);
            }
            else if(round < gossip_round) {

                    if(log.isInfoEnabled()) log.info("received a gossip from a higher round (" +
                            gossip_round + "); adopting my round (" + round +
                            ") to " + gossip_round);
                round=gossip_round;
                set(sender, gossip_seqnos, heard_from);
            }

             if(log.isInfoEnabled()) log.info("heard_from=" + Util.array2String(heard_from));
            if(!heardFromAll())
                return;

            params=new Object[]{
                vid.clone(),
                new Long(gossip_round),
                seqnos.clone(),
                local_addr};
        } // synchronized(this)

        call=new MethodCall("stability", params, 
            new String[] {ViewId.class.getName(), long.class.getName(), long[].class.getName(), Object.class.getName()});
        callRemoteMethods(null, call, GroupRequest.GET_NONE, 0);
    }


    /**
     * Contains the highest message sequence numbers (for each member) that
     * can safely be deleted (because they have been seen by all members).
     */
    public void stability(ViewId view_id, long gossip_round, long[] stability_vector, Object sender) {
        // i. Proceed to the next round; init the heard from list
        // ii. Send up the stability vector
        // iii. get a fresh copy of the highest deliverable seqnos
        synchronized(this) {

                if(log.isInfoEnabled()) log.info("sender=" + sender + ", round=" + gossip_round + ", vector=" +
                        Util.array2String(stability_vector) + ')');
            if(vid == null || view_id == null || !vid.equals(view_id)) {

                    if(log.isInfoEnabled()) log.info("view ID s are different (" + vid + " != " + view_id +
                            "). Discarding gossip received");
                return;
            }

            if(round > gossip_round)
                return;
            round=gossip_round + 1;
            for(int i=0; i < heard_from.length; i++)
                heard_from[i]=false;
        }
        heard_from[mbrs.indexOf(local_addr)]=true;

        passUp(new Event(Event.STABLE, stability_vector));
        getHighestSeqnos();
    }

    /* --------------------- End of Request handler methods --------------- */

    /**
     * <b>Callback</b>. Called by superclass when event may be handled.
     * <p>
     * <b>Do not use <code>PassUp</code> in this method as the event is passed
     * up by default by the superclass after this method returns !</b>
     *
     * @return boolean Defaults to true. If false, event will not be passed
     * up the stack.
     */
    public boolean handleUpEvent(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                if(!upMsg(evt))
                    return (false);
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return true;
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.
     * <p>
     * <b>Do not use <code>PassDown</code> in this method as the event is
     * passed down by default by the superclass after this method returns !</b>
     *
     * @return boolean Defaults to true. If false, event will not be passed
     * down the stack.
     */
    public boolean handleDownEvent(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                if(!downViewChange(evt))
                    return (false);
                break;
                // does anyone else below needs this msg except STABLE?
            case Event.GET_MSGS_RECEIVED_OK:
                if(!downGetMsgsReceived(evt))
                    return (false);
                break;
        }

        return (true);
    }


    /**
     * The gossip task that runs periodically
     */
    private void gossipRun() {
        num_msgs=max_msgs;
        sendGossip();
    }


    /**
     * <pre>
     * Reset the state of msg garbage-collection:
     * i. Reset the table of highest seqnos seen by each member
     * ii. Reset the tbl of mbrs from which highest seqnos have been recorded
     * </pre>
     */
    private void initialize() {
        synchronized(this) {
            seqnos=new long[mbrs.size()];
            for(int i=0; i < seqnos.length; i++)
                seqnos[i]=-1;

            heard_from=new boolean[mbrs.size()];
            for(int i=0; i < heard_from.length; i++)
                heard_from[i]=false;
        }
    }


    /**
     * (1)<br>
     * Merge this member's table of highest seqnos seen by a each member
     * with the one received from a gossip by another member. The result is
     * the element-wise minimum of the input arrays. For each entry:<br>
     *
     * <tt>seqno[mbr_i] = min(seqno[mbr_i], gossip_seqno[mbr_i])</tt>
     * <p>
     *
     * (2)<br>
     * Merge the <tt>heard from</tt> tables of this member and the sender of
     * the gossip. The resulting table is:<br>
     *
     * <tt>heard_from[mbr_i] = heard_from[mbr_i] | sender_heard[mbr_i]</tt>
     *
     * @param sender the sender of the gossip
     * @param gossip_seqnos the highest deliverable seqnos of the sender
     * @param gossip_heard_from the table of members sender has heard from
     *
     */
    private void update(Object sender, long[] gossip_seqnos,
                        boolean[] gossip_heard_from) {
        int index;

        synchronized(this) {
            index=mbrs.indexOf(sender);
            if(index < 0) {
                 if(log.isWarnEnabled()) log.warn("sender " + sender + " not found in mbrs !");
                return;
            }

            for(int i=0; i < gossip_seqnos.length; i++)
                seqnos[i]=Math.min(seqnos[i], gossip_seqnos[i]);

            heard_from[index]=true;
            for(int i=0; i < heard_from.length; i++)
                heard_from[i]=heard_from[i] | gossip_heard_from[i];
        }
    }


    /**
     * Set the seqnos and heard_from arrays to those of the sender. The
     * method is called when the sender seems to know more than this member.
     * The situation occurs if either:
     * <ul>
     * <li>
     * sender.heard_from > this.heard_from, i.e. the sender has heard
     * from more members than we have</li>
     * <li>
     * sender.round > this.round, i.e. the sender is in a more recent round
     * than we are</li>
     * </ul>
     *
     * In both cases, this member is assigned the state of the sender
     */
    private void set(Object sender, long[] gossip_seqnos,
                     boolean[] gossip_heard_from) {
        int index;

        synchronized(this) {
            index=mbrs.indexOf(sender);
            if(index < 0) {
                 if(log.isWarnEnabled()) log.warn("sender " + sender + " not found in mbrs !");
                return;
            }

            seqnos=gossip_seqnos;
            heard_from=gossip_heard_from;
        }
    }


    /**
     * @return true, if we have received the highest deliverable seqnos
     * directly or indirectly from all members
     */
    private boolean heardFromAll() {
        synchronized(this) {
            if(heard_from == null) return false;
            for(int i=0; i < heard_from.length; i++)
                if(!heard_from[i])
                    return false;
        }

        return true;
    }


    /**
     * Send our <code>seqnos</code> array to a subset of the membership
     */
    private void sendGossip() {
        Vector gossip_subset;
        Object[] params;
        MethodCall call;

        synchronized(this) {
            gossip_subset=Util.pickSubset(mbrs, subset);
            if(gossip_subset == null || gossip_subset.size() < 1) {
                 if(log.isWarnEnabled()) log.warn("picked empty subset !");
                return;
            }


                if(log.isInfoEnabled()) log.info("subset=" + gossip_subset + ", round=" + round + ", seqnos=" +
                        Util.array2String(seqnos));

            params=new Object[]{
                vid.clone(),
                new Long(round),
                seqnos.clone(),
                heard_from.clone(),
                local_addr};
        }

        call=new MethodCall("gossip", params, 
            new String[] {ViewId.class.getName(), long.class.getName(), long[].class.getName(), boolean[].class.getName(), Object.class.getName()});
        for(int i=0; i < gossip_subset.size(); i++) {
            try {
                callRemoteMethod((Address)gossip_subset.get(i), call, GroupRequest.GET_NONE, 0);
            }
            catch(Exception e) {
                 if(log.isDebugEnabled()) log.debug("exception=" + e);
            }
        }
    }


    /**
     * Sends GET_MSGS_RECEIVED to NAKACK layer (above us !) and stores result
     * in <code>seqnos</code>. In case <code>seqnos</code> does not yet exist
     * it creates and initializes it.
     */
    private void getHighestSeqnos() {
        synchronized(highest_seqnos_mutex) {
            passUp(new Event(Event.GET_MSGS_RECEIVED));

            try {
                highest_seqnos_mutex.wait(highest_seqnos_timeout);
            }
            catch(InterruptedException e) {

                    if(log.isErrorEnabled()) log.error("Interrupted while waiting for highest seqnos from NAKACK");
            }
        }
    }


    /**
     * Start scheduling the gossip task
     */
    private void startGossip() {
        synchronized(this) {
            if(gossip_task != null)
                gossip_task.cancel();
            gossip_task=new Task(new Times(new long[]{GOSSIP_INTERVAL}));
            sched.add(gossip_task);
        }
    }


    /**
     * Received a <tt>MSG</tt> event from a layer below
     *
     * A msg received:
     * If unicast ignore; if multicast and time for gossiping has been
     * reached, send out a gossip to a subset of the mbrs
     *
     * @return true if the event should be forwarded to the layer above
     */
    private boolean upMsg(Event e) {
        Message msg=(Message)e.getArg();

        if(msg.getDest() != null && (!msg.getDest().isMulticastAddress()))
            return (true);

        synchronized(this) {
            --num_msgs;
            if(num_msgs > 0)
                return (true);
            num_msgs=max_msgs;

            gossip_task.cancel();
            gossip_task=new Task(new Times(new long[]{0, GOSSIP_INTERVAL}));
            sched.add(gossip_task);
        }

        return (true);
    }


    /**
     * Received a <tt>VIEW_CHANGE</tt> event from a layer above
     *
     * A new view:
     * i. Set the new mbrs list and the new view ID.
     * ii. Reset the highest deliverable seqnos seen
     *
     * @return true if the event should be forwarded to the layer below
     */
    private boolean downViewChange(Event e) {
        View v=(View)e.getArg();
        Vector new_mbrs=v.getMembers();

        /*
          // Could this ever happen? GMS is always sending non-null value
          if(new_mbrs == null) {
          / Trace.println(
          "STABLE.handleDownEvent()", Trace.ERROR,
          "Received VIEW_CHANGE event with null mbrs list");
          break;
          }
        */

        synchronized(this) {
            vid=v.getVid();
            mbrs.clear();
            mbrs.addAll(new_mbrs);
            initialize();
        }

        return (true);
    }


    /**
     * Received a <tt>GET_MSGS__RECEIVED_OK</tt> event from a layer above
     *
     * Updated list of highest deliverable seqnos:
     * i. Update the local copy of highest deliverable seqnos
     *
     * @return true if the event should be forwarded to the layer below
     */
    private boolean downGetMsgsReceived(Event e) {
        long[] new_seqnos=(long[])e.getArg();

        try {
            synchronized(this) {
                if(new_seqnos == null)
                    return (true);
                if(new_seqnos.length != seqnos.length) {

                        if(log.isInfoEnabled()) log.info("GET_MSGS_RECEIVED: array of highest " +
                                "seqnos seen so far (received from NAKACK layer) " +
                                "has a different length (" + new_seqnos.length +
                                ") from 'seqnos' array (" + seqnos.length + ')');
                    return (true);
                }
                System.arraycopy(new_seqnos, 0, seqnos, 0, seqnos.length);
            }

        }
        finally {
            synchronized(highest_seqnos_mutex) {
                highest_seqnos_mutex.notifyAll();
            }
        }

        return (true);
    }


    /**
     * Select next interval from list. Once the end of the list is reached,
     * keep returning the last value. It would be sensible that list of
     * times is in increasing order
     */
    private static class Times {
        private int next=0;
        private long[] times;

        public Times(long[] times) {
            if(times.length == 0)
                throw new IllegalArgumentException("times");
            this.times=times;
        }

        public synchronized long next() {
            if(next >= times.length)
                return (times[times.length - 1]);
            else
                return (times[next++]);
        }

        public long[] times() {
            return (times);
        }

        public synchronized void reset() {
            next=0;
        }
    }


    /**
     * The gossiping task
     */
    private class Task implements TimeScheduler.Task {
        private final Times intervals;
        private boolean cancelled=false;

        public Task(Times intervals) {
            this.intervals=intervals;
        }

        public long nextInterval() {
            return (intervals.next());
        }

        public boolean cancelled() {
            return (cancelled);
        }

        public void cancel() {
            cancelled=true;
        }

        public void run() {
            gossipRun();
        }
    }
}

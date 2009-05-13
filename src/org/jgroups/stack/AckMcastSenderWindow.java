package org.jgroups.stack;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.TimeScheduler;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Future;


/**
 * Keeps track of ACKs from receivers for each message. When a new message is
 * sent, it is tagged with a sequence number and the receiver set (set of
 * members to which the message is sent) and added to a hashtable
 * (key = sequence number, val = message + receiver set). Each incoming ACK
 * is noted and when all ACKs for a specific sequence number haven been
 * received, the corresponding entry is removed from the hashtable. A
 * retransmission thread periodically re-sends the message point-to-point to
 * all receivers from which no ACKs have been received yet. A view change or
 * suspect message causes the corresponding non-existing receivers to be
 * removed from the hashtable.
 * <p/>
 * This class may need flow control in order to avoid needless
 * retransmissions because of timeouts.
 * @author Bela Ban June 9 1999, 2007
 * @author John Georgiadis May 8 2001
 * @version $Id: AckMcastSenderWindow.java,v 1.16 2009/05/13 13:06:56 belaban Exp $
 */
public class AckMcastSenderWindow {
    /**
     * Called by retransmitter thread whenever a message needs to be re-sent
     * to a destination. <code>dest</code> has to be set in the
     * <code>dst</code> field of <code>msg</code>, as the latter was sent
     * multicast, but now we are sending a unicast message. Message has to be
     * copied before sending it (as headers will be appended and therefore
     * the message changed!).
     */
    public interface RetransmitCommand {
        /**
         * Retranmit the given msg
         * @param seqno the sequence number associated with the message
         * @param msg   the msg to retransmit (it should be a copy!)
         * @param dest  the msg destination
         */
        void retransmit(long seqno, Message msg, Address dest);
    }





    private static final long SEC=1000;
    /**
     * Default retransmit intervals (ms) - exponential approx.
     */
    private static final Interval RETRANSMIT_TIMEOUTS=new StaticInterval(2 * SEC, 3 * SEC, 5 * SEC, 8 * SEC);
    /**
     * Default retransmit thread suspend timeout (ms)
     */

    protected static final Log log=LogFactory.getLog(AckMcastSenderWindow.class);


    // Msg tables related
    /**
     * Table of pending msgs: seqno -> Entry
     */
    private final Map<Long, Entry> msgs=new HashMap<Long,Entry>();

    /**
     * List of recently suspected members. Used to cease retransmission to suspected members
     */
    private final LinkedList suspects=new LinkedList();

    /**
     * Max number in suspects list
     */
    private static final int max_suspects=20;

    /**
     * List of acknowledged msgs since the last call to
     * <code>getStableMessages()</code>
     */
    private final List stable_msgs=new LinkedList();
    /**
     * Whether a call to <code>waitUntilAcksReceived()</code> is still active
     */
    private boolean waiting=false;

    // Retransmission thread related
    /**
     * Whether retransmitter is externally provided or owned by this object
     */
    private boolean retransmitter_owned;
    /**
     * The retransmission scheduler
     */
    private TimeScheduler timer=null;
    /**
     * Retransmission intervals
     */
    private Interval retransmit_intervals;
    /**
     * The callback object for retransmission
     */
    private RetransmitCommand cmd=null;


    /**
     * Convert exception stack trace to string
     */
    private static String _toString(Throwable ex) {
        StringWriter sw=new StringWriter();
        PrintWriter pw=new PrintWriter(sw);
        ex.printStackTrace(pw);
        return (sw.toString());
    }


    /**
     * @param entry the record associated with the msg to retransmit. It
     *              contains the list of receivers that haven't yet ack reception
     */
    private void _retransmit(Entry entry) {
        Address sender;
        boolean received;

        synchronized(entry) {
            for(Enumeration e=entry.senders.keys(); e.hasMoreElements();) {
                sender=(Address)e.nextElement();
                received=((Boolean)entry.senders.get(sender)).booleanValue();
                if(!received) {
                    if(suspects.contains(sender)) {

                        if(log.isWarnEnabled()) log.warn("removing " + sender +
                                " from retransmit list as it is in the suspect list");
                        remove(sender);
                        continue;
                    }

                    if(log.isInfoEnabled()) log.info("--> retransmitting msg #" +
                            entry.seqno + " to " + sender);
                    cmd.retransmit(entry.seqno, entry.msg.copy(), sender);
                }
            }
        }
    }


    /**
     * Setup this object's state
     * @param cmd                  the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     *                             retransmission attempts
     * @param timer                the external scheduler to use to schedule retransmissions
     * @param sched_owned          if true, the scheduler is owned by this object and
     *                             can be started/stopped/destroyed. If false, the scheduler is shared
     *                             among multiple objects and start()/stop() should not be called from
     *                             within this object
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    private void init(RetransmitCommand cmd, Interval retransmit_intervals, TimeScheduler timer, boolean sched_owned) {
        if(cmd == null) {
            if(log.isErrorEnabled()) log.error("command is null. Cannot retransmit " + "messages !");
            throw new IllegalArgumentException("cmd");
        }

        retransmitter_owned=sched_owned;
        this.timer=timer;
        this.retransmit_intervals=retransmit_intervals;
        this.cmd=cmd;
    }


    /**
     * Create and <b>start</b> the retransmitter
     * @param cmd                  the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     *                             retransmission attempts
     * @param sched                the external scheduler to use to schedule retransmissions
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd,
                                Interval retransmit_intervals, TimeScheduler sched) {
        init(cmd, retransmit_intervals, sched, false);
    }


    /**
     * Create and <b>start</b> the retransmitter
     * @param cmd   the callback object for retranmissions
     * @param sched the external scheduler to use to schedule retransmissions
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd, TimeScheduler sched) {
        init(cmd, RETRANSMIT_TIMEOUTS, sched, false);
    }


    /**
     * Create and <b>start</b> the retransmitter
     * @param cmd                  the callback object for retranmissions
     * @param retransmit_intervals the interval between two consecutive
     *                             retransmission attempts
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd, Interval retransmit_intervals) {
        init(cmd, retransmit_intervals, new TimeScheduler(), true);
    }

    /**
     * Create and <b>start</b> the retransmitter
     * @param cmd the callback object for retranmissions
     * @throws IllegalArgumentException if <code>cmd</code> is null
     */
    public AckMcastSenderWindow(RetransmitCommand cmd) {
        this(cmd, RETRANSMIT_TIMEOUTS);
    }


    /**
     * Adds a new message to the hash table.
     * @param seqno     The sequence number associated with the message
     * @param msg       The message (should be a copy!)
     * @param receivers The set of addresses to which the message was sent
     *                  and from which consequently an ACK is expected
     */
    public void add(long seqno, Message msg, Vector receivers) {
        if(waiting) return;
        if(receivers.isEmpty()) return;

        synchronized(msgs) {
            if(msgs.get(new Long(seqno)) != null) return;
            // each entry needs its own retransmission interval, intervals are stateful *and* mutable, so we *need* to copy !
            Entry e=new Entry(seqno, msg, receivers, retransmit_intervals.copy());
            Future future=timer.scheduleWithDynamicInterval(e);
            e.setFuture(future);
            msgs.put(new Long(seqno), e);
        }
    }


    /**
     * An ACK has been received from <code>sender</code>. Tag the sender in
     * the hash table as 'received'. If all ACKs have been received, remove
     * the entry all together.
     * @param seqno  The sequence number of the message for which an ACK has
     *               been received.
     * @param sender The sender which sent the ACK
     */
    public void ack(long seqno, Address sender) {
        Entry entry;
        Boolean received;

        synchronized(msgs) {
            entry=msgs.get(new Long(seqno));
            if(entry == null) return;

            synchronized(entry) {
                received=(Boolean)entry.senders.get(sender);
                if(received == null || received.booleanValue()) return;

                // If not yet received
                entry.senders.put(sender, Boolean.TRUE);
                entry.num_received++;
                if(!entry.allReceived()) return;
            }

            synchronized(stable_msgs) {
                entry.cancel();
                msgs.remove(new Long(seqno));
                stable_msgs.add(new Long(seqno));
            }
            // wake up waitUntilAllAcksReceived() method
            msgs.notifyAll();
        }
    }


    /**
     * Remove <code>obj</code> from all receiver sets and wake up
     * retransmission thread.
     * @param obj the sender to remove
     */
    public void remove(Address obj) {
        Long key;
        Entry entry;

        synchronized(msgs) {
            for(Iterator<Long> it=msgs.keySet().iterator(); it.hasNext();) {
                key=it.next();
                entry=msgs.get(key);
                synchronized(entry) {
                    //if (((Boolean)entry.senders.remove(obj)).booleanValue()) entry.num_received--;
                    //if (!entry.allReceived()) continue;
                    Boolean received=(Boolean)entry.senders.remove(obj);
                    if(received == null) continue; // suspected member not in entry.senders ?
                    if(received.booleanValue()) entry.num_received--;
                    if(!entry.allReceived()) continue;
                }
                synchronized(stable_msgs) {
                    entry.cancel();
                    msgs.remove(key);
                    stable_msgs.add(key);
                }
                // wake up waitUntilAllAcksReceived() method
                msgs.notifyAll();
            }
        }
    }


    /**
     * Process with address <code>suspected</code> is suspected: remove it
     * from all receiver sets. This means that no ACKs are expected from this
     * process anymore.
     * @param suspected The suspected process
     */
    public void suspect(Address suspected) {

        if(log.isInfoEnabled()) log.info("suspect is " + suspected);
        remove(suspected);
        suspects.add(suspected);
        if(suspects.size() >= max_suspects)
            suspects.removeFirst();
    }


    /**
     * @return a copy of stable messages, or null (if non available). Removes
     *         all stable messages afterwards
     */
    public List getStableMessages() {
        List retval;

        synchronized(stable_msgs) {
            retval=(!stable_msgs.isEmpty())? new LinkedList(stable_msgs) : null;
            if(!stable_msgs.isEmpty()) stable_msgs.clear();
        }

        return retval;
    }


    public void clearStableMessages() {
        synchronized(stable_msgs) {
            stable_msgs.clear();
        }
    }


    /**
     * @return the number of currently pending msgs
     */
    public long size() {
        synchronized(msgs) {
            return (msgs.size());
        }
    }


    /**
     * Returns the number of members for a given entry for which acks have to be received
     */
    public long getNumberOfResponsesExpected(long seqno) {
        Entry entry=msgs.get(new Long(seqno));
        if(entry != null)
            return entry.senders.size();
        else
            return -1;
    }

    /**
     * Returns the number of members for a given entry for which acks have been received
     */
    public long getNumberOfResponsesReceived(long seqno) {
        Entry entry=msgs.get(new Long(seqno));
        if(entry != null)
            return entry.num_received;
        else
            return -1;
    }

    /**
     * Prints all members plus whether an ack has been received from those members for a given seqno
     */
    public String printDetails(long seqno) {
        Entry entry=msgs.get(new Long(seqno));
        if(entry != null)
            return entry.toString();
        else
            return null;
    }


    /**
     * Waits until all outstanding messages have been ACKed by all receivers.
     * Takes into account suspicions and view changes. Returns when there are
     * no entries left in the hashtable. <b>While waiting, no entries can be
     * added to the hashtable (they will be discarded).</b>
     * @param timeout Miliseconds to wait. 0 means wait indefinitely.
     */
    public void waitUntilAllAcksReceived(long timeout) {
        long time_to_wait, start_time, current_time;
        Address suspect;

        // remove all suspected members from retransmission
        for(Iterator it=suspects.iterator(); it.hasNext();) {
            suspect=(Address)it.next();
            remove(suspect);
        }

        time_to_wait=timeout;
        waiting=true;
        if(timeout <= 0) {
            synchronized(msgs) {
                while(!msgs.isEmpty()) {
                    try {
                        msgs.wait();
                    }
                    catch(InterruptedException ex) {
                    }
                }
            }
        }
        else {
            start_time=System.currentTimeMillis();
            synchronized(msgs) {
                while(!msgs.isEmpty()) {
                    current_time=System.currentTimeMillis();
                    time_to_wait=timeout - (current_time - start_time);
                    if(time_to_wait <= 0) break;

                    try {
                        msgs.wait(time_to_wait);
                    }
                    catch(InterruptedException ex) {
                        if(log.isWarnEnabled()) log.warn(ex.toString());
                    }
                }
            }
        }
        waiting=false;
    }



    /**
     * Stop the rentransmition and clear all pending msgs.
     * <p/>
     * If this retransmitter has been provided an externally managed
     * scheduler, then just clear all msgs and the associated tasks, else
     * stop the scheduler. In this case the method blocks until the
     * scheduler's thread is dead. Only the owner of the scheduler should
     * stop it.
     */
    public void stop() {

        // i. If retransmitter is owned, stop it else cancel all tasks
        // ii. Clear all pending msgs and notify anyone waiting
        synchronized(msgs) {
            if(retransmitter_owned) {
                try {
                    timer.stop();
                }
                catch(InterruptedException ex) {
                    if(log.isErrorEnabled()) log.error(_toString(ex));
                }
            }
            else {
                for(Entry entry: msgs.values()) {
                    entry.cancel();
                }
            }
            msgs.clear();
            msgs.notifyAll(); // wake up waitUntilAllAcksReceived() method
        }
    }


    /**
     * Remove all pending msgs from the hashtable. Cancel all associated
     * tasks in the retransmission scheduler
     */
    public void reset() {
        if(waiting) return;
        synchronized(msgs) {
            for(Entry entry: msgs.values()) {
                entry.cancel();
            }
            msgs.clear();
            msgs.notifyAll();
        }
    }


    public String toString() {
        StringBuilder ret;
        Entry entry;

        ret=new StringBuilder();
        synchronized(msgs) {
            ret.append("msgs: (").append(msgs.size()).append(')');
            for(Long key: msgs.keySet()) {
                entry=msgs.get(key);
                ret.append("key = ").append(key).append(", value = ").append(entry).append('\n');
            }
            synchronized(stable_msgs) {
                ret.append("\nstable_msgs: ").append(stable_msgs);
            }
        }
		
        return(ret.toString());
    }



    /**
     * The retransmit task executed by the scheduler in regular intervals
     */
    private static abstract class Task implements TimeScheduler.Task {
        private final Interval intervals;
        private Future future;

        protected Task(Interval intervals) {
            this.intervals=intervals;
        }


        public void setFuture(Future future) {
            this.future=future;
        }

        public long nextInterval() {
            return intervals.next();
        }

        public void cancel() {
            if(future != null) {
                future.cancel(false);
                future=null;
            }
        }

    }


    /**
     * The entry associated with a pending msg
     */
    private class Entry extends Task {
        /**
         * The msg sequence number
         */
        public final long seqno;
        /**
         * The msg to retransmit
         */
        public Message msg=null;
        /**
         * destination addr -> boolean (true = received, false = not)
         */
        public final Hashtable senders=new Hashtable();
        /**
         * How many destinations have received the msg
         */
        public int num_received=0;

        public Entry(long seqno, Message msg, Vector dests, Interval intervals) {
            super(intervals);
            this.seqno=seqno;
            this.msg=msg;
            for(int i=0; i < dests.size(); i++)
                senders.put(dests.elementAt(i), Boolean.FALSE);
        }

        boolean allReceived() {
            return (num_received >= senders.size());
        }

        /**
         * Retransmit this entry
         */
        public void run() {
            _retransmit(this);
        }

        public String toString() {
            StringBuilder buf=new StringBuilder();
            buf.append("num_received = ").append(num_received).append(", received msgs = ").append(senders);
            return buf.toString();
        }
    }


}

// $Id: NakReceiverWindow.java,v 1.3 2004/04/19 18:41:07 belaban Exp $


package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.List;
import org.jgroups.util.RWLock;
import org.jgroups.util.TimeScheduler;

import java.util.Enumeration;


/**
 * Keeps track of messages according to their sequence numbers. Allows
 * messages to be added out of order, and with gaps between sequence numbers.
 * Method <code>remove()</code> removes the first message with a sequence
 * number that is 1 higher than <code>next_to_remove</code> (this variable is
 * then incremented), or it returns null if no message is present, or if no
 * message's sequence number is 1 higher.
 * <p>
 * When there is a gap upon adding a message, its seqno will be added to the
 * Retransmitter, which (using a timer) requests retransmissions of missing
 * messages and keeps on trying until the message has been received, or the
 * member who sent the message is suspected.
 * <p>
 * Started out as a copy of SlidingWindow. Main diff: RetransmitCommand is
 * different, and retransmission thread is only created upon detection of a
 * gap.
 * <p>
 * Change Nov 24 2000 (bela): for PBCAST, which has its own retransmission
 * (via gossip), the retransmitter thread can be turned off
 * <p>
 * Change April 25 2001 (igeorg):<br>
 * i. Restructuring: placed all nested class definitions at the top, then
 * class static/non-static variables, then class private/public methods.<br>
 * ii. Class and all nested classes are thread safe. Readers/writer lock
 * added on <tt>NakReceiverWindow</tt> for finer grained locking.<br>
 * iii. Internal or externally provided retransmission scheduler thread.<br>
 * iv. Exponential backoff in time for retransmissions.<br>
 *
 * @author Bela Ban May 27 1999
 * @author John Georgiadis May 8 2001
 */
public class NakReceiverWindow {


    /** Maintains association between seqno and message */
    private static class Entry {
        private long seqno=0;
        private Message msg=null;

        public Entry() {
            seqno=0;
            msg=null;
        }

        public Entry(long seqno, Message msg) {
            this.seqno=seqno;
            this.msg=msg;
        }

        public Entry copy() {
            Entry retval=new Entry();
            retval.seqno=seqno;
            if(msg != null) retval.msg=msg.copy();
            return retval;
        }


        public String toString() {
            StringBuffer ret=new StringBuffer();
            ret.append(seqno);
            if(msg == null)
                ret.append("-");
            else
                ret.append("+");
            return ret.toString();
        }
    }

//    HashMap xmits=new HashMap(); // Long (seqno)/ XmitEntry
//
//    class XmitEntry {
//        long created=System.currentTimeMillis();
//        long received;
//    }


    /** The big read/write lock */
    private RWLock lock=new RWLock();

    /** keep track of *next* seqno to remove and highest received */
    private long   head=0;
    private long   tail=0;

    /** lowest seqno delivered so far */
    private long   lowest_seen=0;

    /** highest deliverable (or delivered) seqno so far */
    private long   highest_seen=0;

    /** List<Entry>. Received (but not yet delivered) msgs. */
    private List   msgs=new List();

    /** List<Entry>. Delivered (= seen by all members) messages. A remove() method causes a message to
     * be moved from msgs to delivered_msgs. Message garbage colection will gradually remove elements in this list  */
    private List   delivered_msgs=new List();

    /** if not set, no retransmitter thread will be started. Useful if
     * protocols do their own retransmission (e.g PBCAST) */
    private Retransmitter retransmitter=null;

    protected static Log log=LogFactory.getLog(NakReceiverWindow.class);


    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be
     * started
     * @param start_seqno The first sequence number to be received
     * @param sched the external scheduler to use for retransmission
     * requests of missing msgs. If it's not provided or is null, an internal
     * one is created
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd,
                             long start_seqno, TimeScheduler sched) {
        head=start_seqno;
        tail=head;

        if(cmd != null)
            retransmitter=sched == null ?
                    new Retransmitter(sender, cmd) :
                    new Retransmitter(sender, cmd, sched);
    }

    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be
     * started
     * @param start_seqno The first sequence number to be received
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long start_seqno) {
        this(sender, cmd, start_seqno, null);
    }

    /**
     * Creates a new instance without a retransmission thread
     *
     * @param sender The sender associated with this instance
     * @param start_seqno The first sequence number to be received
     */
    public NakReceiverWindow(Address sender, long start_seqno) {
        this(sender, null, start_seqno);
    }


    public void setRetransmitTimeouts(long[] timeouts) {
        if(retransmitter != null)
            retransmitter.setRetransmitTimeouts(timeouts);
    }


    /**
     * Adds a message according to its sequence number (ordered).
     * <p>
     * Variables <code>head</code> and <code>tail</code> mark the start and
     * end of the messages received, but not delivered yet. When a message is
     * received, if its seqno is smaller than <code>head</code>, it is
     * discarded (already received). If it is bigger than <code>tail</code>,
     * we advance <code>tail</code> and add empty elements. If it is between
     * <code>head</code> and <code>tail</code>, we set the corresponding
     * missing (or already present) element. If it is equal to
     * <code>tail</code>, we advance the latter by 1 and add the message
     * (default case).
     */
    public void add(long seqno, Message msg) {
        Entry current=null;
        long old_tail;

        lock.writeLock();
        try {
            old_tail=tail;
            if(seqno < head) {
                if(log.isTraceEnabled())
                    log.trace("seqno " + seqno + " is smaller than " + head + "); discarding message");
                return;
            }

            // add at end (regular expected msg)
            if(seqno == tail) {
                msgs.add(new Entry(seqno, msg));
                tail++;
            }
            // gap detected
            // i. add placeholders, creating gaps
            // ii. add real msg
            // iii. tell retransmitter to retrieve missing msgs
            else if(seqno > tail) {
                for(long i=tail; i < seqno; i++) {
                    msgs.add(new Entry(i, null));
                    // XmitEntry xmit_entry=new XmitEntry();
                    //xmits.put(new Long(i), xmit_entry);
                    tail++;
                }
                msgs.add(new Entry(seqno, msg));
                tail=seqno + 1;
                if(retransmitter != null) {
                    retransmitter.add(old_tail, seqno - 1);
                }
                // finally received missing message
            }
            else if(seqno < tail) {
                if(log.isTraceEnabled())
                    log.trace("added missing msg " + msg.getSrc() + "#" + seqno);
                for(Enumeration en=msgs.elements(); en.hasMoreElements();) {
                    current=(Entry)en.nextElement();
                    // overwrite any previous message (e.g. added by down()) and
                    // remove seqno from retransmitter
                    if(seqno == current.seqno) {

                        // only set message if not yet received (bela July 23 2003)
                        if(current.msg == null) {
                            current.msg=msg;

                            //XmitEntry xmit_entry=(XmitEntry)xmits.get(new Long(seqno));
                            //if(xmit_entry != null)
                              //  xmit_entry.received=System.currentTimeMillis();
                            //long xmit_diff=xmit_entry == null? -1 : xmit_entry.received - xmit_entry.created;
                            //NAKACK.addXmitResponse(msg.getSrc(), seqno);
                            if(retransmitter != null) retransmitter.remove(seqno);
                        }
                        break;
                    }
                }
            }
            _updateLowestSeen();
            _updateHighestSeen();
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * Find the entry with seqno <code>head</code>. If e.msg is != null ->
     * return it and increment <code>head</code>, else return null.
     * <p>
     * This method essentially shrinks the size of the sliding window by one
     */
    public Message remove() {
        Entry e;
        Message retval=null;

        lock.writeLock();
        try {
            e=(Entry)msgs.peekAtHead();
            if((e != null) && (e.msg != null)) {
                retval=e.msg;
                msgs.removeFromHead();
                // delivered_msgs.add(e.copy());
                // changed by bela July 23 2003: no need for a copy
                delivered_msgs.add(new Entry(e.seqno, e.msg));
                head++;
            }
            return retval;
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * Delete all messages <= seqno (they are stable, that is, have been
     * received at all members). Stop when a number > seqno is encountered
     * (all messages are ordered on seqnos).
     */
    public void stable(long seqno) {
        Entry e;

        lock.writeLock();
        try {
            while((e=(Entry)delivered_msgs.peekAtHead()) != null) {
                if(e.seqno > seqno)
                    break;
                else
                    delivered_msgs.removeFromHead();
            }
            _updateLowestSeen();
            _updateHighestSeen();
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * Reset the retransmitter and the nak window<br>
     */
    public void reset() {
        lock.writeLock();
        try {
            if(retransmitter != null)
                retransmitter.reset();
            _reset();
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * Stop the retransmitter and reset the nak window<br>
     */
    public void destroy() {
        lock.writeLock();
        try {
            if(retransmitter != null)
                retransmitter.stop();
            _reset();
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * @return the highest sequence number of a message consumed by the
     * application (by <code>remove()</code>)
     */
    public long getHighestDelivered() {
        lock.readLock();
        try {
            return (Math.max(head - 1, -1));
        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * @return the lowest sequence number of a message that has been
     * delivered or is a candidate for delivery (by the next call to
     * <code>remove()</code>)
     */
    public long getLowestSeen() {
        lock.readLock();
        try {
            return (lowest_seen);
        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Returns the highest deliverable seqno, e.g. for 1,2,3,5,6 it would
     * be 3.
     *
     * @see NakReceiverWindow#getHighestReceived
     */
    public long getHighestSeen() {
        lock.readLock();
        try {
            return (highest_seen);
        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Find all messages between 'low' and 'high' (including 'low' and
     * 'high') that have a null msg.
     * Return them as a list of integers
     *
     * @return List A list of integers, sorted in ascending order.
     * E.g. [1, 4, 7, 8]
     */
    public List getMissingMessages(long low, long high) {
        List retval=new List();
        Entry entry;
        long my_high;

        if(low > high) {

                if(log.isErrorEnabled()) log.error("invalid range: low (" + low +
                                                                      ") is higher than high (" + high + ")");
            return null;
        }

        lock.readLock();
        try {

            my_high=Math.max(head - 1, 0);
            // check only received messages, because delivered messages *must*
            // have a non-null msg
            for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if((entry.seqno >= low) && (entry.seqno <= high) &&
                        (entry.msg == null))
                    retval.add(new Long(entry.seqno));
            }

            if(msgs.size() > 0) {
                entry=(Entry)msgs.peek();
                if(entry != null) my_high=entry.seqno;
            }
            for(long i=my_high + 1; i <= high; i++)
                retval.add(new Long(i));

            return (retval.size() == 0 ? null : retval);

        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Returns the highest sequence number received so far (which may be
     * higher than the highest seqno <em>delivered</em> so far, e.g. for
     * 1,2,3,5,6 it would be 6
     *
     * @see NakReceiverWindow#getHighestSeen
     */
    public long getHighestReceived() {
        lock.readLock();
        try {
            return Math.max(tail - 1, -1);
        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Return messages that are higher than <code>seqno</code> (excluding
     * <code>seqno</code>). Check both received <em>and</em> delivered
     * messages.
     */
    public List getMessagesHigherThan(long seqno) {
        List retval=new List();
        Entry entry;

        lock.readLock();
        try {

            // check received messages
            for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if(entry.seqno > seqno) retval.add(entry.msg);
            }

            // check delivered messages (messages retrieved via remove(), not
            // *stable* messages !)
            for(Enumeration e=delivered_msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if(entry.seqno > seqno && entry.msg != null) retval.add(entry.msg.copy());
            }
            return (retval);

        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Return all messages m for which the following holds:
     * m > lower && m <= upper (excluding lower, including upper). Check both
     * <code>msgs</code> and <code>delivered_msgs</code>.
     */
    public List getMessagesInRange(long lower, long upper) {
        List retval=new List();
        Entry entry;

        lock.readLock();
        try {

            // check received messages
            for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if((entry.seqno > lower) && (entry.seqno <= upper))
                    retval.add(entry.msg);
            }
            // check delivered messages (messages retrieved via remove(), not
            // *stable* messages !)
            for(Enumeration e=delivered_msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if(entry.seqno > lower && entry.seqno <= upper && entry.msg != null)
                    retval.add(entry.msg.copy());
            }
            return (retval.size() == 0 ? null : retval);

        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Return a list of all messages for which there is a seqno in
     * <code>missing_msgs</code>. The seqnos of the argument list are
     * supposed to be in ascending order
     */
    public List getMessagesInList(List missing_msgs) {
        List ret=new List();
        Entry entry;

        if(missing_msgs == null) {

                if(log.isErrorEnabled()) log.error("argument list is null");
            return ret;
        }

        lock.readLock();
        try {

            for(Enumeration e=delivered_msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if(missing_msgs.contains(new Long(entry.seqno)) &&
                        (entry.msg != null))
                    ret.add(entry.msg.copy());
            }
            for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                if(missing_msgs.contains(new Long(entry.seqno)) &&
                        (entry.msg != null))
                    ret.add(entry.msg.copy());
            }

            return (ret);

        }
        finally {
            lock.readUnlock();
        }
    }


    public int size() {
        lock.readLock();
        try {
            return msgs.size();
        }
        finally {
            lock.readUnlock();
        }
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        lock.readLock();
        try {
            sb.append("delivered_msgs: " + delivered_msgs);
            sb.append("\nreceived_msgs: " + msgs);
        }
        finally {
            lock.readUnlock();
        }

        return sb.toString();
    }



    /* ------------------------------- Private Methods -------------------------------------- */


    /**
     * Sets the value of lowest_seen to the lowest seqno of the delivered messages (if available), otherwise
     * to the lowest seqno of received messages.
     */
    private void _updateLowestSeen() {
        Entry entry=null;

        // If both delivered and received messages are empty, let the highest
        // seen seqno be the one *before* the one which is expected to be
        // received next by the NakReceiverWindow (head-1)

        // incorrect: if received and delivered msgs are empty, don't do anything: we may have initial values,
        // but both lists are cleaned after some time of inactivity
        // (bela April 19 2004)
        /*
        if((delivered_msgs.size() == 0) && (msgs.size() == 0)) {
            lowest_seen=0;
            return;
        }
        */

        // The lowest seqno is the first seqno of the delivered messages
        entry=(Entry)delivered_msgs.peekAtHead();
        if(entry != null)
            lowest_seen=entry.seqno;
        // If no elements in delivered messages (e.g. due to message garbage collection), use the received messages
        else {
            if(msgs.size() != 0) {
                entry=(Entry)msgs.peekAtHead();
                if((entry != null) && (entry.msg != null))
                    lowest_seen=entry.seqno;
            }
        }
    }


    /**
     * Find the highest seqno that is deliverable or was actually delivered.
     * Returns seqno-1 if there are no messages in the queues (the first
     * message to be expected is always seqno).
     */
    private void _updateHighestSeen() {
        long ret=0;
        Entry entry=null;

        // If both delivered and received messages are empty, let the highest
        // seen seqno be the one *before* the one which is expected to be
        // received next by the NakReceiverWindow (head-1)

        // changed by bela (April 19 2004): we don't change the value if received and delivered msgs are empty
        /*if((delivered_msgs.size() == 0) && (msgs.size() == 0)) {
            highest_seen=0;
            return;
        }*/


        // The highest seqno is the last of the delivered messages, to start with,
        // or again the one before the first seqno expected (if no delivered
        // msgs). Then iterate through the received messages, and find the highest seqno *before* a gap
        entry=(Entry)delivered_msgs.peek();
        if(entry != null)
            ret=entry.seqno;
        else
            ret=Math.max(head - 1, 0);

        // Now check the received msgs head to tail. if there is an entry
        // with a non-null msg, increment ret until we find an entry with
        // a null msg
        for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
            entry=(Entry)e.nextElement();
            if(entry.msg != null)
                ret=entry.seqno;
            else
                break;
        }
        highest_seen=Math.max(ret, 0);
    }


    /**
     * Reset the Nak window. Should be called from within a writeLock()
     * context.
     * <p>
     * i. Delete all received entries<br>
     * ii. Delete alll delivered entries<br>
     * iii. Reset all indices (head, tail, etc.)<br>
     */
    private void _reset() {
        msgs.removeAll();
        delivered_msgs.removeAll();
        head=0;
        tail=0;
        lowest_seen=0;
        highest_seen=0;
    }
    /* --------------------------- End of Private Methods ----------------------------------- */


}

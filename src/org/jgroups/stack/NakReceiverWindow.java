// $Id: NakReceiverWindow.java,v 1.10 2004/05/10 22:27:55 belaban Exp $


package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.List;
import org.jgroups.util.RWLock;
import org.jgroups.util.TimeScheduler;

import java.util.*;



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
 * @author Bela Ban May 27 1999, May 2004
 * @author John Georgiadis May 8 2001
 */
public class NakReceiverWindow {




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

    /** TreeMap<Long,Message>. Maintains messages keyed by sequence numbers */
    private TreeMap received_msgs=new TreeMap();

    /** TreeMap<Long,Message>. Delivered (= seen by all members) messages. A remove() method causes a message to be
     moved from received_msgs to delivered_msgs. Message garbage colection will gradually remove elements in this map */
    private TreeMap delivered_msgs=new TreeMap();

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
                received_msgs.put(new Long(seqno), msg);
                tail++;
            }
            // gap detected
            // i. add placeholders, creating gaps
            // ii. add real msg
            // iii. tell retransmitter to retrieve missing msgs
            else if(seqno > tail) {
                for(long i=tail; i < seqno; i++) {
                    received_msgs.put(new Long(i), null);
                    // XmitEntry xmit_entry=new XmitEntry();
                    //xmits.put(new Long(i), xmit_entry);
                    tail++;
                }
                received_msgs.put(new Long(seqno), msg);
                tail=seqno + 1;
                if(retransmitter != null) {
                    retransmitter.add(old_tail, seqno - 1);
                }
                // finally received missing message
            }
            else if(seqno < tail) {
                if(log.isTraceEnabled())
                    log.trace("added missing msg " + msg.getSrc() + "#" + seqno);

                Object val=received_msgs.get(new Long(seqno));
                if(val == null) {
                    // only set message if not yet received (bela July 23 2003)
                    received_msgs.put(new Long(seqno), msg);

                    //XmitEntry xmit_entry=(XmitEntry)xmits.get(new Long(seqno));
                    //if(xmit_entry != null)
                    //  xmit_entry.received=System.currentTimeMillis();
                    //long xmit_diff=xmit_entry == null? -1 : xmit_entry.received - xmit_entry.created;
                    //NAKACK.addXmitResponse(msg.getSrc(), seqno);
                    if(retransmitter != null) retransmitter.remove(seqno);
                }
            }
            updateLowestSeen();
            updateHighestSeen();
        }
        finally {
            lock.writeUnlock();
        }
    }


    /**
     * Returns the first entry (with the lowest seqno) from the received_msgs map if its associated message is not
     * null, otherwise returns null. The entry is then added to delivered_msgs
     */
    public Message remove() {
        Message retval=null;
        Object key;

        lock.writeLock();
        try {
            if(received_msgs.size() > 0) {
                key=received_msgs.firstKey();
                retval=(Message)received_msgs.get(key);
                if(retval != null) {
                    received_msgs.remove(key);       // move from received_msgs to ...
                    delivered_msgs.put(key, retval); // delivered_msgs
                    head++;
                }
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
        lock.writeLock();
        try {
            // we need to remove all seqnos *including* seqno: because headMap() *excludes* seqno, we
            // simply increment it, so we have to correct behavior
            SortedMap m=delivered_msgs.headMap(new Long(seqno +1));
            if(m.size() > 0)
                lowest_seen=Math.max(lowest_seen, ((Long)m.lastKey()).longValue());
            m.clear(); // removes entries from delivered_msgs
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
     * Return them as a list of longs
     *
     * @return List<Long>. A list of seqnos, sorted in ascending order.
     * E.g. [1, 4, 7, 8]
     */
    public List getMissingMessages(long low, long high) {
        List retval=new List();
        // long my_high;

        if(low > high) {
            if(log.isErrorEnabled()) log.error("invalid range: low (" + low +
                    ") is higher than high (" + high + ")");
            return null;
        }

        lock.readLock();
        try {

            // my_high=Math.max(head - 1, 0);
            // check only received messages, because delivered messages *must* have a non-null msg
            SortedMap m=received_msgs.subMap(new Long(low), new Long(high+1));
            for(Iterator it=m.keySet().iterator(); it.hasNext();) {
                retval.add(it.next());
            }

//            if(received_msgs.size() > 0) {
//                entry=(Entry)received_msgs.peek();
//                if(entry != null) my_high=entry.seqno;
//            }
//            for(long i=my_high + 1; i <= high; i++)
//                retval.add(new Long(i));

            return retval;
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
     * @return List<Message>. All messages that have a seqno greater than <code>seqno</code>
     */
    public List getMessagesHigherThan(long seqno) {
        List retval=new List();

        lock.readLock();
        try {
            // check received messages
            SortedMap m=received_msgs.tailMap(new Long(seqno+1));
            for(Iterator it=m.values().iterator(); it.hasNext();) {
                retval.add((it.next()));
            }

            // we retrieve all msgs whose seqno is strictly greater than seqno (tailMap() *includes* seqno,
            // but we need to exclude seqno, that's why we increment it
            m=delivered_msgs.tailMap(new Long(seqno +1));
            for(Iterator it=m.values().iterator(); it.hasNext();) {
                retval.add(((Message)it.next()).copy());
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
     * <code>received_msgs</code> and <code>delivered_msgs</code>.
     */
    public List getMessagesInRange(long lower, long upper) {
        List retval=new List();

        lock.readLock();
        try {
            // check received messages
            SortedMap m=received_msgs.subMap(new Long(lower +1), new Long(upper +1));
            for(Iterator it=m.values().iterator(); it.hasNext();) {
                retval.add(it.next());
            }

            m=delivered_msgs.subMap(new Long(lower +1), new Long(upper +1));
            for(Iterator it=m.values().iterator(); it.hasNext();) {
                retval.add(((Message)it.next()).copy());
            }
            return retval;

        }
        finally {
            lock.readUnlock();
        }
    }


    /**
     * Return a list of all messages for which there is a seqno in
     * <code>missing_msgs</code>. The seqnos of the argument list are
     * supposed to be in ascending order
     * @param missing_msgs A List<Long> of seqnos
     * @return List<Message>
     */
    public List getMessagesInList(List missing_msgs) {
        List ret=new List();

        if(missing_msgs == null) {
            if(log.isErrorEnabled()) log.error("argument list is null");
            return ret;
        }

        lock.readLock();
        try {
            Long seqno;
            Message msg;
            for(Enumeration en=missing_msgs.elements(); en.hasMoreElements();) {
                seqno=(Long)en.nextElement();
                msg=(Message)delivered_msgs.get(seqno);
                if(msg != null)
                    ret.add(msg.copy());
                msg=(Message)received_msgs.get(seqno);
                if(msg != null)
                    ret.add(msg.copy());
            }
            return ret;
        }
        finally {
            lock.readUnlock();
        }
    }


    public int size() {
        lock.readLock();
        try {
            return received_msgs.size();
        }
        finally {
            lock.readUnlock();
        }
    }


    public String toString() {
        StringBuffer sb=new StringBuffer();
        lock.readLock();
        try {
            sb.append("received_msgs: " + printReceivedMessages());
            sb.append(", delivered_msgs: " + printDeliveredMessages());
        }
        finally {
            lock.readUnlock();
        }

        return sb.toString();
    }


    /**
     * Prints delivered_msgs. Requires read lock present.
     * @return
     */
    String printDeliveredMessages() {
        StringBuffer sb=new StringBuffer();
        Long min=null, max=null;

        if(delivered_msgs.size() > 0) {
            try {min=(Long)delivered_msgs.firstKey();} catch(NoSuchElementException ex) {}
            try {max=(Long)delivered_msgs.lastKey();}  catch(NoSuchElementException ex) {}
        }
        sb.append("[").append(min).append(" - ").append(max).append("]");
        return sb.toString();
    }


    /**
     * Prints received_msgs. Requires read lock to be present
     * @return
     */
    String printReceivedMessages() {
        StringBuffer sb=new StringBuffer();
        sb.append("[");
        if(received_msgs.size() > 0) {
            Long first=null, last=null;
            try {first=(Long)received_msgs.firstKey();} catch(NoSuchElementException ex) {}
            try {last=(Long)received_msgs.lastKey();}   catch(NoSuchElementException ex) {}
            sb.append(first).append(" - ").append(last);
            int non_received=0;
            Map.Entry entry;

            for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
                entry=(Map.Entry)it.next();
                if(entry.getValue() == null)
                    non_received++;
            }
            sb.append(" (size=").append(received_msgs.size()).append(", missing=").append(non_received).append(")");
        }
        sb.append("]");
        return sb.toString();
    }

    /* ------------------------------- Private Methods -------------------------------------- */


    /**
     * Sets the value of lowest_seen to the lowest seqno of the delivered messages (if available), otherwise
     * to the lowest seqno of received messages.
     */
    private void updateLowestSeen() {
        Long  lowest_seqno=null;

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
        if(delivered_msgs.size() > 0) {
            try {
                lowest_seqno=(Long)delivered_msgs.firstKey();
                if(lowest_seqno != null)
                    lowest_seen=lowest_seqno.longValue();
            }
            catch(NoSuchElementException ex) {
            }
        }
        // If no elements in delivered messages (e.g. due to message garbage collection), use the received messages
        else {
            if(received_msgs.size() > 0) {
                try {
                    lowest_seqno=(Long)received_msgs.firstKey();
                    if(received_msgs.get(lowest_seqno) != null) { // only set lowest_seen if we *have* a msg
                        lowest_seen=lowest_seqno.longValue();
                    }
                }
                catch(NoSuchElementException ex) {}
            }
        }
    }


    /**
     * Find the highest seqno that is deliverable or was actually delivered.
     * Returns seqno-1 if there are no messages in the queues (the first
     * message to be expected is always seqno).
     */
    private void updateHighestSeen() {
        long      ret=0;
        Map.Entry entry=null;

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
        Long highest_seqno=null;
        if(delivered_msgs.size() > 0) {
            try {
                highest_seqno=(Long)delivered_msgs.lastKey();
                ret=highest_seqno.longValue();
            }
            catch(NoSuchElementException ex) {
            }
        }
        else {
            ret=Math.max(head - 1, 0);
        }

        // Now check the received msgs head to tail. if there is an entry
        // with a non-null msg, increment ret until we find an entry with
        // a null msg
        for(Iterator it=received_msgs.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            if(entry.getValue() != null)
                ret=((Long)entry.getKey()).longValue();
            else
                break;
        }
        highest_seen=Math.max(ret, 0);
    }


    /**
     * Reset the Nak window. Should be called from within a writeLock() context.
     * <p>
     * i. Delete all received entries<br>
     * ii. Delete alll delivered entries<br>
     * iii. Reset all indices (head, tail, etc.)<br>
     */
    private void _reset() {
        received_msgs.clear();
        delivered_msgs.clear();
        head=0;
        tail=0;
        lowest_seen=0;
        highest_seen=0;
    }
    /* --------------------------- End of Private Methods ----------------------------------- */


}




package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.RetransmitTable;
import org.jgroups.util.TimeScheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
 *
 * There are 2 variables which keep track of messages:
 * <ul>
 * <li>highest_delivered: the highest delivered seqno, updated on remove(). The next message to be removed is highest_delivered + 1
 * <li>highest_received: the highest received message, updated on add (if a new message is added, not updated e.g.
 * if a missing msg was received)
 * </ul>
 * <p/>
 * Note that the first seqno expected is 1. This design is described in doc/design.NAKACK.txt
 * <p/>
 * Example:
 * 1,2,3,5,6,8: highest_delivered=2 (or 3, depending on whether remove() was called !), highest_received=8
 * 
 * @author Bela Ban
 */
public class NakReceiverWindow {

    public interface Listener {
        void missingMessageReceived(long seqno, Address original_sender);
        void messageGapDetected(long from, long to, Address src);
    }

    private final Lock lock=new ReentrantLock();

    private volatile boolean running=true;

    /** The highest delivered seqno, updated on remove(). The next message to be removed is highest_delivered + 1 */
    @GuardedBy("lock")
    private long highest_delivered=0;

    /** The highest received message, updated on add (if a new message is added, not updated e.g. if a missing msg
     * was received) */
    @GuardedBy("lock")
    private long highest_received=0;


    /**
     * ConcurrentMap<Long,Message>. Maintains messages keyed by (sorted) sequence numbers
     */
    private final RetransmitTable xmit_table;


    private final AtomicBoolean processing=new AtomicBoolean(false);

    /** if not set, no retransmitter thread will be started. Useful if
     * protocols do their own retransmission (e.g PBCAST) */
    private Retransmitter retransmitter=null;

    private Listener listener=null;

    protected static final Log log=LogFactory.getLog(NakReceiverWindow.class);

    /** The highest stable() seqno received */
    long highest_stability_seqno=0;

    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be started
     * @param highest_delivered_seqno The next seqno to remove is highest_delivered_seqno +1
     * @param sched the external scheduler to use for retransmission
     * requests of missing msgs. If it's not provided or is null, an internal
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long highest_delivered_seqno,
                             TimeScheduler sched) {
        this(sender, cmd, highest_delivered_seqno, sched, true);
    }



    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd,
                             long highest_delivered_seqno, TimeScheduler sched,
                             boolean use_range_based_retransmitter) {
        this(sender, cmd, highest_delivered_seqno, sched, use_range_based_retransmitter,
             5, 10000, 1.2, 5 * 60 * 1000, false);
    }


    public NakReceiverWindow(final Address sender, Retransmitter.RetransmitCommand cmd,
                             long highest_delivered_seqno, TimeScheduler sched,
                             boolean use_range_based_retransmitter,
                             int num_rows, int msgs_per_row, double resize_factor, long max_compaction_time,
                             boolean automatic_purging) {
        highest_delivered=highest_delivered_seqno;
        highest_received=highest_delivered;
        if(sched == null)
            throw new IllegalStateException("timer has to be provided and cannot be null");
        if(cmd != null) {
            retransmitter=use_range_based_retransmitter?
              new RangeBasedRetransmitter(sender, cmd, sched) :
              new DefaultRetransmitter(sender, cmd, sched);
        }

        xmit_table=new RetransmitTable(num_rows, msgs_per_row, highest_delivered, resize_factor, max_compaction_time, automatic_purging);
    }


   

    public AtomicBoolean getProcessing() {
        return processing;
    }

    public void setRetransmitTimeouts(Interval timeouts) {
        retransmitter.setRetransmitTimeouts(timeouts);
    }

    public void setXmitStaggerTimeout(long timeout) {
        if(retransmitter != null)
            retransmitter.setXmitStaggerTimeout(timeout);
    }


    public void setListener(Listener l) {
        this.listener=l;
    }

    public int getPendingXmits() {
        return retransmitter!= null? retransmitter.size() : 0;
    }

    // No need to acquire the lock as this method is called via JMX, and stale values are acceptable. Ditto for the next
    // few methods below
    public int getRetransmitTableSize() {return xmit_table.size();}

    public int getRetransmitTableCapacity() {return xmit_table.capacity();}

    public double getRetransmitTableFillFactor() {return xmit_table.getFillFactor();}

    public long getRetransmitTableOffset() {return xmit_table.getOffset();}

    public void compact() {
        lock.lock();
        try {
            xmit_table.compact();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Adds a message according to its seqno (sequence number).
     * <p>
     * There are 4 cases where messages are added:
     * <ol>
     * <li>seqno is the next to be expected seqno: added to map
     * <li>seqno is <= highest_delivered: discard as we've already delivered it
     * <li>seqno is smaller than the next expected seqno: missing message, add it
     * <li>seqno is greater than the next expected seqno: add it to map and fill the gaps with null messages
     *     for retransmission. Add the seqno to the retransmitter too
     * </ol>
     * @return True if the message was added successfully, false otherwise (e.g. duplicate message)
     */
    public boolean add(final long seqno, final Message msg) {
        long old_next, next_to_add;
        boolean missing_msg_received=false;

        lock.lock();
        try {
            if(!running)
                return false;

            next_to_add=highest_received +1;
            old_next=next_to_add;

            // Case #1: we received the expected seqno: most common path
            if(seqno == next_to_add) {
                xmit_table.put(seqno, msg);
                return true;
            }

            // Case #2: we received a message that has already been delivered: discard it
            if(seqno <= highest_delivered) {
                if(log.isTraceEnabled())
                    log.trace("seqno " + seqno + " is smaller than " + next_to_add + "); discarding message");
                return false;
            }

            // Case #3: we finally received a missing message. Case #2 handled seqno <= highest_delivered, so this
            // seqno *must* be between highest_delivered and next_to_add 
            if(seqno < next_to_add) {
                Message existing=xmit_table.putIfAbsent(seqno, msg);
                if(existing != null)
                    return false; // key/value was present
                retransmitter.remove(seqno);
                missing_msg_received=true;

                if(log.isTraceEnabled())
                    log.trace(new StringBuilder("added missing msg ").append(msg.getSrc()).append('#').append(seqno));
                return true;
            }

            // Case #4: we received a seqno higher than expected: add to Retransmitter
            if(seqno > next_to_add) {
                xmit_table.put(seqno, msg);
                retransmitter.add(old_next, seqno -1);
                if(listener != null) {
                    try {listener.messageGapDetected(next_to_add, seqno, msg.getSrc());} catch(Throwable t) {}
                }
                return true;
            }
        }
        finally {
            highest_received=Math.max(highest_received, seqno);
            lock.unlock();
            if(listener != null && missing_msg_received) {
                try {listener.missingMessageReceived(seqno, msg.getSrc());} catch(Throwable t) {}
            }
        }
        return true;
    }



    public Message remove() {
        return remove(true, false);
    }


    public Message remove(boolean acquire_lock, boolean remove_msg) {
        Message retval;

        if(acquire_lock)
            lock.lock();
        try {
            long next=highest_delivered +1;
            retval=remove_msg? xmit_table.remove(next) : xmit_table.get(next);

            if(retval != null) { // message exists and is ready for delivery
                highest_delivered=next;
                return retval;
            }
            return null;
        }
        finally {
            if(acquire_lock)
                lock.unlock();
        }
    }


    /**
     * Removes as many messages as possible
     * @return List<Message> A list of messages, or null if no available messages were found
     */
    public List<Message> removeMany(final AtomicBoolean processing) {
        return removeMany(processing, false, 0);
    }


    /**
     * Removes as many messages as possible
     * @param remove_msgs Removes messages from xmit_table
     * @param max_results Max number of messages to remove in one batch
     * @return List<Message> A list of messages, or null if no available messages were found
     */
    public List<Message> removeMany(final AtomicBoolean processing, boolean remove_msgs, int max_results) {
        List<Message> retval=null;
        int num_results=0;

        lock.lock();
        try {
            while(true) {
                long next=highest_delivered +1;
                Message msg=remove_msgs? xmit_table.remove(next) : xmit_table.get(next);
                if(msg != null) { // message exists and is ready for delivery
                    highest_delivered=next;
                    if(retval == null)
                        retval=new LinkedList<>();
                    retval.add(msg);
                    if(max_results <= 0 || ++num_results < max_results)
                        continue;
                }

                if((retval == null || retval.isEmpty()) && processing != null)
                    processing.set(false);
                return retval;
            }
        }
        finally {
            lock.unlock();
        }
    }



    /**
     * Delete all messages <= seqno (they are stable, that is, have been delivered by all members).
     * Stop when a number > seqno is encountered (all messages are ordered on seqnos).
     */
    public void stable(long seqno) {
        lock.lock();
        try {
            if(seqno > highest_delivered) {
                if(log.isWarnEnabled())
                    log.warn("seqno " + seqno + " is > highest_delivered (" + highest_delivered + ";) ignoring stability message");
                return;
            }


            xmit_table.purge(seqno); // we need to remove all seqnos *including* seqno
            
            // remove all seqnos below (and including) seqno from retransmission

            /** We don't need to remove a range from the retransmitter, as the messages in the range will always be empty:
                - When we get a stable() message, it'll only include seqnos that were *delivered* by everyone
                - To get a seqno delivered, all seqnos below it must have been delivered (no gaps)
                - Therefore all seqnos below seqno are non-null, and were either never in a retransmitter, or got
                  removed from the retransmitter when a missing message was received.
                ==> The call below is therefore unneeded, as it will never remove any seqnos from a retransmitter !
                belaban Aug 2011
                // retransmitter.remove(seqno, true);
             **/

            highest_stability_seqno=Math.max(highest_stability_seqno, seqno);
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Destroys the NakReceiverWindow. After this method returns, no new messages can be added and a new
     * NakReceiverWindow should be used instead. Note that messages can still be <em>removed</em> though.
     */
    public void destroy() {
        lock.lock();
        try {
            running=false;
            retransmitter.reset();
            xmit_table.clear();
            highest_delivered=highest_received=highest_stability_seqno=0;
        }
        finally {
            lock.unlock();
        }
    }


    /** Returns the lowest, highest delivered and highest received seqnos */
    public long[] getDigest() {
        lock.lock();
        try {
            return new long[]{highest_delivered, highest_received};
        }
        finally {
            lock.unlock();
        }
    }



    /** Returns the highest sequence number of a message <em>consumed</em> by the application (by <code>remove()</code>).
     * Note that this is different from the highest <em>deliverable</em> seqno. E.g. in 23,24,26,27,29, the highest
     * <em>delivered</em> message may be 22, whereas the highest <em>deliverable</em> message may be 24 !
     * @return the highest sequence number of a message consumed by the
     * application (by <code>remove()</code>)
     */
    public long getHighestDelivered() {
        lock.lock();
        try {
            return highest_delivered;
        }
        finally {
            lock.unlock();
        }
    }


    public long setHighestDelivered(long new_val) {
        lock.lock();
        try {
            long retval=highest_delivered;
            highest_delivered=new_val;
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Returns the highest sequence number received so far (which may be
     * higher than the highest seqno <em>delivered</em> so far; e.g., for
     * 1,2,3,5,6 it would be 6.
     *
     * @see NakReceiverWindow#getHighestDelivered
     */
    public long getHighestReceived() {
        lock.lock();
        try {
            return highest_received;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Returns the message from xmit_table
     * @param seqno
     * @return Message from xmit_table
     */
    public Message get(long seqno) {
        lock.lock();
        try {
            return xmit_table.get(seqno);
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Returns a list of messages in the range [from .. to], including from and to
     * @param from
     * @param to
     * @return A list of messages, or null if none in range [from .. to] was found
     */
    public List<Message> get(long from, long to) {
        lock.lock();
        try {
            return xmit_table.get(from, to);
        }
        finally {
            lock.unlock();
        }
    }


    public int size() {
        lock.lock();
        try {
            return xmit_table.size();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of bytes taken up by all of the messages in the RetransmitTable
     * @param include_headers
     * @return
     */
    public long sizeOfAllMessages(boolean include_headers) {
        lock.lock();
        try {
            return xmit_table.sizeOfAllMessages(include_headers);
        }
        finally {
            lock.unlock();
        }
    }

    public int getMissingMessages() {
        lock.lock();
        try {
            return xmit_table.getNullMessages(highest_delivered, highest_received);
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        lock.lock();
        try {
            return printMessages();
        }
        finally {
            lock.unlock();
        }
    }



    /**
     * Prints xmit_table. Requires read lock to be present
     * @return String
     */
    protected String printMessages() {
        StringBuilder sb=new StringBuilder();
        lock.lock();
        try {
            sb.append('[').append(highest_delivered).append(" (").append(highest_received).append(")");
            if(xmit_table != null && !xmit_table.isEmpty()) {
                int non_received=xmit_table.getNullMessages(highest_delivered, highest_received);
                sb.append(" (size=").append(xmit_table.size()).append(", missing=").append(non_received).
                  append(", highest stability=").append(highest_stability_seqno).append(')');
            }
            sb.append(']');
            return sb.toString();
        }
        finally {
            lock.unlock();
        }
    }

    public String printLossRate() {
        StringBuilder sb=new StringBuilder();
        int num_missing=getPendingXmits();
        int num_received=size();
        int total=num_missing + num_received;
        sb.append("total=").append(total).append(" (received=").append(num_received).append(", missing=")
          .append(num_missing).append(")");
        return sb.toString();
    }

    public String printRetransmitStats() {
        return retransmitter instanceof RangeBasedRetransmitter? ((RangeBasedRetransmitter)retransmitter).printStats() : "n/a";
    }

    /* ------------------------------- Private Methods -------------------------------------- */



    /* --------------------------- End of Private Methods ----------------------------------- */


}

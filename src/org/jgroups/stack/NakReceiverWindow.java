


package org.jgroups.stack;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.util.TimeScheduler;

import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


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
 * There are 3 variables which keep track of messages:
 * <ul>
 * <li>low: lowest seqno, modified on stable(). On stable(), we purge msgs [low digest.highest_delivered]
 * <li>highest_delivered: the highest delivered seqno, updated on remove(). The next message to be removed is highest_delivered + 1
 * <li>highest_received: the highest received message, updated on add (if a new message is added, not updated e.g.
 * if a missing msg was received)
 * </ul>
 * <p/>
 * Note that the first seqno expected is 1. This design is described in doc/design.NAKACK.txt
 * <p/>
 * Example:
 * 1,2,3,5,6,8: low=1, highest_delivered=2 (or 3, depending on whether remove() was called !), highest_received=8
 * 
 * @author Bela Ban May 27 1999, May 2004, Jan 2007
 * @author John Georgiadis May 8 2001
 * @version $Id: NakReceiverWindow.java,v 1.65 2009/05/13 13:06:56 belaban Exp $
 */
public class NakReceiverWindow {

    public interface Listener {
        void missingMessageReceived(long seqno, Address original_sender);

        void messageGapDetected(long from, long to, Address src);
    }

    private final ReadWriteLock lock=new ReentrantReadWriteLock();

    Address local_addr=null;


    /** Lowest seqno, modified on stable(). On stable(), we purge msgs [low digest.highest_delivered] */
    @GuardedBy("lock")
    private long low=0;

    /** The highest delivered seqno, updated on remove(). The next message to be removed is highest_delivered + 1 */
    @GuardedBy("lock")
    private long highest_delivered=0;

    /** The highest received message, updated on add (if a new message is added, not updated e.g. if a missing msg
     * was received) */
    @GuardedBy("lock")
    private long highest_received=0;


    /** ConcurrentMap<Long,Message>. Maintains messages keyed by (sorted) sequence numbers */
    private final ConcurrentMap<Long,Message> xmit_table=new ConcurrentHashMap<Long,Message>();

    /**
     * Messages that have been received in order are sent up the stack (= delivered to the application). Delivered
     * messages are removed from NakReceiverWindow.xmit_table and moved to NakReceiverWindow.delivered_msgs, where
     * they are later garbage collected (by STABLE). Since we do retransmits only from sent messages, never
     * received or delivered messages, we can turn the moving to delivered_msgs off, so we don't keep the message
     * around, and don't need to wait for garbage collection to remove them.
     */
    private boolean discard_delivered_msgs=false;

    private final AtomicBoolean processing=new AtomicBoolean(false);

    /** If value is > 0, the retransmit buffer is bounded: only the max_xmit_buf_size latest messages are kept,
     * older ones are discarded when the buffer size is exceeded. A value <= 0 means unbounded buffers
     */
    private int max_xmit_buf_size=0;

    /** if not set, no retransmitter thread will be started. Useful if
     * protocols do their own retransmission (e.g PBCAST) */
    private Retransmitter retransmitter=null;

    private Listener listener=null;

    protected static final Log log=LogFactory.getLog(NakReceiverWindow.class);

    /** The highest stable() seqno received */
    long highest_stability_seqno=0;

    /** The loss rate (70% of the new value and 30% of the old value) */
    private double smoothed_loss_rate=0.0;


    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be started
     * @param highest_delivered_seqno The next seqno to remove is highest_delivered_seqno +1
     * @param lowest_seqno The low seqno purged
     * @param sched the external scheduler to use for retransmission
     * requests of missing msgs. If it's not provided or is null, an internal
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long highest_delivered_seqno, long lowest_seqno, TimeScheduler sched) {
        this(null, sender, cmd, highest_delivered_seqno, lowest_seqno, sched);
    }


    public NakReceiverWindow(Address local_addr, Address sender, Retransmitter.RetransmitCommand cmd, long highest_delivered_seqno, long lowest_seqno, TimeScheduler sched) {
        this.local_addr=local_addr;
        highest_delivered=highest_delivered_seqno;
        highest_received=highest_delivered;
        low=Math.min(lowest_seqno, highest_delivered);
        if(sched == null)
            throw new IllegalStateException("timer has to be provided and cannot be null");
        if(cmd != null)
            retransmitter=new Retransmitter(sender, cmd, sched);
    }


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
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long highest_delivered_seqno, TimeScheduler sched) {
        this(sender, cmd, highest_delivered_seqno, 0, sched);
    }

    /**
     * Creates a new instance with the given retransmit command
     *
     * @param sender The sender associated with this instance
     * @param cmd The command used to retransmit a missing message, will
     * be invoked by the table. If null, the retransmit thread will not be started
     * @param highest_delivered_seqno The next seqno to remove is highest_delivered_seqno +1
     */
    public NakReceiverWindow(Address sender, Retransmitter.RetransmitCommand cmd, long highest_delivered_seqno) {
        this(sender, cmd, highest_delivered_seqno, null);
    }

    public AtomicBoolean getProcessing() {
        return processing;
    }

    public void setRetransmitTimeouts(Interval timeouts) {
        retransmitter.setRetransmitTimeouts(timeouts);
    }


    public void setDiscardDeliveredMessages(boolean flag) {
        this.discard_delivered_msgs=flag;
    }

    public int getMaxXmitBufSize() {
        return max_xmit_buf_size;
    }

    public void setMaxXmitBufSize(int max_xmit_buf_size) {
        this.max_xmit_buf_size=max_xmit_buf_size;
    }

    public void setListener(Listener l) {
        this.listener=l;
    }

    public int getPendingXmits() {
        return retransmitter!= null? retransmitter.size() : 0;
    }

    /**
     * Returns the loss rate, which is defined as the number of pending retransmission requests / the total number of
     * messages in xmit_table
     * @return The loss rate
     */
    public double getLossRate() {
        int total_msgs=size();
        int pending_xmits=getPendingXmits();
        if(pending_xmits == 0 || total_msgs == 0)
            return 0.0;

        return pending_xmits / (double)total_msgs;
    }

    public double getSmoothedLossRate() {
        return smoothed_loss_rate;
    }

    /** Set the new smoothed_loss_rate value to 70% of the new value and 30% of the old value */
    private void setSmoothedLossRate() {
        double new_loss_rate=getLossRate();
        if(smoothed_loss_rate == 0) {
            smoothed_loss_rate=new_loss_rate;
        }
        else {
            smoothed_loss_rate=smoothed_loss_rate * .3 + new_loss_rate * .7;
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
    public boolean add(long seqno, Message msg) {
        long old_next, next_to_add;

        lock.writeLock().lock();
        try {
            next_to_add=highest_received +1;
            old_next=next_to_add;

            // Case #1: we received the expected seqno: most common path
            if(seqno == next_to_add) {
                xmit_table.put(new Long(seqno), msg);
                return true;
            }

            // Case #2: we received a message that has already been delivered: discard it
            if(seqno <= highest_delivered) {
                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder("seqno ");
                    sb.append(seqno).append(" is smaller than ").append(next_to_add).append("); discarding message");
                    log.trace(sb);
                }
                return false;
            }

            // Case #3: we finally received a missing message. // Case #2 handled seqno <= highest_delivered, so this
            // seqno *must* be between highest_delivered and next_to_add 
            if(seqno < next_to_add) {
                Message tmp=xmit_table.putIfAbsent(seqno, msg); // only set message if not yet received (bela July 23 2003)
                if(tmp == null) { // key/value was not present
                    int num_xmits=retransmitter.remove(seqno);
                    if(log.isTraceEnabled())
                        log.trace(new StringBuilder("added missing msg ").append(msg.getSrc()).append('#').append(seqno));
                    if(listener != null && num_xmits > 0) {
                        try {listener.missingMessageReceived(seqno, msg.getSrc());} catch(Throwable t) {}
                    }
                    return true;
                }
                else { // key/value was present
                    return false;
                }
            }

            // Case #4: we received a seqno higher than expected: add NULL_MSG values for missing messages, add to Retransmitter
            if(seqno > next_to_add) {
                xmit_table.put(seqno, msg);
                retransmitter.add(old_next, seqno -1);     // BUT: add only null messages to xmitter
                if(listener != null) {
                    try {listener.messageGapDetected(next_to_add, seqno, msg.getSrc());} catch(Throwable t) {}
                }
                return true;
            }
        }
        finally {
            highest_received=Math.max(highest_received, seqno);
            // setSmoothedLossRate();
            lock.writeLock().unlock();
        }
        return true;
    }



    public Message remove() {
        Message retval=null;

        lock.writeLock().lock();
        try {
            long next_to_remove=highest_delivered +1;
            retval=xmit_table.get(next_to_remove);

            if(retval != null) { // message exists and is ready for delivery
                if(discard_delivered_msgs) {
                    Address sender=retval.getSrc();
                    if(!local_addr.equals(sender)) { // don't remove if we sent the message !
                        xmit_table.remove(next_to_remove);
                    }
                }
                highest_delivered=next_to_remove;
                return retval;
            }

            // message has not yet been received (gap in the message sequence stream)
            // drop all messages that have not been received
            if(max_xmit_buf_size > 0 && xmit_table.size() > max_xmit_buf_size) {
                highest_delivered=next_to_remove;
                retransmitter.remove(next_to_remove);
            }
            return null;
        }
        finally {
            // setSmoothedLossRate();
            lock.writeLock().unlock();
        }
    }


    /**
     * Removes a message from the NakReceiverWindow. The message is in order. If the next message cannot be removed
     * (e.g. because there is no message available, or because the next message is not in order), null will
     * be returned. When null is returned, processing is set to false. This is needed to atomically remove a message
     * and set the atomic boolean variable to false.
     * @param processing
     * @return
     */
    public Message remove(final AtomicBoolean processing) {
        Message retval=null;
        boolean found=false;

        lock.writeLock().lock();
        try {
            long next_to_remove=highest_delivered +1;
            retval=xmit_table.get(next_to_remove);
            found=retval != null;

            if(retval != null) { // message exists and is ready for delivery
                if(discard_delivered_msgs) {
                    Address sender=retval.getSrc();
                    if(!local_addr.equals(sender)) { // don't remove if we sent the message !
                        xmit_table.remove(next_to_remove);
                    }
                }
                highest_delivered=next_to_remove;
                return retval;
            }

            // message has not yet been received (gap in the message sequence stream)
            // drop all messages that have not been received
            if(max_xmit_buf_size > 0 && xmit_table.size() > max_xmit_buf_size) {
                highest_delivered=next_to_remove;
                retransmitter.remove(next_to_remove);
            }
            return null;
        }
        finally {
            if(!found)
                processing.set(false);
            // setSmoothedLossRate();
            lock.writeLock().unlock();
        }
    }


    /**
     * Removes as many messages as possible
     * @return List<Message> A list of messages, or null if no available messages were found
     */
    public List<Message> removeMany() {
        List<Message> retval=null;

        lock.writeLock().lock();
        try {
            while(true) {
                long next_to_remove=highest_delivered +1;
                Message msg=xmit_table.get(next_to_remove);

                if(msg != null) { // message exists and is ready for delivery
                    if(discard_delivered_msgs) {
                        Address sender=msg.getSrc();
                        if(!local_addr.equals(sender)) { // don't remove if we sent the message !
                            xmit_table.remove(next_to_remove);
                        }
                    }
                    highest_delivered=next_to_remove;
                    if(retval == null)
                        retval=new LinkedList<Message>();
                    retval.add(msg);
                    continue;
                }

                // message has not yet been received (gap in the message sequence stream)
                // drop all messages that have not been received
                if(max_xmit_buf_size > 0 && xmit_table.size() > max_xmit_buf_size) {
                    highest_delivered=next_to_remove;
                    retransmitter.remove(next_to_remove);
                    continue;
                }

                return retval;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }


    public Message removeOOBMessage() {
        lock.writeLock().lock();
        try {
            Message retval=xmit_table.get(highest_delivered +1);
            if(retval != null && retval.isFlagSet(Message.OOB)) {
                return remove();
            }
            return null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public boolean hasMessagesToRemove() {
        lock.readLock().lock();
        try {
            return xmit_table.get(highest_delivered + 1) != null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public boolean hasRegularMessageToRemove() {
        lock.readLock().lock();
        try {
            Message msg=xmit_table.get(highest_delivered + 1);
            return msg != null && !msg.isFlagSet(Message.OOB);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Delete all messages <= seqno (they are stable, that is, have been received at all members).
     * Stop when a number > seqno is encountered (all messages are ordered on seqnos).
     */
    public void stable(long seqno) {
        lock.writeLock().lock();
        try {
            if(seqno > highest_delivered) {
                if(log.isErrorEnabled())
                    log.error("seqno " + seqno + " is > highest_delivered " + highest_delivered + "; ignoring stability message");
                return;
            }
            
            // we need to remove all seqnos *including* seqno
            if(!xmit_table.isEmpty()) {
                for(long i=low; i <= seqno; i++) {
                    xmit_table.remove(i);
                }
            }
            // remove all seqnos below seqno from retransmission
            for(long i=low; i <= seqno; i++) {
                retransmitter.remove(i);
            }

            highest_stability_seqno=Math.max(highest_stability_seqno, seqno);
            low=Math.max(low, seqno);
        }
        finally {
            lock.writeLock().unlock();
        }
    }


    /**
     * Reset the retransmitter and the nak window<br>
     */
    public void reset() {
        lock.writeLock().lock();
        try {
            retransmitter.reset();
            _reset();
        }
        finally {
            lock.writeLock().unlock();
        }
    }




    /**
     * @return the lowest sequence number of a message that has been
     * delivered or is a candidate for delivery (by the next call to
     * <code>remove()</code>)
     */
    public long getLowestSeen() {
        lock.readLock().lock();
        try {
            return low;
        }
        finally {
            lock.readLock().unlock();
        }
    }


    /** Returns the highest sequence number of a message <em>consumed</em> by the application (by <code>remove()</code>).
     * Note that this is different from the highest <em>deliverable</em> seqno. E.g. in 23,24,26,27,29, the highest
     * <em>delivered</em> message may be 22, whereas the highest <em>deliverable</em> message may be 24 !
     * @return the highest sequence number of a message consumed by the
     * application (by <code>remove()</code>)
     */
    public long getHighestDelivered() {
        lock.readLock().lock();
        try {
            return highest_delivered;
        }
        finally {
            lock.readLock().unlock();
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
        lock.readLock().lock();
        try {
            return highest_received;
        }
        finally {
            lock.readLock().unlock();
        }
    }


    /**
     * Returns the message from xmit_table
     * @param seqno
     * @return Message from xmit_table
     */
    public Message get(long seqno) {
        lock.readLock().lock();
        try {
            return xmit_table.get(seqno);
        }
        finally {
            lock.readLock().unlock();
        }
    }


    public int size() {
        lock.readLock().lock();
        try {
            return xmit_table.size();
        }
        finally {
            lock.readLock().unlock();
        }
    }


    public String toString() {
        lock.readLock().lock();
        try {
            return printMessages();
        }
        finally {
            lock.readLock().unlock();
        }
    }




    /**
     * Prints xmit_table. Requires read lock to be present
     * @return String
     */
    String printMessages() {
        StringBuilder sb=new StringBuilder();
        sb.append('[').append(low).append(" : ").append(highest_delivered).append(" (").append(highest_received).append(")");
        if(xmit_table != null && !xmit_table.isEmpty()) {
            int non_received=0;

            for(Map.Entry<Long,Message> entry: xmit_table.entrySet()) {
                if(entry.getValue() == null)
                    non_received++;
            }
            sb.append(" (size=").append(xmit_table.size()).append(", missing=").append(non_received).
                    append(", highest stability=").append(highest_stability_seqno).append(')');
        }
        sb.append(']');
        return sb.toString();
    }

    public String printLossRate() {
        StringBuilder sb=new StringBuilder();
        int num_missing=getPendingXmits();
        int num_received=size();
        int total=num_missing + num_received;
        sb.append("total=").append(total).append(" (received=").append(num_received).append(", missing=")
                .append(num_missing).append("), loss rate=").append(getLossRate())
                .append(", smoothed loss rate=").append(smoothed_loss_rate);
        return sb.toString();
    }

    /* ------------------------------- Private Methods -------------------------------------- */





    /**
     * Reset the Nak window. Should be called from within a writeLock() context.
     * <p>
     * i. Delete all received entries<br>
     * ii. Reset all indices<br>
     */
    private void _reset() {
        xmit_table.clear();
        low=0;
        highest_delivered=0; // next (=first) to deliver will be 1
        highest_received=0;
        highest_stability_seqno=0;
    }
    /* --------------------------- End of Private Methods ----------------------------------- */


}

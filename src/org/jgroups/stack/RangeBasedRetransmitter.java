
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.XmitRange;
import org.jgroups.util.Range;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This retransmitter is specialized in maintaining <em>ranges of seqnos</em>, e.g. [3-20, [89-89], [100-120].
 * The ranges are stored in a sorted hashmap and the {@link Comparable#compareTo(Object)} method compares both ranges
 * again ranges, and ranges against seqnos. The latter helps to find a range given a seqno, e.g. seqno 105 will find
 * range [100-120].<p/>
 * Each range is implemented by {@link org.jgroups.util.XmitRange}, which has a bitset of all missing seqnos. When
 * a seqno is received, that bit set is updated; the bit corresponding to the seqno is set to 1. A task linked to
 * the range periodically retransmits missing messages.<p/>
 * When all bits are 1 (= all messages have been received), the range is removed from the hashmap and the retransmission
 * task is cancelled.
 *
 * @author Bela Ban
 * @version $Id: RangeBasedRetransmitter.java,v 1.3 2009/11/27 15:36:36 belaban Exp $
 */
public class RangeBasedRetransmitter extends Retransmitter {


    // todo: when JDK 6 is the baseline, convert the TreeMap to a TreeSet or ConcurrentSkipListSet and use ceiling()
    /** Sorted hashmap storing the ranges */
    private final Map<XmitRange,XmitRange> ranges=Collections.synchronizedSortedMap(new TreeMap<XmitRange,XmitRange>());

    /** Association between ranges and retransmission tasks */
    private final Map<XmitRange,Task> tasks=new ConcurrentHashMap<XmitRange,Task>();


    /**
     * Create a new Retransmitter associated with the given sender address
     * @param sender the address from which retransmissions are expected or to which retransmissions are sent
     * @param cmd    the retransmission callback reference
     * @param sched  retransmissions scheduler
     */
    public RangeBasedRetransmitter(Address sender, RetransmitCommand cmd, TimeScheduler sched) {
        super(sender, cmd, sched);
    }



    /**
     * Add the given range [first_seqno, last_seqno] in the list of
     * entries eligible for retransmission. If first_seqno > last_seqno,
     * then the range [last_seqno, first_seqno] is added instead
     */
    public void add(long first_seqno, long last_seqno) {
        if(first_seqno > last_seqno) {
            long tmp=first_seqno;
            first_seqno=last_seqno;
            last_seqno=tmp;
        }

        XmitRange range=new XmitRange(first_seqno, last_seqno);

        // each task needs its own retransmission interval, as they are stateful *and* mutable, so we *need* to copy !
        RangeTask new_task=new RangeTask(range, RETRANSMIT_TIMEOUTS.copy(), cmd, sender);

        XmitRange old_range=ranges.put(range, range);
        if(old_range != null)
            log.error("new range " + range + " overlaps with old range " + old_range);

        tasks.put(range, new_task);
        new_task.doSchedule(); // Entry adds itself to the timer

        if(log.isTraceEnabled())
            log.trace("added range " + sender + " [" + range + "]");
    }

    /**
     * Remove the given sequence number from the list of seqnos eligible
     * for retransmission. If there are no more seqno intervals in the
     * respective entry, cancel the entry from the retransmission
     * scheduler and remove it from the pending entries
     */
    public int remove(long seqno) {
        int retval=0;
        XmitRange dummy_range=new XmitRange(seqno, true);
        XmitRange range=ranges.get(dummy_range);
        if(range == null)
            return 0;
        
        range.set(seqno);
        if(log.isTraceEnabled())
            log.trace("removed " + sender + " #" + seqno + " from retransmitter");

        // if the range has no missing messages, get the associated task and cancel it
        if(range.getNumberOfMissingMessages() == 0) {
            Task task=tasks.get(range);
            if(task != null) {
                task.cancel();
                tasks.remove(range);
                retval=task.getNumRetransmits();
            }            
            ranges.remove(range);
            if(log.isTraceEnabled())
                log.trace("all messages for " + sender + " [" + range + "] have been received; removing range");
        }

        return retval;
    }

    /**
     * Reset the retransmitter: clear all msgs and cancel all the
     * respective tasks
     */
    public void reset() {
        synchronized(ranges) {
            for(XmitRange range: ranges.keySet()) {
                // get task associated with range and cancel it
                Task task=tasks.get(range);
                if(task != null) {
                    task.cancel();
                    tasks.remove(range);
                }
            }

            ranges.clear();
        }

        for(Task task: tasks.values())
            task.cancel();
    }



    public String toString() {
        int missing_msgs=0;

        synchronized(ranges) {
            for(XmitRange range: ranges.keySet()) {
                missing_msgs+=range.getNumberOfMissingMessages();
            }
        }

        StringBuilder sb=new StringBuilder();
        sb.append(missing_msgs).append(" messages to retransmit");
        if(missing_msgs < 50) {
            Collection<Range> all_missing_msgs=new LinkedList<Range>();
            for(XmitRange range: ranges.keySet()) {
                all_missing_msgs.addAll(range.getMessagesToRetransmit());
            }
            sb.append(": ").append(all_missing_msgs);
        }
        return sb.toString();
    }


    public int size() {
        int retval=0;

        synchronized(ranges) {
            for(XmitRange range: ranges.keySet()) {
                retval+=range.getNumberOfMissingMessages();
            }
        }
        return retval;
    }


    protected class RangeTask extends Task {
        protected final XmitRange range;

        protected RangeTask(XmitRange range, Interval intervals, RetransmitCommand cmd, Address msg_sender) {
            super(intervals, cmd, msg_sender);
            this.range=range;
        }


        public String toString() {
            return range.toString();
        }

        protected void callRetransmissionCommand() {
            Collection<Range> missing=range.getMessagesToRetransmit();
            if(missing.isEmpty()) {
                cancel();
            }
            else {
                for(Range range: missing) {
                    command.retransmit(range.low, range.high, msg_sender);
                }
            }
        }
    }



    /* ------------------------------- Private Methods -------------------------------------- */


    /* ---------------------------- End of Private Methods ------------------------------------ */


}

package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This retransmitter is specialized in maintaining <em>ranges of seqnos</em>, e.g. [3-20], [89-89], [100-120].
 * The ranges are stored in a sorted hashmap and the {@link Comparable#compareTo(Object)} method compares both ranges
 * again ranges, and ranges against seqnos. The latter helps to find a range given a seqno, e.g. seqno 105 will find
 * range [100-120].<p/>
 * Each range is implemented by {@link org.jgroups.util.SeqnoRange}, which has a bitset of all missing seqnos. When
 * a seqno is received, that bit set is updated; the bit corresponding to the seqno is set to 1. A task linked to
 * the range periodically retransmits missing messages.<p/>
 * When all bits are 1 (= all messages have been received), the range is removed from the hashmap and the retransmission
 * task is cancelled.<p/>
 * RangeBasedRetransmitter requires sequence numbers and sequence number ranges to be <em>added in ascending and
 * non-overlapping</em> order: e.g. 1 4 [10-21] 22 [50-60] is ok, while [3-7] [5-8] 20 18 is <em>not</em> !
 *
 * @author Bela Ban
 */
public class RangeBasedRetransmitter extends AbstractRetransmitter {

    /** Sorted hashmap storing the ranges */
    private final Map<Seqno,Seqno> ranges=new ConcurrentSkipListMap<>(new SeqnoComparator());

    /** Association between ranges and retransmission tasks */
    private final Map<Seqno,AbstractTask> tasks=new ConcurrentHashMap<>();


    private final AtomicLong num_missing_seqnos=new AtomicLong(0);
    private final AtomicLong num_ranges=new AtomicLong(0);
    private final AtomicLong num_single_msgs=new AtomicLong(0);


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

        num_missing_seqnos.addAndGet(last_seqno - first_seqno +1);

        // create a single seqno if we have no range or else a SeqnoRange
        Seqno range=first_seqno == last_seqno? new Seqno(first_seqno) : new SeqnoRange(first_seqno, last_seqno);
        if(range instanceof SeqnoRange)
            num_ranges.incrementAndGet();
        else
            num_single_msgs.incrementAndGet();

        // each task needs its own retransmission interval, as they are stateful *and* mutable, so we *need* to copy !
        RangeTask new_task=new RangeTask(range, retransmit_timeouts.copy(), cmd, sender);

        Seqno old_range=ranges.put(range, range);
        if(old_range != null)
            log.error(Util.getMessage("NewRange") + range + " overlaps with old range " + old_range);

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
    public void remove(long seqno) {
        Seqno range=ranges.get(new Seqno(seqno, true));
        if(range == null)
            return;
        
        range.set(seqno);

        // if the range has no missing messages, get the associated task and cancel it
        if(range.getNumberOfMissingMessages() == 0) {
            AbstractTask task=tasks.remove(range);
            if(task != null)
                task.cancel();
            else
                log.error(Util.getMessage("TaskForRange") + range + " not found");
            ranges.remove(range);
            if(log.isTraceEnabled())
                log.trace("all messages for " + sender + " [" + range + "] have been received; removing range");
        }
    }



    /**
     * Reset the retransmitter: clear all msgs and cancel all the respective tasks
     */
    public void reset() {
        synchronized(ranges) {
            for(Seqno range: ranges.keySet()) {
                // get task associated with range and cancel it
                AbstractTask task=tasks.get(range);
                if(task != null) {
                    task.cancel();
                    tasks.remove(range);
                }
            }

            ranges.clear();
        }

        for(AbstractTask task: tasks.values())
            task.cancel();

        num_missing_seqnos.set(0);
        num_ranges.set(0);
        num_single_msgs.set(0);
    }



    public String toString() {
        int missing_msgs=0;

        int size=0;
        for(Seqno range: ranges.keySet()) {
            missing_msgs+=range.getNumberOfMissingMessages();
            size++;
        }

        StringBuilder sb=new StringBuilder();
        sb.append(missing_msgs).append(" messages to retransmit");
        if(size < 50) {
            Collection<Range> all_missing_msgs=new LinkedList<>();
            for(Seqno range: ranges.keySet()) {
                all_missing_msgs.addAll(range.getMessagesToRetransmit());
            }
            sb.append(": ").append(all_missing_msgs);
        }
        return sb.toString();
    }


    public int size() {
        int retval=0;

        for(Seqno range: ranges.keySet())
            retval+=range.getNumberOfMissingMessages();
        return retval;
    }


    public String printStats() {
        StringBuilder sb=new StringBuilder();
        sb.append("total seqnos=" + num_missing_seqnos);
        sb.append(", single seqnos=" + num_single_msgs);
        sb.append(", ranges=" + num_ranges);
        double avg_seqnos_per_range=(double)(num_missing_seqnos.get() - num_single_msgs.get()) / num_ranges.get();
        sb.append(", seqnos / range: " + avg_seqnos_per_range);
        return sb.toString();
    }


    protected class RangeTask extends AbstractTask {
        protected final Seqno range;

        protected RangeTask(Seqno range, Interval intervals, RetransmitCommand cmd, Address msg_sender) {
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
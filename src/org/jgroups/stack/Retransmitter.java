// $Id: Retransmitter.java,v 1.23.2.4 2009/09/29 04:34:44 belaban Exp $

package org.jgroups.stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.util.TimeScheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Maintains a pool of sequence numbers of messages that need to be retransmitted. Messages
 * are aged and retransmission requests sent according to age (configurable backoff). If a
 * TimeScheduler instance is given to the constructor, it will be used, otherwise Reransmitter
 * will create its own. The retransmit timeouts have to be set first thing after creating an instance.
 * The <code>add()</code> method adds the sequence numbers of messages to be retransmitted. The
 * <code>remove()</code> method removes a sequence number again, cancelling retransmission requests for it.
 * Whenever a message needs to be retransmitted, the <code>RetransmitCommand.retransmit()</code> method is called.
 * It can be used e.g. by an ack-based scheme (e.g. AckSenderWindow) to retransmit a message to the receiver, or
 * by a nak-based scheme to send a retransmission request to the sender of the missing message.<br/>
 * Changes Aug 2007 (bela): the retransmitter was completely rewritten. Entry was removed, instead all tasks
 * are directly placed into a hashmap, keyed by seqnos. When a message has been received, we simply remove
 * the task from the hashmap and cancel it. This simplifies the code and avoids having to iterate through
 * the (previous) message list linearly on removal. Performance is about the same, or slightly better in
 * informal tests.
 * @author Bela Ban
 * @version $Revision: 1.23.2.4 $
 */
public class Retransmitter {

    private static final long SEC=1000;
    /** Default retransmit intervals (ms) - exponential approx. */
    private Interval                       RETRANSMIT_TIMEOUTS=new StaticInterval(2 * SEC, 3 * SEC, 5 * SEC, 8 * SEC);
    private Address                        sender=null;
    private final ConcurrentMap<Long,Task> msgs=new ConcurrentHashMap<Long,Task>(11);
    private RetransmitCommand              cmd=null;
    private TimeScheduler                  timer=null;
    protected static final Log             log=LogFactory.getLog(Retransmitter.class);


    /** Retransmit command (see Gamma et al.) used to retrieve missing messages */
    public interface RetransmitCommand {
        /**
         * Get the missing messages between sequence numbers
         * <code>first_seqno</code> and <code>last_seqno</code>. This can either be done by sending a
         * retransmit message to destination <code>sender</code> (nak-based scheme), or by
         * retransmitting the missing message(s) to <code>sender</code> (ack-based scheme).
         * @param first_seqno The sequence number of the first missing message
         * @param last_seqno  The sequence number of the last missing message
         * @param sender The destination of the member to which the retransmit request will be sent
         *               (nak-based scheme), or to which the message will be retransmitted (ack-based scheme).
         */
        void retransmit(long first_seqno, long last_seqno, Address sender);
    }


    /**
     * Create a new Retransmitter associated with the given sender address
     * @param sender the address from which retransmissions are expected or to which retransmissions are sent
     * @param cmd the retransmission callback reference
     * @param sched retransmissions scheduler
     */
    public Retransmitter(Address sender, RetransmitCommand cmd, TimeScheduler sched) {
        init(sender, cmd, sched);
    }



    public void setRetransmitTimeouts(Interval interval) {
        if(interval != null)
            RETRANSMIT_TIMEOUTS=interval;
    }


    /**
     * Add the given range [first_seqno, last_seqno] in the list of
     * entries eligible for retransmission. If first_seqno > last_seqno,
     * then the range [last_seqno, first_seqno] is added instead
     * <p>
     * If retransmitter thread is suspended, wake it up
     */
    public void add(long first_seqno, long last_seqno) {
        if(first_seqno > last_seqno) {
            long tmp=first_seqno;
            first_seqno=last_seqno;
            last_seqno=tmp;
        }

        Task new_task;
        for(long seqno=first_seqno; seqno <= last_seqno; seqno++) {
            // each task needs its own retransmission interval, as they are stateful *and* mutable, so we *need* to copy !
            new_task=new Task(seqno, RETRANSMIT_TIMEOUTS.copy(), cmd, sender);
            Task old_task=msgs.putIfAbsent(seqno, new_task);
            if(old_task == null) // only schedule if we actually *added* the new task !
                new_task.doSchedule(); // Entry adds itself to the timer
        }

    }

    /**
     * Remove the given sequence number from the list of seqnos eligible
     * for retransmission. If there are no more seqno intervals in the
     * respective entry, cancel the entry from the retransmission
     * scheduler and remove it from the pending entries
     */
    public int remove(long seqno) {
        Task task=msgs.remove(seqno);
        if(task != null) {
            task.cancel();
            return task.getNumRetransmits();
        }
        return -1;
    }

    /**
     * Reset the retransmitter: clear all msgs and cancel all the
     * respective tasks
     */
    public void reset() {
        for(Task task: msgs.values())
            task.cancel();
        msgs.clear();
    }


    public void stop() {
        reset();
    }


    public String toString() {
        int size=size();
        StringBuilder sb=new StringBuilder();
        sb.append(size).append(" messages to retransmit: ").append(msgs.keySet());
        return sb.toString();
    }


    public int size() {
        return msgs.size();
    }




    /* ------------------------------- Private Methods -------------------------------------- */

    /**
     * Init this object
     *
     * @param sender the address from which retransmissions are expected
     * @param cmd the retransmission callback reference
     * @param sched retransmissions scheduler
     */
    private void init(Address sender, RetransmitCommand cmd, TimeScheduler sched) {
        this.sender=sender;
        this.cmd=cmd;
        timer=sched;
    }


    /* ---------------------------- End of Private Methods ------------------------------------ */



    /**
     * The retransmit task executed by the scheduler in regular intervals
     */
    private class Task implements TimeScheduler.Task {
        private final Interval    intervals;
        private long              seqno=-1;
        private volatile Future   future;
        private Address           msg_sender=null;
        protected volatile int    num_retransmits=0;
        private RetransmitCommand command;
        private volatile boolean  cancelled=false;

        protected Task(long seqno, Interval intervals, RetransmitCommand cmd, Address msg_sender) {
            this.seqno=seqno;
            this.intervals=intervals;
            this.command=cmd;
            this.msg_sender=msg_sender;
        }

        public int getNumRetransmits() {
            return num_retransmits;
        }

        public long nextInterval() {
            return intervals.next();
        }

        public void doSchedule() {
            if(cancelled) {
                return;
            }
            long delay=intervals.next();
            future=timer.schedule(this, delay, TimeUnit.MILLISECONDS);
        }

        public void cancel() {
            if(!cancelled) {
                cancelled=true;
            }
            if(future != null)
                future.cancel(true);
        }

        public void run() {
            if(cancelled) {
                return;
            }
            try {
                command.retransmit(seqno, seqno, msg_sender);
                num_retransmits++;
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error("failed retransmission task", t);
            }
            doSchedule();
        }

        public String toString() {
            return String.valueOf(seqno);
        }
    }


}



package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.util.TimeScheduler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


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
 * @version $Revision: 1.3 $
 */
public class DefaultRetransmitter extends Retransmitter {
    private final ConcurrentMap<Long,Task> msgs=new ConcurrentHashMap<Long,Task>(11);


    /**
     * Create a new Retransmitter associated with the given sender address
     * @param sender the address from which retransmissions are expected or to which retransmissions are sent
     * @param cmd    the retransmission callback reference
     * @param sched  retransmissions scheduler
     */
    public DefaultRetransmitter(Address sender, RetransmitCommand cmd, TimeScheduler sched) {
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

        Task new_task;
        for(long seqno=first_seqno; seqno <= last_seqno; seqno++) {
            // each task needs its own retransmission interval, as they are stateful *and* mutable, so we *need* to copy !
            new_task=new SeqnoTask(seqno, RETRANSMIT_TIMEOUTS.copy(), cmd, sender);
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


    public String toString() {
        Set<Long> keys=msgs.keySet();
        int size=keys.size();
        StringBuilder sb=new StringBuilder();
        sb.append(size).append(" messages to retransmit");
        if(size < 50)
            sb.append(": ").append(keys);
        return sb.toString();
    }


    public int size() {
        return msgs.size();
    }


    protected class SeqnoTask extends Task {
        private long              seqno=-1;

        protected SeqnoTask(long seqno, Interval intervals, RetransmitCommand cmd, Address msg_sender) {
            super(intervals, cmd, msg_sender);
            this.seqno=seqno;
        }


        public String toString() {
            return String.valueOf(seqno);
        }

        protected void callRetransmissionCommand() {
            command.retransmit(seqno, seqno, msg_sender);
        }
    }




}


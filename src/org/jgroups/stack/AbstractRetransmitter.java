
package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Abstract class of a retransmitter.
 * Maintains a pool of sequence numbers of messages that need to be retransmitted. Messages
 * are aged and retransmission requests sent according to age (configurable backoff). If a
 * TimeScheduler instance is given to the constructor, it will be used, otherwise Reransmitter
 * will create its own. The retransmit timeouts have to be set first thing after creating an instance.
 * The <code>add()</code> method adds the sequence numbers of messages to be retransmitted. The
 * <code>remove()</code> method removes a sequence number again, cancelling retransmission requests for it.
 * Whenever a message needs to be retransmitted, the <code>RetransmitCommand.retransmit()</code> method is called.
 * It can be used e.g. by an ack-based scheme (e.g. AckSenderWindow) to retransmit a message to the receiver, or
 * by a nak-based scheme to send a retransmission request to the sender of the missing message.<br/>
 * @author Bela Ban
 */
@Deprecated
public abstract class AbstractRetransmitter {

    /** Default retransmit intervals (ms) - exponential approx. */
    protected Interval                       retransmit_timeouts=new ExponentialInterval(300);
    protected final Address                  sender;
    protected final RetransmitCommand        cmd;
    protected final TimeScheduler            timer;
    protected long                           xmit_stagger_timeout=0;
    protected static final Log log=LogFactory.getLog(AbstractRetransmitter.class);


    /** Retransmit command (see Gamma et al.) used to retrieve missing messages */
    public interface RetransmitCommand {
        /**
         * Get the missing messages between sequence numbers <code>first_seqno</code> and <code>last_seqno</code>.
         * This can either be done by sending a retransmit message to destination <code>sender</code>
         * (nak-based scheme), or by retransmitting the missing message(s) to <code>sender</code> (ack-based scheme).
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
    public AbstractRetransmitter(Address sender, RetransmitCommand cmd, TimeScheduler sched) {
        this.sender=sender;
        this.cmd=cmd;
        timer=sched;
    }



    public void setRetransmitTimeouts(Interval interval) {
        if(interval != null)
            retransmit_timeouts=interval;
    }

    public long getXmitStaggerTimeout() {
        return xmit_stagger_timeout;
    }

    public void setXmitStaggerTimeout(long xmit_stagger_timeout) {
        this.xmit_stagger_timeout=xmit_stagger_timeout;
    }

    /**
     * Add messages from <code>first_seqno</code> to <code>last_seqno</code/> for retransmission
     */
    public abstract void add(long first_seqno, long last_seqno);

    /**
     * Remove the given sequence number from retransmission
     */
    public abstract void remove(long seqno);

    /**
     * Reset the retransmitter: clear all msgs and cancel all the respective tasks
     */
    public abstract void reset();


    public abstract int size();




    /* ------------------------------- Private Methods -------------------------------------- */



    /* ---------------------------- End of Private Methods ------------------------------------ */

    protected abstract class AbstractTask implements TimeScheduler.Task {
        protected final Interval       intervals;
        protected volatile Future      future;
        protected Address              msg_sender=null;
        protected RetransmitCommand    command;
        protected volatile boolean     cancelled=false;

        protected AbstractTask(Interval intervals, RetransmitCommand cmd, Address msg_sender) {
            this.intervals=intervals;
            this.command=cmd;
            this.msg_sender=msg_sender;
        }

        public long nextInterval() {
            return intervals.next();
        }

        public void doSchedule() {
            if(cancelled) {
                return;
            }
            long delay=intervals.next();

            if(xmit_stagger_timeout > 0) {
                long stagger_time=Util.random(xmit_stagger_timeout);
                delay+=stagger_time;
            }

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
                callRetransmissionCommand();
            }
            catch(Throwable t) {
                if(log.isErrorEnabled())
                    log.error(Util.getMessage("FailedRetransmissionTask"), t);
            }
            doSchedule();
        }


        protected abstract void callRetransmissionCommand();
    }


}


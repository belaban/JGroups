package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.stack.MessageProcessingPolicy;

import java.util.Iterator;
import java.util.Objects;

/**
 * Default message processing policy. Submits all received messages and batches to the thread pool
 * @author Bela Ban
 * @since  4.0
 */
public class SubmitToThreadPool implements MessageProcessingPolicy {
    protected TP    tp;
    protected Log   log;

    protected TP getTransport() {return tp;}

    public void init(TP transport) {
        this.tp=transport;
        this.log=Objects.requireNonNull(tp.getLog());
    }

    public boolean loopback(Message msg, boolean oob) {
        return tp.getThreadPool().execute(new SingleMessageHandler(msg, true));
    }

    public boolean loopback(MessageBatch batch, boolean oob) {
        if(oob) {
            boolean removed=removeAndDispatchNonBundledMessages(batch, true);
            if(removed && batch.isEmpty())
                return true;
        }
        return tp.getThreadPool().execute(new BatchHandler(batch, true));
    }

    public boolean process(Message msg, boolean oob) {
        return tp.getThreadPool().execute(new SingleMessageHandler(msg, false));
    }

    public boolean process(MessageBatch batch, boolean oob) {
        if(oob) {
            boolean removed=removeAndDispatchNonBundledMessages(batch, false);
            if(removed && batch.isEmpty())
                return true;
        }
        return tp.getThreadPool().execute(new BatchHandler(batch, false));
    }


    /**
     * Removes messages with flags DONT_BUNDLE and OOB set and executes them in the oob or internal thread pool. JGRP-1737
     * Returns true if at least one message was removed
     */
    protected boolean removeAndDispatchNonBundledMessages(MessageBatch oob_batch, boolean loopback) {
        if(oob_batch == null)
            return false;
        boolean removed=false;
        for(Iterator<Message> it=oob_batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.DONT_BUNDLE) && msg.isFlagSet(Message.Flag.OOB)) {
                it.remove();
                tp.getThreadPool().execute(new SingleMessageHandler(msg, loopback));
                removed=true;
            }
        }
        return removed;
    }


    public class SingleMessageHandler implements Runnable {
        protected final Message msg;
        protected final boolean loopback;

        protected SingleMessageHandler(final Message msg, boolean loopback) {
            this.msg=msg;
            this.loopback=loopback;
        }

        public Message getMessage() {return msg;}

        public void run() {
            try {
                tp.passMessageUp(msg, !loopback, msg.dest() == null, !loopback);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("PassUpFailure"), t);
            }
        }
    }

    public class BatchHandler implements Runnable {
        protected MessageBatch batch;
        protected boolean      loopback;

        public BatchHandler(final MessageBatch batch, boolean loopback) {
            this.batch=batch;
            this.loopback=loopback;
        }

        public void run() {
            if(batch == null || batch.isEmpty() || (!batch.multicast() && tp.unicastDestMismatch(batch.dest())))
                return;
            passBatchUp();
        }

        protected void passBatchUp() {
            tp.passBatchUp(batch, !loopback, !loopback);
        }
    }

}

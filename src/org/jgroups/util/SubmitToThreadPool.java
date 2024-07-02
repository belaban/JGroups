package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.TpHeader;
import org.jgroups.stack.MessageProcessingPolicy;

import java.util.Iterator;

/**
 * Default message processing policy. Submits all received messages and batches to the thread pool
 * @author Bela Ban
 * @since  4.0
 */
public class SubmitToThreadPool implements MessageProcessingPolicy {
    protected TP    tp;
    protected short tp_id;
    protected Log   log;

    protected TP getTransport() {return tp;}

    public void init(TP transport) {
        this.tp=transport;
        this.tp_id=tp.getId();
        this.log=tp.getLog();
    }

    public boolean loopback(Message msg, boolean oob) {
        return tp.getThreadPool().execute(new SingleLoopbackHandler(msg));
    }

    public boolean process(Message msg, boolean oob) {
        return tp.getThreadPool().execute(new SingleMessageHandler(msg));
    }

    public boolean process(MessageBatch batch, boolean oob) {
        if(oob) {
            boolean removed=removeAndDispatchNonBundledMessages(batch);
            if(removed && batch.isEmpty())
                return true;
        }
        return tp.getThreadPool().execute(new BatchHandler(batch));
    }


    /**
     * Removes messages with flags DONT_BUNDLE and OOB set and executes them in the oob or internal thread pool. JGRP-1737
     * Returns true if at least one message was removed
     */
    protected boolean removeAndDispatchNonBundledMessages(MessageBatch oob_batch) {
        if(oob_batch == null)
            return false;
        AsciiString tmp=oob_batch.clusterName();
        byte[] cname=tmp != null? tmp.chars() : null;
        boolean removed=false;
        for(Iterator<Message> it=oob_batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.DONT_BUNDLE) && msg.isFlagSet(Message.Flag.OOB)) {
                it.remove();
                tp.getThreadPool().execute(new SingleMessageHandlerWithClusterName(msg, cname));
                removed=true;
            }
        }
        return removed;
    }

    public class SingleLoopbackHandler implements Runnable {
        protected final Message msg;

        public SingleLoopbackHandler(Message msg) {
            this.msg=msg;
        }

        public void run() {
            tp.passMessageUp(msg, null, false, msg.getDest() == null, false);
        }
    }

    public class SingleMessageHandler implements Runnable {
        protected final Message msg;

        protected SingleMessageHandler(final Message msg) {
            this.msg=msg;
        }

        public Message getMessage() {return msg;}

        public void run() {
            Address dest=msg.getDest();
            boolean multicast=dest == null;
            try {
                byte[] cname=getClusterName();
                tp.passMessageUp(msg, cname, true, multicast, true);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("PassUpFailure"), t);
            }
        }

        protected byte[] getClusterName() {
            TpHeader hdr=msg.getHeader(tp_id);
            return hdr.clusterName();
        }
    }

    protected class SingleMessageHandlerWithClusterName extends SingleMessageHandler {
        protected final byte[] cluster;

        @Override protected byte[] getClusterName() {
            return cluster;
        }

        protected SingleMessageHandlerWithClusterName(Message msg, byte[] cluster_name) {
            super(msg);
            this.cluster=cluster_name;
        }
    }

    public class BatchHandler implements Runnable {
        protected MessageBatch batch;

        public BatchHandler(final MessageBatch batch) {
            this.batch=batch;
        }

        public MessageBatch getBatch() {return batch;}

        public void run() {
            if(batch == null || (!batch.multicast() && tp.unicastDestMismatch(batch.dest())))
                return;
            passBatchUp();
        }

        protected void passBatchUp() {
            tp.passBatchUp(batch, true, true);
        }
    }

}

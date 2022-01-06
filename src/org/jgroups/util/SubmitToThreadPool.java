package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.protocols.MsgStats;
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
        if(oob)
            removeAndDispatchNonBundledMessages(batch);
        return tp.getThreadPool().execute(new BatchHandler(batch));
    }


    /**
     * Removes messages with flags DONT_BUNDLE and OOB set and executes them in the oob or internal thread pool. JGRP-1737
     */
    protected void removeAndDispatchNonBundledMessages(MessageBatch oob_batch) {
        if(oob_batch == null)
            return;
        AsciiString tmp=oob_batch.clusterName();
        byte[] cname=tmp != null? tmp.chars() : null;
        for(Iterator<Message> it=oob_batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg.isFlagSet(Message.Flag.DONT_BUNDLE) && msg.isFlagSet(Message.Flag.OOB)) {
                it.remove();
                if(tp.statsEnabled())
                    tp.getMessageStats().incrNumOOBMsgsReceived(1);
                tp.getThreadPool().execute(new SingleMessageHandlerWithClusterName(msg, cname));
            }
        }
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
                if(tp.statsEnabled()) {
                    MsgStats msg_stats=tp.getMessageStats();
                    boolean oob=msg.isFlagSet(Message.Flag.OOB);
                    if(oob)
                        msg_stats.incrNumOOBMsgsReceived(1);
                    else
                        msg_stats.incrNumMsgsReceived(1);
                    msg_stats.incrNumBytesReceived(msg.getLength());
                }
                byte[] cname=getClusterName();
                tp.passMessageUp(msg, cname, true, multicast, true);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("PassUpFailure"), t);
            }
        }

        protected byte[] getClusterName() {
            TpHeader hdr=msg.getHeader(tp_id);
            return hdr.getClusterName();
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
            if(tp.statsEnabled()) {
                int batch_size=batch.size();
                MsgStats msg_stats=tp.getMessageStats();
                boolean oob=batch.getMode() == MessageBatch.Mode.OOB;
                if(oob)
                    msg_stats.incrNumOOBMsgsReceived(batch_size);
                else
                    msg_stats.incrNumMsgsReceived(batch_size);
                msg_stats.incrNumBatchesReceived(1);
                msg_stats.incrNumBytesReceived(batch.length());
                tp.avgBatchSize().add(batch_size);
            }
            passBatchUp();
        }

        protected void passBatchUp() {
            tp.passBatchUp(batch, true, true);
        }
    }

}

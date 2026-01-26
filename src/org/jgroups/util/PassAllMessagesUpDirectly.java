package org.jgroups.util;

import org.jgroups.Message;

/**
 * {@link org.jgroups.stack.MessageProcessingPolicy} which passes all messages and message batches up directly
 * (on the same thread). This allows for better debugging of wire format issues (entire stack traces down to the
 * read socket).
 * @author Bela Ban
 * @since  5.5.3
 */
public class PassAllMessagesUpDirectly extends SubmitToThreadPool {

    @Override
    public boolean loopback(Message msg, boolean oob) {
        tp.passMessageUp(msg, false, msg.getDest() == null, false);
        return true;
    }

    @Override
    public boolean loopback(MessageBatch batch, boolean oob) {
        if(oob) {
            boolean removed=removeAndDispatchNonBundledMessages(batch, true);
            if(removed && batch.isEmpty())
                return true;
        }
        BatchHandler bh=new BatchHandler(batch, true);
        bh.run();
        return true;
    }

    @Override
    public boolean process(Message msg, boolean oob) {
        SingleMessageHandler smh=new SingleMessageHandler(msg);
        smh.run();
        return true;
    }

    @Override
    public boolean process(MessageBatch batch, boolean oob) {
        BatchHandler bh=new BatchHandler(batch, false);
        bh.run();
        return true;
    }
}

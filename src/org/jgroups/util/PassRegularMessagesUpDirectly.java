package org.jgroups.util;

import org.jgroups.Message;

/**
 * {@link org.jgroups.stack.MessageProcessingPolicy} which passes regular messages and message batches up directly
 * (on the same thread), but passes OOB messages to the thread pool.
 * @author Bela Ban
 * @since  5.2.14
 */
public class PassRegularMessagesUpDirectly extends SubmitToThreadPool {

    @Override
    public boolean loopback(Message msg, boolean oob) {
        if(oob)
            return super.loopback(msg, oob);
        tp.passMessageUp(msg, null, false, msg.getDest() == null, false);
        return true;
    }

    @Override
    public boolean process(Message msg, boolean oob) {
        if(oob)
            return super.process(msg, oob);
        SingleMessageHandler smh=new SingleMessageHandler(msg);
        smh.run();
        return true;
    }

    @Override
    public boolean process(MessageBatch batch, boolean oob) {
        if(oob)
            return super.process(batch, oob);
        BatchHandler bh=new BatchHandler(batch, false);
        bh.run();
        return true;
    }
}

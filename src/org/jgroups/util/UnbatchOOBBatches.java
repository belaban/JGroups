package org.jgroups.util;

import org.jgroups.Message;

/**
 * Same as {@link MaxOneThreadPerSender}, but for OOB message batches, every message of the batch is passed to the
 * thread pool separately (https://issues.redhat.com/browse/JGRP-2800).
 * @author Bela Ban
 * @since  5.4, 5.3.7
 */
public class UnbatchOOBBatches extends MaxOneThreadPerSender {

    @Override
    public boolean process(MessageBatch batch, boolean oob) {
        if(!oob)
            return super.process(batch, oob);
        AsciiString tmp=batch.clusterName();
        byte[] cname=tmp != null? tmp.chars() : null;
        for(Message msg: batch)
            tp.getThreadPool().execute(new SingleMessageHandlerWithClusterName(msg, cname));
        batch.clear();
        return true;
    }
}

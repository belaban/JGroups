package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.annotations.Property;

/**
 * Same as {@link MaxOneThreadPerSender}, but for OOB message batches, every message of the batch is passed to the
 * thread pool separately (https://issues.redhat.com/browse/JGRP-2800).
 * @author Bela Ban
 * @since  5.4, 5.3.7
 */
public class UnbatchOOBBatches extends MaxOneThreadPerSender {

    @Property(description="If > 0, then batches > max_size will be unbatched, batches <= max_size will be " +
      "sent up. This ensures that no batch will ever be greater than a given size.")
    protected int max_size;

    @Override
    public boolean process(MessageBatch batch, boolean oob) {
        if(!oob)
            return super.process(batch, oob);
        if(max_size > 0 && batch.size() <= max_size)
            return super.process(batch, oob);
        for(Message msg: batch)
            tp.getThreadPool().execute(new SingleMessageHandler(msg));
        batch.clear();
        return true;
    }
}

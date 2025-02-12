package org.jgroups.protocols;

import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ThreadPool;

/**
 * Delivers message batches in separate threads. Note that this can destroy ordering for
 * regular message batches. For OOB batches, this is OK.
 * @author Bela Ban
 * @since  5.4.3
 */
@MBean(description="Delivers (OOB) message batches in a seperate thread, so that")
public class ASYNC extends Protocol {
    protected ThreadPool thread_pool;

    @Property(description="Handle regular messages and batches (destroys ordering)")
    protected boolean handle_reg_msgs;

    @Property(description="Process (= async dispatch) only batches >= max_batch_size. Smaller batches are dispatched " +
      "on the same thread. 0 disables this")
    protected int max_batch_size;

    @Override
    public void init() throws Exception {
        thread_pool=getTransport().getThreadPool();
    }

    @Override
    public void up(MessageBatch batch) {
        MessageBatch.Mode mode=batch.mode();
        if((mode == MessageBatch.Mode.OOB || handle_reg_msgs) && batch.size() >= max_batch_size)
            thread_pool.execute(new AsyncBatchDispatcher(batch));
        else
            up_prot.up(batch);
    }

    protected class AsyncBatchDispatcher implements Runnable {
        protected final MessageBatch batch;

        protected AsyncBatchDispatcher(MessageBatch batch) {
            this.batch=batch;
        }

        @Override
        public void run() {
            up_prot.up(batch);
        }
    }
}

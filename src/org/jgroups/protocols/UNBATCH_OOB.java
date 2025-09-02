package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.ThreadPool;

import java.util.concurrent.atomic.LongAdder;

/**
 * Unbatches OOB batches when enabled and virtual threads are available.<p>
 * https://issues.redhat.com/browse/JGRP-2888
 * @author Bela Ban
 * @since  5.4.9
 */
@MBean(description="Unbatches messages from an OOB batch and delivers them in separate threads")
public class UNBATCH_OOB extends Protocol {
    @Property(description="Enables or disables unbatching of OOB messages")
    protected boolean         enabled=true;

    @Property(description="Skip OOB batches smaller than this value. 0 ignores this")
    protected int             min_size;

    @ManagedAttribute(description="Number of unbatched OOB messages",type=AttributeType.SCALAR)
    protected final LongAdder num_unbatched_msgs=new LongAdder();

    @ManagedAttribute(description="Number of OOB batches which were unbatched",type=AttributeType.SCALAR)
    protected final LongAdder num_oob_batches=new LongAdder();

    protected ThreadPool      thread_pool;

    public boolean enabled() {
        return enabled;
    }

    public UNBATCH_OOB enable(boolean enable) {
        this.enabled=enable;
        return this;
    }

    public int minSize() {
        return min_size;
    }

    public UNBATCH_OOB minSize(int min_size) {
        this.min_size=min_size;
        return this;
    }

    @ManagedAttribute(description="Number of unbatches messages per batch")
    public double avgUnbatchedMessagesPerBatch() {
        if(!enabled)
            return 0.0;
        return num_oob_batches.sum() == 0? 0.0 : (double)num_unbatched_msgs.sum() / num_oob_batches.sum();
    }

    @Override
    public void resetStats() {
        super.resetStats();
        num_unbatched_msgs.reset();
        num_oob_batches.reset();
    }

    @Override
    public void start() throws Exception {
        super.start();
        thread_pool=getTransport().getThreadPool();
        if(enabled && !thread_pool.getThreadFactory().useVirtualThreads()) {
            log.warn("%s: virtual threads are not available; setting enabled to false", local_addr);
            enabled=false;
        }
    }

    @Override
    public void up(MessageBatch batch) {
        if(!enabled || batch.mode() != MessageBatch.Mode.OOB) {
            up_prot.up(batch);
            return;
        }
        int size=batch.size();
        if(min_size > 0 && size < min_size) {
            up_prot.up(batch);
            return;
        }
        for(Message msg: batch) {
            boolean rc=thread_pool.execute(() -> passUp(msg));
            if(!rc)
                passUp(msg);
        }
        batch.clear();
        if(size > 0) {
            num_oob_batches.increment();
            num_unbatched_msgs.add(size);
        }
    }

    protected void passUp(Message msg) {
        try {
            up_prot.up(msg);
        }
        catch(Throwable t) {
            log.warn("%s: failed delivering message %s", local_addr, msg);
        }
    }
}

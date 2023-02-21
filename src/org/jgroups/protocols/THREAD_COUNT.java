package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains averages of up- and down-threads
 * @author Bela Ban
 * @since  5.2.13
 */
@MBean(description="Maintains averages of up and down threads")
public class THREAD_COUNT extends Protocol {

    @Property(description="Enables the average for up threads")
    protected boolean up_threads=true;

    @Property(description="Enables the average for down threads")
    protected boolean down_threads=true;

    @ManagedAttribute(description="Average of up threads")
    protected final AverageMinMax avg_up=new AverageMinMax();

    @ManagedAttribute(description="Average of down threads")
    protected final AverageMinMax avg_down=new AverageMinMax();

    protected final AtomicInteger up_count=new AtomicInteger(), down_count=new AtomicInteger();


    @Override
    public Object down(Message msg) {
        if(!down_threads)
            return down_prot.down(msg);
        try {
            int cnt=down_count.incrementAndGet();
            synchronized(avg_down) {
                avg_down.add(cnt);
            }
            return down_prot.down(msg);
        }
        finally {
            down_count.decrementAndGet();
        }
    }

    @Override
    public Object up(Message msg) {
        if(!up_threads)
            return up_prot.up(msg);
        try {
            int cnt=up_count.incrementAndGet();
            synchronized(avg_up) {
                avg_up.add(cnt);
            }
            return up_prot.up(msg);
        }
        finally {
            up_count.decrementAndGet();
        }
    }

    @Override
    public void up(MessageBatch batch) {
        if(!up_threads) {
            up_prot.up(batch);
            return;
        }
        try {
            int cnt=up_count.incrementAndGet();
            synchronized(avg_up) {
                avg_up.add(cnt);
            }
            up_prot.up(batch);
        }
        finally {
            up_count.decrementAndGet();
        }
    }

    @Override
    public void resetStats() {
        super.resetStats();
        avg_up.clear();
        avg_down.clear();
    }
}

package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Protocol which sends at most max_bytes in time_period milliseconds. Can be used instead of a flow control protocol,
 * e.g. UFC or MFC (same position in the stack)
 * @author Bela Ban
 */
@Experimental
@MBean(description="Limits the sending rate to max_bytes per time_period")
public class RATE_LIMITER extends Protocol {

    @Property(description="Max number of bytes to be sent in time_period ms. Blocks the sender if exceeded until a new " +
            "time period has started")
    protected long max_bytes=300_000;

    @Property(description="Number of milliseconds during which max_bytes bytes can be sent")
    protected long time_period=10L;

    protected long time_period_ns;


    /** Keeps track of the number of bytes sent in the current time period */
    @GuardedBy("lock")
    @ManagedAttribute(description="Number of bytes sent in the current time period. Reset after every time period.")
    protected long num_bytes_sent_in_period=0L;

    @GuardedBy("lock")
    protected long current_period_start; // time (ns) at which the current period was started

    protected final Lock lock=new ReentrantLock();

    @ManagedAttribute
    protected int num_blockings=0;

    protected long total_block_time=0L; // ns

    protected int frag_size=0;

    protected volatile boolean running=true;

    public long getMaxBytes() {
        return max_bytes;
    }

    public void setMaxBytes(long max_bytes) {
        this.max_bytes=max_bytes;
    }

    public long getTimePeriod() {
        return time_period;
    }

    public void setTimePeriod(long time_period) {
        this.time_period=time_period;
        this.time_period_ns=TimeUnit.NANOSECONDS.convert(time_period, TimeUnit.MILLISECONDS);
    }

    @ManagedAttribute(description="Total block time in milliseconds")
    public long getTotalBlockTime() {
        return TimeUnit.MILLISECONDS.convert(total_block_time,TimeUnit.NANOSECONDS);
    }

    @ManagedAttribute(description="Average block time in ms (total block time / number of blockings)")
    public double getAverageBlockTime() {
        long block_time_ms=getTotalBlockTime();
        return num_blockings == 0? 0.0 : block_time_ms / (double)num_blockings;
    }

    public void resetStats() {
        super.resetStats();
        num_blockings=0; total_block_time=0;
    }

    public void init() throws Exception {
        super.init();
        if(time_period <= 0)
            throw new IllegalArgumentException("time_period needs to be positive");
        time_period_ns=TimeUnit.NANOSECONDS.convert(time_period, TimeUnit.MILLISECONDS);
    }
    
    public void start() throws Exception {
        super.start();
        if(max_bytes < frag_size)
            throw new IllegalStateException("max_bytes (" + max_bytes + ") need to be bigger than frag_size (" + frag_size + ")");
        running=true;
    }

    public void stop() {
        running=false;
        super.stop();
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.CONFIG) {
            Map<String,Object> map=evt.getArg();
            Integer tmp=map != null? (Integer)map.get("frag_size") : null;
            if(tmp != null)
                frag_size=tmp;
            if(frag_size > 0 && max_bytes % frag_size != 0)
                log.warn("For optimal performance, max_bytes (%d) should be a multiple of frag_size (%d)", max_bytes, frag_size);
        }
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        int len=msg.getLength();
        if(len == 0 || msg.isFlagSet(Message.Flag.NO_FC))
            return down_prot.down(msg);

        lock.lock();
        try {
            if(len > max_bytes) {
                log.error(Util.getMessage("MessageLength") + len + " bytes) exceeded max_bytes (" + max_bytes + "); " +
                            "adjusting max_bytes to " + len);
                max_bytes=len;
            }

            if(num_bytes_sent_in_period + len > max_bytes) { // size exceeded
                long current_time=System.nanoTime();
                long block_time=time_period_ns - (current_time - current_period_start);
                if(block_time > 0) {
                    LockSupport.parkNanos(block_time);
                    num_blockings++;
                    total_block_time+=block_time;
                }
                current_period_start=block_time > 0? current_time + block_time : System.nanoTime();
                num_bytes_sent_in_period=0;
            }
        }
        finally {
            num_bytes_sent_in_period+=len;
            lock.unlock();
        }

        return down_prot.down(msg);
    }
}

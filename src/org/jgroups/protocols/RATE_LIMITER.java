package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Protocol which sends at most max_bytes in time_period milliseconds. Can be used instead of a flow control protocol,
 * e.g. FC or SFC (same position in the stack)
 * @author Bela Ban
 */
@Experimental
@MBean(description="Limits the sending rate to max_bytes per time_period")
public class RATE_LIMITER extends Protocol {

    @Property(description="Max number of bytes to be sent in time_period ms. Blocks the sender if exceeded until a new " +
            "time period has started")
    protected long max_bytes=500000;

    @Property(description="Number of milliseconds during which max_bytes bytes can be sent")
    protected long time_period=1000L;


    /** Keeps track of the number of bytes sent in the current time period */
    @GuardedBy("lock")
    @ManagedAttribute(description="Number of bytes sent in the current time period. Reset after every time period.")
    protected long num_bytes_sent=0L;

    @GuardedBy("lock")
    protected long end_of_current_period=0L; // ns

    protected final Lock lock=new ReentrantLock();

    @ManagedAttribute
    protected int num_blockings=0;

    protected long total_block_time=0L; // ns

    protected volatile boolean running=true;


    @ManagedAttribute(description="Total block time in milliseconds")
    public long getTotalBlockTime() {
        return TimeUnit.MILLISECONDS.convert(total_block_time, TimeUnit.NANOSECONDS);
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

    public void start() throws Exception {
        super.start();
        running=true;
    }

    public void stop() {
        running=false;
        super.stop();
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            int len=msg.getLength();

            lock.lock();
            try {
                if(len > max_bytes) {
                    log.error("message length (" + len + " bytes) exceeded max_bytes (" + max_bytes + "); " +
                            "adjusting max_bytes to " + len);
                    max_bytes=len;
                }

                while(running) {
                    boolean size_exceeded=num_bytes_sent + len >= max_bytes,
                            time_exceeded=System.nanoTime() >= end_of_current_period;
                    if(!size_exceeded && !time_exceeded)
                        break;

                    if(time_exceeded) {
                        num_bytes_sent=0L;
                        end_of_current_period=System.nanoTime() + TimeUnit.NANOSECONDS.convert(time_period, TimeUnit.MILLISECONDS);
                    }
                    else { // size exceeded
                        long block_time=end_of_current_period - System.nanoTime();
                        if(block_time > 0) {
                            LockSupport.parkNanos(block_time);
                            num_blockings++;
                            total_block_time+=block_time;
                        }
                    }
                }
            }
            finally {
                num_bytes_sent+=len;
                lock.unlock();
            }

            return down_prot.down(evt);
        }

        return down_prot.down(evt);
    }
    

    public void init() throws Exception {
        super.init();
        if(time_period <= 0)
            throw new IllegalArgumentException("time_period needs to be positive");
    }


}

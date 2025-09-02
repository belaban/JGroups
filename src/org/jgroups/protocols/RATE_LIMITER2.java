package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.TimeScheduler;

import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Rate limiter based credits (max_bytes). Senders send until max_bytes is exceeded; a replenisher task replenishes
 * periodically.
 * <p>
 * See https://issues.redhat.com/browse/JGRP-2779 for details
 * @author Bela Ban
 * @since  5.3.5
 */
public class RATE_LIMITER2 extends Protocol {

    @Property(description="Max number of bytes that can be sent in the given interval",type=AttributeType.BYTES)
    protected int                 max_bytes=20_000_000;

    @Property(description="Interval (ms) at which the bucket is replenished",type=AttributeType.TIME)
    protected long                interval=1000;

    @Property(description="If true, messages with a DONT_BLOCK flag will be dropped instead of blocking when " +
      "no bytes are left")
    protected boolean             drop_dont_block_msgs;

    @ManagedAttribute(type=AttributeType.BYTES)
    protected long                bytes_left=max_bytes;

    @ManagedAttribute(description="Number of replenishments")
    protected int                 num_replenishments;

    @ManagedAttribute(description="Number of messages dropped instead of blocking because of the DONT_BLOCK flag")
    protected final LongAdder     num_dropped_msgs=new LongAdder();

    @ManagedAttribute(description="Number of times a thread was blocked trying to send a message")
    protected final LongAdder     num_blockings=new LongAdder();

    @ManagedAttribute(description="Average blocking time",unit=TimeUnit.NANOSECONDS)
    protected final AverageMinMax avg_block_time=new AverageMinMax().unit(TimeUnit.NANOSECONDS);

    // The lock is fair so that requests are more or less processed in arrival order
    protected Lock                lock=new ReentrantLock(true);
    protected Condition           cond=lock.newCondition();
    protected boolean             running=true;
    protected TimeScheduler       timer;
    protected Future<?>           f;
    protected final Runnable      task=this::replenish;

    public int           maxBytes()                       {return max_bytes;}
    public RATE_LIMITER2 maxBytes(int m)                  {max_bytes=m; return this;}
    public long          interval()                       {return interval;}
    public RATE_LIMITER2 interval(long i)                 {interval=i; return this;}
    public boolean       dropDontBlockMessages()          {return drop_dont_block_msgs;}
    public RATE_LIMITER2 dropDontBlockMessages(boolean p) {drop_dont_block_msgs=p; return this;}

    @Override
    public void init() throws Exception {
        super.init();
        bytes_left=max_bytes;
        timer=Objects.requireNonNull(getTransport().getTimer());
    }

    @Override
    public void start() throws Exception {
        super.start();
        running=true;
        f=timer.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS, false);
    }

    @Override
    public void stop() {
        super.stop();
        running=false;
        f.cancel(false);
        lock.lock();
        try {
            bytes_left=max_bytes;
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void resetStats() {
        super.resetStats();
        num_replenishments=0;
        num_blockings.reset();
        num_dropped_msgs.reset();
        avg_block_time.clear();
    }

    @ManagedOperation(description="Replenishes the bytes")
    public void replenish() {
        lock.lock();
        try {
            bytes_left=max_bytes;
            num_replenishments++;
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    @ManagedOperation(description="Change the interval and restart the replenisher task")
    public void changeInterval(long new_interval) {
        if(new_interval <= 0)
            throw new IllegalArgumentException("interval cannot be <= 0");
        this.interval=new_interval;
        if(f != null)
            f.cancel(false);
        if(timer != null)
            f=timer.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS, false);
    }

    @Override
    public Object down(Message msg) {
        int len=msg.getLength();
        if(len == 0 || msg.isFlagSet(Message.Flag.NO_FC))
            return down_prot.down(msg);

        lock.lock();
        try {
            if(bytes_left - len >= 0) {
                bytes_left-=len;
                return down_prot.down(msg);
            }
            // not enough bytes left to send this message
            if(msg.isFlagSet(Message.TransientFlag.DONT_BLOCK) && drop_dont_block_msgs) {
                num_dropped_msgs.increment();
                return null;
            }

            // block until more bytes are available
            while(running && bytes_left - len < 0) {
                try {
                    long start=System.nanoTime();
                    cond.await();
                    long time=System.nanoTime() - start;
                    avg_block_time.add(time);
                    num_blockings.increment();
                }
                catch(InterruptedException e) {
                    ;
                }
            }
        }
        finally {
            lock.unlock();
        }
        return down_prot.down(msg);
    }
}

package org.jgroups.util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a coarse grained time service. Registers a timer task which calls and caches {@link System#nanoTime()}
 * and returns the cached value. This is way faster than calling {@link System#nanoTime()} many times, e.g.
 * for each received message. The granularity (interval) can be chosen by the user.<p/>
 * Note that use of values returned by {@link #timestamp()} needs to obey the same rules as for {@link System#nanoTime()}
 * @author Bela Ban
 * @since  3.5
 */
public class TimeService  implements Runnable {
    protected TimeScheduler  timer;
    protected Future<?>      task;
    protected long           interval=500;                // ms
    protected volatile long  timestamp=System.nanoTime(); // ns
    protected final Lock     lock=new ReentrantLock();


    public TimeService(final TimeScheduler timer) {
        this(timer, 500);
    }

    public TimeService(final TimeScheduler timer, long interval) {
        this.timer=timer;
        this.interval=interval;
        if(timer == null)
            throw new IllegalArgumentException("timer must not be null");
    }

    /**
     * Returns the timestamp (ns). Because timestamp is volatile, the read will return the result of the most recent write
     * @return the result of the last call to {@link System#nanoTime()} (ns)
     */
    public long timestamp() {return timestamp;}

    public long interval() {
        return interval;
    }

    public TimeService interval(long interval) {
        if(interval != this.interval)
            this.interval=interval;
        return this;
    }

    public boolean running() {
        lock.lock();
        try {
            return task != null && !task.isDone();
        }
        finally {
            lock.unlock();
        }
    }

    public TimeService start() {
        lock.lock();
        try {
            if(task == null || task.isDone())
                task=timer.scheduleWithFixedDelay(this, interval, interval, TimeUnit.MILLISECONDS, false);
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public TimeService stop() {
        lock.lock();
        try {
            if(task != null) {
                task.cancel(false);
                task=null;
            }
            return this;
        }
        finally {
            lock.unlock();
        }

    }


    public void run() {
        timestamp=System.nanoTime(); // JLS 17.7: the write to the volatile var makes the change visible to the next read
    }

    public String toString() {
        return getClass().getSimpleName() + " (interval=" + interval + "ms)";
    }

}

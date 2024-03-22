package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Lifecycle;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Thread pool based on {@link java.util.concurrent.ThreadPoolExecutor}
 * @author Bela Ban
 * @since  5.2
 */
public class ThreadPool implements Lifecycle {
    protected Executor            thread_pool;
    protected Log                 log;
    protected ThreadFactory       thread_factory;
    protected Address             address;

    // Incremented when a message is rejected due to a full thread pool. When this value exceeds thread_dumps_threshold,
    // the threads will be dumped at FATAL level, and thread_dumps will be reset to 0
    protected final AtomicInteger thread_dumps=new AtomicInteger();

    @Property(description="Whether or not the thread pool is enabled. If false, tasks will be run on the caller's thread")
    protected boolean             enabled=true;

    @Property(description="If true, create virtual threads, otherwise create native threads")
    protected boolean             use_virtual_threads;

    @Property(description="Minimum thread pool size for the thread pool")
    protected int                 min_threads;

    @Property(description="Maximum thread pool size for the thread pool")
    protected int                 max_threads=200;

    @Property(description="Timeout (ms) to remove idle threads from the pool", type=AttributeType.TIME)
    protected long                keep_alive_time=30000;

    @Property(description="The rejection policy to be used in the thread pool (abort, discard, run, custom etc. " +
      "See Util.parseRejectionPolicy() for details")
    protected String              rejection_policy="abort";

    @Property(description="The number of times a thread pool needs to be full before a thread dump is logged")
    protected int                 thread_dumps_threshold=1;

    @Property(description="Path to which the thread dump will be written. Ignored if null",
      systemProperty="jgroups.threaddump.path")
    protected String              thread_dump_path;

    @Property(description="Increases max_threads by the view size + delta if enabled " +
      "(https://issues.redhat.com/browse/JGRP-2655)")
    protected boolean             increase_max_size_dynamically=true;

    @Property(description="Added to the view size when the pool is increased dynamically")
    protected int                 delta=10;

    @ManagedAttribute(description="The number of messages dropped because the thread pool was full",type= SCALAR)
    protected final LongAdder     num_rejected_msgs=new LongAdder();

    public ThreadPool() {
    }

    public boolean isEnabled() {return enabled;}

    public Executor getThreadPool() {
        return thread_pool;
    }

    public ThreadPool setThreadPool(Executor thread_pool) {
        if(this.thread_pool != null)
            destroy();
        this.thread_pool=thread_pool;
        return this;
    }

    public ThreadPool setThreadFactory(ThreadFactory factory) {
        this.thread_factory=factory;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setThreadFactory(factory);
        return this;
    }

    public ThreadFactory getThreadFactory() {return thread_factory;}

    public boolean isShutdown() {
        return thread_pool instanceof ExecutorService && ((ExecutorService)thread_pool).isShutdown();
    }

    public int getMinThreads() {return min_threads;}

    public ThreadPool setMinThreads(int size) {
        min_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setCorePoolSize(size);
        return this;
    }

    public int getMaxThreads() {return max_threads;}

    public ThreadPool setMaxThreads(int size) {
        max_threads=size;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setMaximumPoolSize(size);
        return this;
    }

    public long getKeepAliveTime() {return keep_alive_time;}

    public ThreadPool setKeepAliveTime(long time) {
        keep_alive_time=time;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setKeepAliveTime(time, TimeUnit.MILLISECONDS);
        return this;
    }

    public ThreadPool setRejectionPolicy(String policy) {
        RejectedExecutionHandler p=Util.parseRejectionPolicy(policy);
        this.rejection_policy=policy;
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setRejectedExecutionHandler(p);
        return this;
    }

    public RejectedExecutionHandler getRejectedExecutionHandler() {
        Executor t=thread_pool;
        return t instanceof ThreadPoolExecutor? ((ThreadPoolExecutor)t).getRejectedExecutionHandler() : null;
    }

    public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setRejectedExecutionHandler(handler);
    }

    public int getThreadDumpsThreshold() {
        return thread_dumps_threshold;
    }

    public ThreadPool setThreadDumpsThreshold(int t) {
        this.thread_dumps_threshold=t;
        return this;
    }

    public Address    getAddress()                             {return address;}
    public ThreadPool setAddress(Address a)                    {this.address=a; return this;}
    public boolean    getIncreaseMaxSizeDynamically()          {return increase_max_size_dynamically;}
    public ThreadPool setIncreaseMaxSizeDynamically(boolean b) {increase_max_size_dynamically=b; return this;}
    public int        getDelta()                               {return delta;}
    public ThreadPool setDelta(int d)                          {delta=d; return this;}
    public long       numberOfRejectedMessages()               {return num_rejected_msgs.sum();}
    public ThreadPool log(Log l)                               {log=l; return this;}
    public boolean    useVirtualThreads()                      {return use_virtual_threads;}
    public ThreadPool useVirtualThreads(boolean b)             {use_virtual_threads=b; return this;}

    @ManagedAttribute(description="Number of thread dumps")
    public int getNumberOfThreadDumps() {return thread_dumps.get();}

    @ManagedOperation(description="Resets the thread_dumps counter")
    public void resetThreadDumps() {thread_dumps.set(0);}

    @ManagedAttribute(description="Current number of threads in the thread pool")
    public int getThreadPoolSize() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getPoolSize();
        return 0;
    }


    @ManagedAttribute(description="Current number of active threads in the thread pool")
    public int getThreadPoolSizeActive() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getActiveCount();
        return 0;
    }

    @ManagedAttribute(description="Largest number of threads in the thread pool")
    public int getLargestSize() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getLargestPoolSize();
        return 0;
    }

    public void resetStats() {
        num_rejected_msgs.reset();
    }


    @Override
    public void init() throws Exception {
        if(log == null)
            log=LogFactory.getLog(getClass());
        if(enabled) {
            if(thread_factory == null)
                thread_factory=new DefaultThreadFactory("thread-pool", true, true);
            thread_pool=ThreadCreator.createThreadPool(min_threads, max_threads, keep_alive_time,
                                  rejection_policy, new SynchronousQueue<>(), thread_factory, use_virtual_threads, log);
        }
        else // otherwise use the caller's thread to unmarshal the byte buffer into a message
            thread_pool=new DirectExecutor();
    }

    @Override
    public void destroy() {
        if(thread_pool instanceof ExecutorService) {
            ExecutorService service=(ExecutorService)thread_pool;
            service.shutdownNow();
            try {
                service.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException ignored) {
            }
        }
    }

    public void doExecute(Runnable task) {
        thread_pool.execute(task);
    }

    public Executor pool() {return thread_pool;}

    public boolean execute(Runnable task) {
        try {
            thread_pool.execute(task);
            return true;
        }
        catch(RejectedExecutionException ex) {
            num_rejected_msgs.increment();
            // https://issues.redhat.com/browse/JGRP-2403
            if(thread_dumps.incrementAndGet() == thread_dumps_threshold) {
                String thread_dump=Util.dumpThreads();
                if(thread_dump_path != null) {
                    File file=new File(thread_dump_path, "jgroups_threaddump_" + System.currentTimeMillis() + ".txt");
                    try(BufferedWriter writer=new BufferedWriter(new FileWriter(file))) {
                        writer.write(thread_dump);
                        log.fatal("%s: thread pool is full (max=%d, active=%d); thread dump (dumped once, until thread_dump is reset): %s",
                                  address, max_threads, getThreadPoolSize(), file.getAbsolutePath());
                    }
                    catch(IOException e) {
                        log.warn("%s: cannot generate the thread dump to %s: %s", address, file.getAbsolutePath(), e);
                        log.fatal("%s: thread pool is full (max=%d, active=%d); " +
                                    "thread dump (dumped once, until thread_dump is reset):\n%s",
                                  address, max_threads, getThreadPoolSize(), thread_dump);
                    }
                }
                else
                    log.fatal("%s: thread pool is full (max=%d, active=%d); thread dump (dumped once, until thread_dump is reset):\n%s",
                              address, max_threads, getThreadPoolSize(), thread_dump);
            }
            return false;
        }
        catch(Throwable t) {
            log.error("failure submitting task to thread pool", t);
            num_rejected_msgs.increment();
            return false;
        }
    }

    public String toString() {
        return thread_pool != null? thread_pool.toString() : "n/a";
    }

    protected static ExecutorService createThreadPool(int min_threads, int max_threads, long keep_alive_time,
                                                      String rejection_policy,
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory) {
        ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time, TimeUnit.MILLISECONDS,
                                                       queue, factory);
        RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
        pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
        return pool;
    }

}

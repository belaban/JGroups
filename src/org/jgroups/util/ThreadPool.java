package org.jgroups.util;

import org.jgroups.Global;
import org.jgroups.Lifecycle;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.protocols.TP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread pool based on {@link java.util.concurrent.ThreadPoolExecutor}
 * @author Bela Ban
 * @since  5.2
 */
public class ThreadPool implements Lifecycle {
    protected Executor            thread_pool;
    protected final TP            tp;

    // Incremented when a message is rejected due to a full thread pool. When this value exceeds thread_dumps_threshold,
    // the threads will be dumped at FATAL level, and thread_dumps will be reset to 0
    protected final AtomicInteger thread_dumps=new AtomicInteger();

    @Property(description="Whether or not the thread pool is enabled. If false, tasks will be run on the caller's thread")
    protected boolean             enabled=true;

    @Property(description="Minimum thread pool size for the thread pool")
    protected int                 min_threads;

    @Property(description="Maximum thread pool size for the thread pool")
    protected int                 max_threads=100;

    @Property(description="Timeout (ms) to remove idle threads from the pool", type=AttributeType.TIME)
    protected long                keep_alive_time=30000;

    @Property(description="The number of times a thread pool needs to be full before a thread dump is logged")
    protected int                 thread_dumps_threshold=1;

    @Property(description="Path to which the thread dump will be written. Ignored if null",
      systemProperty="jgroups.threaddump.path")
    protected String              thread_dump_path;




    public ThreadPool(TP tp) {
        this.tp=Objects.requireNonNull(tp);
    }

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
        if(thread_pool instanceof ThreadPoolExecutor)
            ((ThreadPoolExecutor)thread_pool).setThreadFactory(factory);
        return this;
    }

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


    public int getThreadDumpsThreshold() {
        return thread_dumps_threshold;
    }

    public ThreadPool setThreadDumpsThreshold(int t) {
        this.thread_dumps_threshold=t;
        return this;
    }

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


    @Override
    public void init() throws Exception {
        if(enabled) {
            if(tp.useVirtualThreads())
                thread_pool=Util.createFiberThreadPool(); // Executors.newVirtualThreadExecutor();
            else {
                tp.getLog().debug("thread pool min/max/keep-alive (ms): %d/%d/%d", min_threads, max_threads, keep_alive_time);
                thread_pool=createThreadPool(min_threads, max_threads, keep_alive_time,
                                             "abort", new SynchronousQueue<>(), tp.getThreadFactory());
            }
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


    public boolean execute(Runnable task) {
        try {
            thread_pool.execute(task);
            return true;
        }
        catch(RejectedExecutionException ex) {
            tp.getMessageStats().incrNumRejectedMsgs(1);
            // https://issues.redhat.com/browse/JGRP-2403
            if(thread_dumps.incrementAndGet() == thread_dumps_threshold) {
                String thread_dump=Util.dumpThreads();
                Log l=tp.getLog();
                if(thread_dump_path != null) {
                    File file=new File(thread_dump_path, "jgroups_threaddump_" + System.currentTimeMillis() + ".txt");
                    try(BufferedWriter writer=new BufferedWriter(new FileWriter(file))) {
                        writer.write(thread_dump);
                        l.fatal("%s: thread pool is full (max=%d, active=%d); thread dump (dumped once, until thread_dump is reset): %s",
                                tp.getAddress(), max_threads, getThreadPoolSize(), file.getAbsolutePath());
                    }
                    catch(IOException e) {
                        l.warn("%s: cannot generate the thread dump to %s: %s", tp.getAddress(), file.getAbsolutePath(), e);
                        l.fatal("%s: thread pool is full (max=%d, active=%d); " +
                                  "thread dump (dumped once, until thread_dump is reset):\n%s",
                                tp.getAddress(), max_threads, getThreadPoolSize(), thread_dump);
                    }
                }
                else
                    l.fatal("%s: thread pool is full (max=%d, active=%d); thread dump (dumped once, until thread_dump is reset):\n%s",
                            tp.getAddress(), max_threads, getThreadPoolSize(), thread_dump);
            }
            return false;
        }
        catch(Throwable t) {
            tp.getLog().error("failure submitting task to thread pool", t);
            tp.getMessageStats().incrNumRejectedMsgs(1);
            return false;
        }
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

package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Lifecycle;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static org.jgroups.conf.AttributeType.SCALAR;
import static org.jgroups.conf.AttributeType.TIME;
import static org.jgroups.util.SuppressLog.Level.warn;

/**
 * Thread pool based on {@link java.util.concurrent.ThreadPoolExecutor}
 * @author Bela Ban
 * @since  5.2
 */
public class ThreadPool implements Lifecycle {
    private static final MethodHandle EXECUTORS_NEW_THREAD_PER_TASK_EXECUTOR=getNewThreadPerTaskExecutorHandle();
    protected Executor            thread_pool;
    protected Log                 log;
    protected ThreadFactory       thread_factory;
    protected Address             address;
    protected SuppressLog<String> thread_pool_full_log;

    @Property(description="Whether or not the thread pool is enabled. If false, tasks will be run on the caller's thread")
    protected boolean             enabled=true;

    @Property(description="Minimum thread pool size for the thread pool")
    protected int                 min_threads;

    @Property(description="Maximum thread pool size for the thread pool")
    protected int                 max_threads=200;

    @Property(description="Timeout (ms) to remove idle threads from the pool", type=AttributeType.TIME)
    protected long                keep_alive_time=30000;

    @Property(description="The rejection policy to be used in the thread pool (abort, discard, run, custom etc. " +
      "See Util.parseRejectionPolicy() for details")
    protected String              rejection_policy="abort";

    @Property(description="Time (in milliseconds) during which thread-pool full messages are suppressed",type=TIME)
    protected long                thread_pool_full_suppress_time=60_000;

    @Property(description="The number of times a thread pool needs to be full before a thread dump is logged",
    deprecatedMessage="ignored")
    @Deprecated(since="5.4")
    protected int                 thread_dumps_threshold=1;

    @Property(description="Path to which the thread dump will be written. Ignored if null",
      systemProperty="jgroups.threaddump.path",deprecatedMessage="ignored")
    @Deprecated(since="5.4")
    protected String              thread_dump_path;

    @Property(description="Dump threads when the thread pool is full")
    protected boolean             thread_dumps_enabled;

    @Property(description="Increases max_threads by the view size + delta if enabled " +
      "(https://issues.redhat.com/browse/JGRP-2655)")
    protected boolean             increase_max_size_dynamically=true;

    @Property(description="If the view is greater than the max thread pool size, the latter is set to " +
      "view size + delta. Only enabled if increase_max_size_dynamically is true")
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

    public long       getThreadPoolFullSuppressTime()          {return thread_pool_full_suppress_time;}
    public ThreadPool setThreadPoolFullSuppressTime(long t)    {this.thread_pool_full_suppress_time=t; return this;}
    public boolean    getThreadDumpsEnabled()                  {return thread_dumps_enabled;}
    public ThreadPool setThreadDumpsEnabled(boolean b)         {thread_dumps_enabled=b; return this;}
    public Address    getAddress()                             {return address;}
    public ThreadPool setAddress(Address a)                    {this.address=a; return this;}
    public boolean    getIncreaseMaxSizeDynamically()          {return increase_max_size_dynamically;}
    public ThreadPool setIncreaseMaxSizeDynamically(boolean b) {increase_max_size_dynamically=b; return this;}
    public int        getDelta()                               {return delta;}
    public ThreadPool setDelta(int d)                          {delta=d; return this;}
    public long       numberOfRejectedMessages()               {return num_rejected_msgs.sum();}
    public ThreadPool log(Log l)                               {log=l; return this;}

    @Deprecated public static int getThreadDumpsThreshold()           {return 0;}
    @Deprecated public ThreadPool setThreadDumpsThreshold(int ignore) {return this;}
    @Deprecated public static int getNumberOfThreadDumps()            {return -1;}
    @Deprecated public void       resetThreadDumps()                  {}

    @ManagedAttribute(description="Current number of threads in the thread pool",type=SCALAR)
    public int getThreadPoolSize() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getPoolSize();
        return 0;
    }

    @ManagedAttribute(description="Current number of active threads in the thread pool",type=SCALAR)
    public int getThreadPoolSizeActive() {
        if(thread_pool instanceof ThreadPoolExecutor)
            return ((ThreadPoolExecutor)thread_pool).getActiveCount();
        return 0;
    }

    @ManagedAttribute(description="Largest number of threads in the thread pool",type=SCALAR)
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
        thread_pool_full_log=new SuppressLog<>(log, "ThreadPoolFull");
        if(enabled) {
            if(thread_factory == null)
                thread_factory=new DefaultThreadFactory("thread-pool", true, true);
            thread_pool=createThreadPool(min_threads, max_threads, keep_alive_time,
                                         rejection_policy, new SynchronousQueue<>(), thread_factory, log);
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

    public ThreadPool removeExpired() {
        thread_pool_full_log.removeExpired(thread_pool_full_suppress_time);
        return this;
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
            //https://issues.redhat.com/browse/JGRP-2802
            String thread_dump=thread_dumps_enabled? String.format(". Threads:\n%s", Util.dumpThreads()) : "";
            thread_pool_full_log.log(warn, "thread-pool-full", thread_pool_full_suppress_time,
                                     address, max_threads, getThreadPoolSize(), thread_dump);
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
                                                      BlockingQueue<Runnable> queue, final ThreadFactory factory,
                                                      Log log) {
        if(!factory.useVirtualThreads() || EXECUTORS_NEW_THREAD_PER_TASK_EXECUTOR == null) {
            ThreadPoolExecutor pool=new ThreadPoolExecutor(min_threads, max_threads, keep_alive_time,
                                                           TimeUnit.MILLISECONDS, queue, factory);
            RejectedExecutionHandler handler=Util.parseRejectionPolicy(rejection_policy);
            pool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(handler));
            if(log != null)
                log.debug("thread pool min/max/keep-alive (ms): %d/%d/%d", min_threads, max_threads, keep_alive_time);
            return pool;
        }

        try {
            return (ExecutorService)EXECUTORS_NEW_THREAD_PER_TASK_EXECUTOR.invokeExact((java.util.concurrent.ThreadFactory)factory);
        }
        catch(Throwable t) {
            throw new IllegalStateException(String.format("failed to create virtual thread pool: %s", t));
        }
    }

    protected static MethodHandle getNewThreadPerTaskExecutorHandle() {
        MethodType type=MethodType.methodType(ExecutorService.class, java.util.concurrent.ThreadFactory.class);
        String[] names={
          "newThreadPerTaskExecutor", // jdk 17+
          "newUnboundedExecutor"      // jdk 15 & 16
        };

        MethodHandles.Lookup LOOKUP=MethodHandles.publicLookup();
        for(int i=0; i < names.length; i++) {
            try {
                return LOOKUP.findStatic(Executors.class, names[i], type);
            }
            catch(Exception e) {
            }
        }
        return null;
    }

}

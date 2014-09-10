
package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.List;
import java.util.concurrent.*;


/**
 * Implementation of {@link TimeScheduler}. Based on the {@link TimeScheduler2} implementation
 * with various fixes and enhancements. Uses a {@link DelayQueue} to order tasks according to execution times
 * @author Bela Ban
 * @since  3.3
 */
public class TimeScheduler3 implements TimeScheduler, Runnable {
    /** Thread pool used to execute the tasks */
    protected final ThreadPoolExecutor                pool;

    /** DelayQueue with tasks being sorted according to execution times (next execution first) */
    protected final BlockingQueue<Task>               queue=new DelayQueue<Task>();

    /** Thread which removes tasks ready to be executed from the queue and submits them to the pool for execution */
    protected volatile Thread                         runner;

    protected static final Log                        log=LogFactory.getLog(TimeScheduler3.class);

    protected ThreadFactory                           timer_thread_factory=null;

    protected static enum TaskType                    {dynamic, fixed_rate, fixed_delay}


    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler3() {
        pool=new ThreadPoolExecutor(4, 10,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        start();
    }


    public TimeScheduler3(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time, int max_queue_size,
                          String rejection_policy) {
        timer_thread_factory=factory;
        pool=new ThreadPoolExecutor(min_threads, max_threads,keep_alive_time, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(max_queue_size),
                                    factory, Util.parseRejectionPolicy(rejection_policy));
        start();
    }



    public void    setThreadFactory(ThreadFactory f) {pool.setThreadFactory(f);}
    public int     getMinThreads()                   {return pool.getCorePoolSize();}
    public void    setMinThreads(int size)           {pool.setCorePoolSize(size);}
    public int     getMaxThreads()                   {return pool.getMaximumPoolSize();}
    public void    setMaxThreads(int size)           {pool.setMaximumPoolSize(size);}
    public long    getKeepAliveTime()                {return pool.getKeepAliveTime(TimeUnit.MILLISECONDS);}
    public void    setKeepAliveTime(long time)       {pool.setKeepAliveTime(time, TimeUnit.MILLISECONDS);}
    public int     getCurrentThreads()               {return pool.getPoolSize();}
    public int     getQueueSize()                    {return pool.getQueue().size();}
    public int     size()                            {return queue.size();}
    public String  toString()                        {return getClass().getSimpleName();}
    public boolean isShutdown()                      {return pool.isShutdown();}


    public String dumpTimerTasks() {
        StringBuilder sb=new StringBuilder();
        for(Task task: queue) {
            sb.append(task);
            if(task.isCancelled())
                sb.append(" (cancelled)");
            sb.append("\n");
        }
        return sb.toString();
    }



    public void execute(Runnable task) {
        submitToPool(task instanceof TimeScheduler.Task?
                       new RecurringTask(task, TaskType.dynamic, 0, ((TimeScheduler.Task)task).nextInterval(), TimeUnit.MILLISECONDS)
                       : new Task(task)); // we'll execute the task directly
    }


    public Future<?> schedule(Runnable work, long initial_delay, TimeUnit unit) {
        return doSchedule(new Task(work, initial_delay, unit), initial_delay);
    }



    public Future<?> scheduleWithFixedDelay(Runnable work, long initial_delay, long delay, TimeUnit unit) {
        return scheduleRecurring(work, TaskType.fixed_delay, initial_delay, delay, unit);
    }


    public Future<?> scheduleAtFixedRate(Runnable work, long initial_delay, long delay, TimeUnit unit) {
        return scheduleRecurring(work,TaskType.fixed_rate,initial_delay,delay,unit);
    }


    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler.Task#nextInterval()} milliseconds. The task is never done until
     * nextInterval() returns a value <= 0 or the task is cancelled.<p/>
     * Note that the task is rescheduled relative to the last time is actually executed. This is similar to
     * {@link #scheduleWithFixedDelay(Runnable,long,long,java.util.concurrent.TimeUnit)}.
     * @param work the task to execute
     */
    public Future<?> scheduleWithDynamicInterval(TimeScheduler.Task work) {
        return scheduleRecurring(work, TaskType.dynamic, work.nextInterval(), 0, TimeUnit.MILLISECONDS);
    }


    protected void start() {
        startRunner();
    }


    /**
     * Stops the timer, cancelling all tasks
     */
    public void stop() {
        stopRunner();

        // we may need to do multiple iterations as the iterator works on a copy and tasks might have been added just
        // after the iterator() call returned
        while(!queue.isEmpty())
            for(Task entry: queue) {
                entry.cancel(true);
                queue.remove(entry);
            }
        queue.clear();

        List<Runnable> remaining_tasks=pool.shutdownNow();
        for(Runnable task: remaining_tasks) {
            if(task instanceof Future) {
                Future future=(Future)task;
                future.cancel(true);
            }
        }
        pool.getQueue().clear();
        try {
            pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
        }
        catch(InterruptedException e) {
        }
    }




    public void run() {
        while(Thread.currentThread() == runner) {
            try {
                final Task entry=queue.take();
                submitToPool(entry);
            }
            catch(InterruptedException interrupted) {
                // flag is cleared and we check if the loop should be terminated at the top of the loop
            }
            catch(Throwable t) {
                log.error("failed submitting task to thread pool", t);
            }
        }
    }


    protected Future<?> scheduleRecurring(Runnable work, TaskType type, long initial_delay, long delay, TimeUnit unit) {
        return doSchedule(new RecurringTask(work, type, initial_delay, delay, unit), initial_delay);
    }


    protected Future<?> doSchedule(Task task, long initial_delay) {
        if(task.getRunnable() == null)
            throw new NullPointerException();
        if (isShutdown())
            return null;

        if(initial_delay <= 0) {
            submitToPool(task);
            return task;
        }
        return add(task);
    }




    protected void submitToPool(final Task entry) {
        try {
            pool.execute(entry);
        }
        catch(RejectedExecutionException rejected) { // only thrown if rejection policy is "abort"
            Thread thread=timer_thread_factory != null?
              timer_thread_factory.newThread(entry, "Timer temp thread")
              : new Thread(entry, "Timer temp thread");
            thread.start();
        }
    }

    protected Task add(Task task) {
        if(!isRunning())
            return null;
        queue.add(task);
        return task;
    }

    protected boolean isRunning() {
        Thread tmp=runner;
        return tmp != null && tmp.isAlive();
    }

    protected synchronized void startRunner() {
        stopRunner();
        runner=timer_thread_factory != null? timer_thread_factory.newThread(this, "Timer runner") : new Thread(this, "Timer runner");
        runner.start();
    }

    protected synchronized void stopRunner() {
        Thread tmp=runner;
        runner=null;
        if(tmp != null) {
            tmp.interrupt();
            try {tmp.join(500);} catch(InterruptedException e) {}
        }
        queue.clear();
    }


    public static class Task implements Runnable, Delayed, Future {
        protected final Runnable   runnable;      // the task to execute
        protected long             creation_time; // time (in ns) at which the task was created
        protected long             delay;         // time (in ns) after which the task should execute
        protected volatile boolean cancelled;
        protected volatile boolean done;

        public Task(Runnable runnable) {
            this.runnable=runnable;
        }

        public Task(Runnable runnable, long initial_delay, TimeUnit unit) {
            this.creation_time=System.nanoTime();
            this.delay=TimeUnit.NANOSECONDS.convert(initial_delay, unit);
            this.runnable=runnable;
            if(runnable == null)
                throw new IllegalArgumentException("runnable cannot be null");
        }

        public Runnable getRunnable() {return runnable;}

        public int compareTo(Delayed o) {
            long my_delay=getDelay(TimeUnit.NANOSECONDS), other_delay=o.getDelay(TimeUnit.NANOSECONDS);
            return Long.compare(my_delay, other_delay);
        }

        public long getDelay(TimeUnit unit) {
            // time (in ns) until execution, can be negative when already elapsed
            long remaining_time=delay - (System.nanoTime() - creation_time);
            return unit.convert(remaining_time, TimeUnit.NANOSECONDS);
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean retval=!isDone();
            cancelled=true;
            return retval;
        }

        public boolean isCancelled() {return cancelled;}
        public boolean isDone()      {return done || cancelled;}
        public Object  get() throws InterruptedException, ExecutionException {return null;}
        public Object  get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        public void run() {
            if(isDone())
                return;
            try {
                runnable.run();
            }
            catch(Throwable t) {
                log.error("failed executing task " + runnable, t);
            }
            finally {
                done=true;
            }
        }

        public String toString() {
            return runnable.toString();
        }
    }

    /** Tasks which runs more than once, either dynamic, fixed-rate or fixed-delay, until cancelled */
    protected class RecurringTask extends Task {
        protected final TaskType type;
        protected final long     period; // ns
        protected final long     initial_delay; // ns
        protected int            cnt=1; // number of invocations (for fixed rate invocations)

        public RecurringTask(Runnable runnable, TaskType type, long initial_delay, long delay, TimeUnit unit) {
            super(runnable, initial_delay, unit);
            this.initial_delay=TimeUnit.NANOSECONDS.convert(initial_delay, TimeUnit.MILLISECONDS);
            this.type=type;
            period=TimeUnit.NANOSECONDS.convert(delay, unit);
            if(type == TaskType.dynamic && !(runnable instanceof TimeScheduler.Task))
                throw new IllegalArgumentException("Need to provide a TimeScheduler.Task as runnable when type is dynamic");
        }

        public void run() {
            if(isDone())
                return;
            super.run();
            if(cancelled)
                return;
            done=false; // run again

            switch(type) {
                case dynamic:
                    long next_interval=TimeUnit.NANOSECONDS.convert(((TimeScheduler.Task)runnable).nextInterval(), TimeUnit.MILLISECONDS);
                    if(next_interval <= 0) {
                        if(log.isTraceEnabled())
                            log.trace("task will not get rescheduled as interval is " + next_interval);
                        done=true;
                        return;
                    }
                    creation_time=System.nanoTime();
                    delay=next_interval;
                    break;
                case fixed_rate:
                    delay=initial_delay + cnt++ * period;
                    break;
                case fixed_delay:
                    creation_time=System.nanoTime();
                    delay=period;
                    break;
            }
            add(this); // schedule this task again
        }
    }



}

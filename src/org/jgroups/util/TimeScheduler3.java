
package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * Implementation of {@link TimeScheduler}. Uses a {@link DelayQueue} to order tasks according to execution times
 * @author Bela Ban
 * @since  3.3
 */
public class TimeScheduler3 implements TimeScheduler, Runnable {
    /** Thread pool used to execute the tasks */
    protected Executor                    pool;

    /** DelayQueue with tasks being sorted according to execution times (next execution first) */
    protected final BlockingQueue<Task>   queue=new DelayQueue<>();

    /** Thread which removes tasks ready to be executed from the queue and submits them to the pool for execution */
    protected volatile Thread             runner;

    protected static final Log            log=LogFactory.getLog(TimeScheduler3.class);

    protected ThreadFactory               timer_thread_factory;

    // if true, non-blocking timer tasks are run directly by the runner thread and not submitted to the thread pool
    protected boolean                     non_blocking_task_handling=true;

    protected enum TaskType               {dynamic, fixed_rate, fixed_delay}


    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler3() {
        pool=new ThreadPoolExecutor(4, 10,
                                    30000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        start();
    }


    public TimeScheduler3(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time, int max_queue_size,
                          String rejection_policy) {
        this(factory, min_threads, max_threads, keep_alive_time, new ArrayBlockingQueue<>(max_queue_size), rejection_policy, true);
    }

    public TimeScheduler3(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time,
                          BlockingQueue<Runnable> queue, String rejection_policy, boolean thread_pool_enabled) {
        timer_thread_factory=factory;
        pool=thread_pool_enabled?
          new ThreadPoolExecutor(min_threads, max_threads,keep_alive_time, TimeUnit.MILLISECONDS,
                                 queue, factory, Util.parseRejectionPolicy(rejection_policy))
          : new DirectExecutor();
        start();
    }

    public TimeScheduler3(Executor thread_pool, ThreadFactory factory) {
        timer_thread_factory=factory;
        pool=thread_pool;
        start();
    }

    public void    setThreadFactory(ThreadFactory f)     {condSet((p) -> p.setThreadFactory(f));}
    public void    setThreadPool(Executor new_pool)      {pool=new_pool;}
    public int     getMinThreads()                       {return condGet(ThreadPoolExecutor::getCorePoolSize, 0);}
    public void    setMinThreads(int size)               {condSet(p -> p.setCorePoolSize(size));}
    public int     getMaxThreads()                       {return condGet(ThreadPoolExecutor::getMaximumPoolSize, 0);}
    public void    setMaxThreads(int size)               {condSet(p -> p.setMaximumPoolSize(size));}
    public long    getKeepAliveTime()                    {return condGet(p -> p.getKeepAliveTime(TimeUnit.MILLISECONDS), 0L);}
    public void    setKeepAliveTime(long time)           {condSet(p -> p.setKeepAliveTime(time, TimeUnit.MILLISECONDS));}
    public int     getCurrentThreads()                   {return condGet(ThreadPoolExecutor::getPoolSize, 0);}
    public int     getQueueSize()                        {return condGet(p -> p.getQueue().size(), 0);}
    public int     size()                                {return queue.size();}
    public String  toString()                            {return getClass().getSimpleName();}
    public boolean isShutdown()                          {return condGet(ThreadPoolExecutor::isShutdown, false);}
    public boolean getNonBlockingTaskHandling()          {return non_blocking_task_handling;}
    public void    setNonBlockingTaskHandling(boolean b) {this.non_blocking_task_handling=b;}


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



    public void execute(Runnable task, boolean can_block) {
        submitToPool(task instanceof TimeScheduler.Task?
                       new RecurringTask(task, TaskType.dynamic, 0, ((TimeScheduler.Task)task).nextInterval(), TimeUnit.MILLISECONDS, can_block)
                       : new Task(task, can_block)); // we'll execute the task directly
    }


    public Future<?> schedule(Runnable work, long initial_delay, TimeUnit unit, boolean can_block) {
        return doSchedule(new Task(work, initial_delay, unit, can_block), initial_delay);
    }



    public Future<?> scheduleWithFixedDelay(Runnable work, long initial_delay, long delay, TimeUnit unit, boolean can_block) {
        return scheduleRecurring(work, TaskType.fixed_delay, initial_delay, delay, unit, can_block);
    }


    public Future<?> scheduleAtFixedRate(Runnable work, long initial_delay, long delay, TimeUnit unit, boolean can_block) {
        return scheduleRecurring(work,TaskType.fixed_rate,initial_delay,delay,unit, can_block);
    }


    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler.Task#nextInterval()} milliseconds. The task is never done until
     * nextInterval() returns a value <= 0 or the task is cancelled.<p/>
     * Note that the task is rescheduled relative to the last time is actually executed. This is similar to
     * {@link #scheduleWithFixedDelay(Runnable,long,long,java.util.concurrent.TimeUnit)}.
     * @param work the task to execute
     */
    public Future<?> scheduleWithDynamicInterval(TimeScheduler.Task work, boolean can_block) {
        return scheduleRecurring(work, TaskType.dynamic, work.nextInterval(), 0, TimeUnit.MILLISECONDS, can_block);
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

        if(pool instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor p=(ThreadPoolExecutor)pool;
            List<Runnable> remaining_tasks=p.shutdownNow();
            remaining_tasks.stream().filter(task -> task instanceof Future).forEach(task -> ((Future)task).cancel(true));
            p.getQueue().clear();
            try {
                p.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e) {
            }
        }

        // clears the threads list (https://issues.jboss.org/browse/JGRP-1971)
        if(timer_thread_factory instanceof LazyThreadFactory)
            ((LazyThreadFactory)timer_thread_factory).destroy();
    }




    public void run() {
        while(Thread.currentThread() == runner) {
            try {
                Task task=queue.take();
                submitToPool(task);
            }
            catch(InterruptedException interrupted) {
                // flag is cleared and we check if the loop should be terminated at the top of the loop
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedSubmittingTaskToThreadPool"), t);
            }
        }
    }


    protected Future<?> scheduleRecurring(Runnable work, TaskType type, long initial_delay, long delay, TimeUnit unit, boolean can_block) {
        return doSchedule(new RecurringTask(work, type, initial_delay, delay, unit, can_block), initial_delay);
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

    protected void condSet(Consumer<ThreadPoolExecutor> setter) {
        if(pool instanceof ThreadPoolExecutor)
            setter.accept((ThreadPoolExecutor)pool);
    }

    protected <T> T condGet(Function<ThreadPoolExecutor,T> getter, T default_value) {
        if(pool instanceof ThreadPoolExecutor)
            return getter.apply((ThreadPoolExecutor)pool);
        return default_value;
    }


    protected void submitToPool(Task task) {
        if(non_blocking_task_handling && !task.canBlock()) {
            task.run();
            return;
        }

        try {
            pool.execute(task);
        }
        catch(RejectedExecutionException rejected) { // only thrown if rejection policy is "abort"
            Thread thread=timer_thread_factory != null?
              timer_thread_factory.newThread(task, "Timer temp thread")
              : new Thread(task, "Timer temp thread");
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
        protected final boolean    can_block;

        public Task(Runnable runnable, boolean can_block) {
            this.runnable=runnable;
            this.can_block=can_block;
        }

        public Task(Runnable runnable, long initial_delay, TimeUnit unit, boolean can_block) {
            this.can_block=can_block;
            this.creation_time=System.nanoTime();
            this.delay=TimeUnit.NANOSECONDS.convert(initial_delay, unit);
            this.runnable=runnable;
            if(runnable == null)
                throw new IllegalArgumentException("runnable cannot be null");
        }

        public Runnable getRunnable() {return runnable;}
        public boolean  canBlock()    {return can_block;}

        public int compareTo(Delayed o) {
            long my_delay=getDelay(TimeUnit.NANOSECONDS), other_delay=o.getDelay(TimeUnit.NANOSECONDS);
            // return Long.compare(my_delay, other_delay); // JDK 7 only
            return (my_delay < other_delay) ? -1 : ((my_delay == other_delay) ? 0 : 1);
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
                log.error(Util.getMessage("FailedExecutingTask") + ' ' + runnable, t);
            }
            finally {
                done=true;
            }
        }

        public String toString() {
            return String.format("%s (can block=%b)", runnable.toString(), can_block);
        }
    }

    /** Tasks which runs more than once, either dynamic, fixed-rate or fixed-delay, until cancelled */
    protected class RecurringTask extends Task {
        protected final TaskType type;
        protected final long     period; // ns
        protected final long     initial_delay; // ns
        protected int            cnt=1; // number of invocations (for fixed rate invocations)

        public RecurringTask(Runnable runnable, TaskType type, long initial_delay, long delay, TimeUnit unit, boolean can_block) {
            super(runnable, initial_delay, unit, can_block);
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

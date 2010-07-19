
package org.jgroups.util;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Global;

import java.util.concurrent.*;


/**
 * Implementation of {@link org.jgroups.util.TimeScheduler} by extending
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} to keep tasks sorted. Tasks will get executed in order
 * of execution time (by using a {@link java.util.concurrent.DelayQueue} internally.
 * 
 * @author Bela Ban
 * @version $Id: DefaultTimeScheduler.java,v 1.1 2010/07/19 06:25:47 belaban Exp $
 */
public class DefaultTimeScheduler extends ScheduledThreadPoolExecutor implements TimeScheduler {

    /** How many core threads */
    private static int TIMER_DEFAULT_NUM_THREADS=3;


    protected static final Log log=LogFactory.getLog(DefaultTimeScheduler.class);



    static {
        String tmp;
        try {
            tmp=System.getProperty(Global.TIMER_NUM_THREADS);
            if(tmp != null)
                TIMER_DEFAULT_NUM_THREADS=Integer.parseInt(tmp);
        }
        catch(Exception e) {
            log.error("could not set number of timer threads", e);
        }
    }

    private ThreadDecorator threadDecorator=null;

    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public DefaultTimeScheduler() {
        this(TIMER_DEFAULT_NUM_THREADS);
    }

    public DefaultTimeScheduler(ThreadFactory factory) {
        this(factory, TIMER_DEFAULT_NUM_THREADS);
    }

    public DefaultTimeScheduler(ThreadFactory factory, int max_threads) {
        super(max_threads, factory);
        setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(getRejectedExecutionHandler()));
    }

    public DefaultTimeScheduler(int corePoolSize) {
        super(corePoolSize);
        setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(getRejectedExecutionHandler()));
    }

    public ThreadDecorator getThreadDecorator() {
        return threadDecorator;
    }

    public void setThreadDecorator(ThreadDecorator threadDecorator) {
        this.threadDecorator=threadDecorator;
    }

    public String dumpTaskQueue() {
        return getQueue().toString();
    }


    public int getActiveThreads() {
        return super.getActiveCount();
    }

    public int getMinThreads() {
        return super.getCorePoolSize();
    }

    public int getMaxThreads() {
        return super.getMaximumPoolSize();
    }

    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.DefaultTimeScheduler.Task#nextInterval()} milliseconds. The task is neve done until nextInterval()
     * return a value <= 0 or the task is cancelled.
     * @param task the task to execute
     * @param relative scheduling scheme: <tt>true</tt>:<br>
     * Task is rescheduled relative to the last time it <i>actually</i> started execution<p/>
     * <tt>false</tt>:<br> Task is scheduled relative to its <i>last</i> execution schedule. This has the effect
     * that the time between two consecutive executions of the task remains the same.<p/>
     * Note that relative is always true; we always schedule the next execution relative to the last *actual*
     * (not scheduled) execution
     */
    public ScheduledFuture<?> scheduleWithDynamicInterval(Task task) {
        if(task == null)
            throw new NullPointerException();

        if (isShutdown())
            return null;

        TaskWrapper task_wrapper=new TaskWrapper(task);
        task_wrapper.doSchedule(); // calls schedule() in ScheduledThreadPoolExecutor
        return task_wrapper;
    }


    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return super.scheduleWithFixedDelay(new RobustRunnable(command), initialDelay, delay, unit);
    }

    /**
     * Answers the number of tasks currently in the queue.
     * @return The number of tasks currently in the queue.
     */
    public int size() {
        return getQueue().size();
    }



    /**
     * Stop the scheduler if it's running. Switch to stopped, if it's
     * suspended. Clear the task queue, cancelling all un-executed tasks
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() {
        java.util.List<Runnable> tasks=shutdownNow();
        for(Runnable task: tasks) {
            if(task instanceof Future) {
                Future future=(Future)task;
                future.cancel(true);
            }
        }
        getQueue().clear();
        try {
            awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
        }
        catch(InterruptedException e) {
        }
    }




    @Override
    protected void afterExecute(Runnable r, Throwable t)
    {
        try {
           super.afterExecute(r, t);
        }
        finally {
           if(threadDecorator != null)
              threadDecorator.threadReleased(Thread.currentThread());
        }
    }


    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Class which catches exceptions in run() - https://jira.jboss.org/jira/browse/JGRP-1062
     */
    static class RobustRunnable implements Runnable {
        final Runnable command;

        public RobustRunnable(Runnable command) {
            this.command=command;
        }

        public void run() {
            if(command != null) {
                try {
                    command.run();
                }
                catch(Throwable t) {
                    if(log.isErrorEnabled())
                        log.error("exception executing task " + command + ": " +  t);
                }
            }
        }
    }


    private class TaskWrapper<V> implements Runnable, ScheduledFuture<V> {
        private final Task                  task;
        private volatile ScheduledFuture<?> future; // cannot be null !
        private volatile boolean            cancelled=false;


        public TaskWrapper(Task task) {
            this.task=task;
        }

        public ScheduledFuture<?> getFuture() {
            return future;
        }

        public void run() {
            try {
                if(cancelled) {
                    if(future != null)
                        future.cancel(true);
                    return;
                }
                if(future != null && future.isCancelled())
                    return;
                task.run();
            }
            catch(Throwable t) {
                log.error("failed running task " + task, t);
            }

            if(cancelled) {
                if(future != null)
                    future.cancel(true);
                return;
            }
            if(future != null && future.isCancelled())
                return;
            
            doSchedule();
        }


        public void doSchedule() {
            long next_interval=task.nextInterval();
            if(next_interval <= 0) {
                if(log.isTraceEnabled())
                    log.trace("task will not get rescheduled as interval is " + next_interval);
            }
            else {
                future=schedule(this, next_interval, TimeUnit.MILLISECONDS);
                if(cancelled)
                    future.cancel(true);
            }
        }

        public int compareTo(Delayed o) {
            long my_delay=future.getDelay(TimeUnit.MILLISECONDS), their_delay=o.getDelay(TimeUnit.MILLISECONDS);
            return my_delay < their_delay? -1 : my_delay > their_delay? 1 : 0;
        }

        public long getDelay(TimeUnit unit) {
            return future != null? future.getDelay(unit) : -1;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled=true;
            if(future != null)
                future.cancel(mayInterruptIfRunning);
            return cancelled;
        }

        public boolean isCancelled() {
            return cancelled || (future != null && future.isCancelled());
        }

        public boolean isDone() {
            return future == null || future.isDone();
        }

        public V get() throws InterruptedException, ExecutionException {
            return null;
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

    }

}

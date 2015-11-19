
package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.concurrent.*;


/**
 * Implementation of {@link org.jgroups.util.TimeScheduler} by extending
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} to keep tasks sorted. Tasks will get executed in order
 * of execution time (by using a {@link java.util.concurrent.DelayQueue} internally.
 * 
 * @author Bela Ban
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
            log.error(Util.getMessage("CouldNotSetNumberOfTimerThreads"), e);
        }
    }


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

    public void setThreadFactory(ThreadFactory factory) {
        super.setThreadFactory(factory);
    }

    public String dumpTimerTasks() {
        return getQueue().toString();
    }


    public int getCurrentThreads() {
        return super.getPoolSize();
    }

    public int getMinThreads() {
        return super.getCorePoolSize();
    }

    public void setMinThreads(int size) {
        super.setCorePoolSize(size);
    }

    public int getMaxThreads() {
        return super.getMaximumPoolSize();
    }

    public void setMaxThreads(int size) {
        super.setMaximumPoolSize(size);
    }

    public long getKeepAliveTime() {
        return super.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    public void setKeepAliveTime(long time) {
        super.setKeepAliveTime(time, TimeUnit.MILLISECONDS);
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
    public Future<?> scheduleWithDynamicInterval(Task task) {
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




    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Class which catches exceptions in run() - https://jira.jboss.org/jira/browse/JGRP-1062
     */
    protected static class RobustRunnable implements Runnable {
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
                        log.error(Util.getMessage("ExceptionExecutingTask") + command + ": " +  t);
                }
            }
        }

        public String toString() {
            return command != null? command.toString() : null;
        }
    }


    protected class TaskWrapper<V> implements Runnable, Future<V> {
        private final Task         task;
        private volatile Future<?> future; // cannot be null !
        private volatile boolean   cancelled=false;


        public TaskWrapper(Task task) {
            this.task=task;
        }

        public Future<?> getFuture() {
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
                log.error(Util.getMessage("FailedRunningTask") + task, t);
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


        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean retval=!isDone();
            cancelled=true;
            if(future != null)
                future.cancel(mayInterruptIfRunning);
            return retval;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isDone() {
            return cancelled || (future == null || future.isDone());
        }

        public V get() throws InterruptedException, ExecutionException {
            return null;
        }

        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

    }

}

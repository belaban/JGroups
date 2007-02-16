
package org.jgroups.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Global;

import java.util.concurrent.*;


/**
 * Fixed-delay & fixed-rate single thread scheduler
 * <p/>
 * The scheduler supports varying scheduling intervals by asking the task
 * every time for its next preferred scheduling interval. Scheduling can
 * either be <i>fixed-delay</i> or <i>fixed-rate</i>. The notions are
 * borrowed from <tt>java.util.Timer</tt> and retain the same meaning.
 * I.e. in fixed-delay scheduling, the task's new schedule is calculated
 * as:<br>
 * new_schedule = time_task_starts + scheduling_interval
 * <p/>
 * In fixed-rate scheduling, the next schedule is calculated as:<br>
 * new_schedule = time_task_was_supposed_to_start + scheduling_interval
 * <p/>
 * The scheduler internally holds a queue of tasks sorted in ascending order
 * according to their next execution time. A task is removed from the queue
 * if it is cancelled, i.e. if <tt>TimeScheduler.Task.isCancelled()</tt>
 * returns true.
 * <p/>
 * The scheduler internally uses a <tt>java.util.SortedSet</tt> to keep tasks
 * sorted. <tt>java.util.Timer</tt> uses an array arranged as a binary heap
 * that doesn't shrink. It is likely that the latter arrangement is faster.
 * <p/>
 * Initially, the scheduler is in <tt>SUSPEND</tt>ed mode, <tt>start()</tt>
 * need not be called: if a task is added, the scheduler gets started
 * automatically. Calling <tt>start()</tt> starts the scheduler if it's
 * suspended or stopped else has no effect. Once <tt>stop()</tt> is called,
 * added tasks will not restart it: <tt>start()</tt> has to be called to
 * restart the scheduler.
 * @author Bela Ban
 * @version $Id: TimeScheduler.java,v 1.17 2007/02/16 07:32:11 belaban Exp $
 */
public class TimeScheduler extends ScheduledThreadPoolExecutor  {

    /** The interface that submitted tasks must implement */
    public interface Task extends Runnable {
        /** @return the next schedule interval. If <= 0 the task will not be re-scheduled */
        long nextInterval();

        /** Execute the task */
        void run();
    }


    /** Number of milliseconds to wait for pool termination after shutdown */
    private static final long TERMINATION_TIMEOUT=5000;

    /** How many core threads */
    private static int TIMER_DEFAULT_NUM_THREADS=5;


    protected static final Log log=LogFactory.getLog(TimeScheduler.class);



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




    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler() {
        this(TIMER_DEFAULT_NUM_THREADS);
    }

    public TimeScheduler(ThreadFactory factory) {
        super(TIMER_DEFAULT_NUM_THREADS, factory);
    }

    public TimeScheduler(int corePoolSize) {
        super(corePoolSize);
    }



    public String dumpTaskQueue() {
        return getQueue().toString();
    }



    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler.Task#nextInterval()} milliseconds. The task is neve done until nextInterval()
     * return a value <= 0 or the task is cancelled.
     * @param task the task to execute
     * @param relative scheduling scheme: <tt>true</tt>:<br>
     * Task is rescheduled relative to the last time it <i>actually</i> started execution<p/>
     * <tt>false</tt>:<br> Task is scheduled relative to its <i>last</i> execution schedule. This has the effect
     * that the time between two consecutive executions of the task remains the same.<p/>
     * Note that relative is always true; we always schedule the next execution relative to the last *actual*
     * (not scheduled) execution
     */
    public ScheduledFuture<?> scheduleWithDynamicInterval(Task task, boolean relative) {
        if(task == null)
            throw new NullPointerException();

        if (isShutdown())
            return null;

        TaskWrapper task_wrapper=new TaskWrapper(task);
        task_wrapper.doSchedule(); // calls schedule() in ScheduledThreadPoolExecutor
        return new FutureWrapper(task_wrapper);
    }




    /**
     * Add a task for execution at adjustable intervals
     * @param t the task to execute
     */
    public ScheduledFuture<?> scheduleWithDynamicInterval(Task t) {
        return scheduleWithDynamicInterval(t, true);
    }

    /**
     * Answers the number of tasks currently in the queue.
     * @return The number of tasks currently in the queue.
     */
    public int size() {
        return getQueue().size();
    }


    /**
     * Start the scheduler, if it's suspended or stopped
     */
    public void start() {
        ;
    }


    /**
     * Stop the scheduler if it's running. Switch to stopped, if it's
     * suspended. Clear the task queue.
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() throws InterruptedException {
        shutdownNow();
        awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS);
    }


    private class TaskWrapper implements Runnable {
        Task               task;
        ScheduledFuture<?> future; // cannot be null !


        public TaskWrapper(Task task) {
            this.task=task;
        }

        public ScheduledFuture<?> getFuture() {
            return future;
        }

        public void run() {
            try {
                if(future != null && future.isCancelled())
                    return;
                task.run();
            }
            catch(Throwable t) {
                log.error("failed running task " + task, t);
            }
            if(!future.isCancelled()) {
                doSchedule();
            }
        }


        public void doSchedule() {
            long next_interval=task.nextInterval();
            if(next_interval <= 0) {
                if(log.isTraceEnabled())
                    log.trace("task will not get rescheduled as interval is " + next_interval);
            }
            else {
                future=schedule(this, next_interval, TimeUnit.MILLISECONDS);
            }
        }
    }


    private static class FutureWrapper<V> implements ScheduledFuture<V> {
        TaskWrapper task_wrapper;


        public FutureWrapper(TaskWrapper task_wrapper) {
            this.task_wrapper=task_wrapper;
        }

        public long getDelay(TimeUnit unit) {
            ScheduledFuture future=task_wrapper.getFuture();
            return future != null? future.getDelay(unit) : -1;
        }

        public int compareTo(Delayed o) {
            ScheduledFuture future=task_wrapper.getFuture();
            return future != null? future.compareTo(o) : -1;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            ScheduledFuture future=task_wrapper.getFuture();
            return future != null && future.cancel(mayInterruptIfRunning);
        }

        public boolean isCancelled() {
            ScheduledFuture future=task_wrapper.getFuture();
            return future != null && future.isCancelled();
        }

        public boolean isDone() {
            ScheduledFuture future=task_wrapper.getFuture();
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

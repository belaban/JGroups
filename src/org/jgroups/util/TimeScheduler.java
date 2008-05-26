
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
 * @version $Id: TimeScheduler.java,v 1.23.4.3 2008/05/26 09:14:40 belaban Exp $
 */
public class TimeScheduler extends ScheduledThreadPoolExecutor implements ThreadManager  {

    /** The interface that submitted tasks must implement */
    public interface Task extends Runnable {
        /** @return the next schedule interval. If <= 0 the task will not be re-scheduled */
        long nextInterval();

        /** Execute the task */
        void run();
    }   

    /** How many core threads */
    private static int TIMER_DEFAULT_NUM_THREADS=3;


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

    private ThreadDecorator threadDecorator=null;

   /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler() {
        this(TIMER_DEFAULT_NUM_THREADS);
    }

    public TimeScheduler(ThreadFactory factory) {
        this(factory, TIMER_DEFAULT_NUM_THREADS);
    }

    public TimeScheduler(ThreadFactory factory, int max_threads) {
        super(max_threads, factory);
        setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(getRejectedExecutionHandler()));
    }

    public TimeScheduler(int corePoolSize) {
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
        return task_wrapper;
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
     * suspended. Clear the task queue, cancelling all un-executed tasks
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() throws InterruptedException {
        java.util.List<Runnable> tasks=shutdownNow();
        for(Runnable task: tasks) {
            if(task instanceof Future) {
                Future future=(Future)task;
                future.cancel(true);
            }
        }
        getQueue().clear();
        awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
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


   private class TaskWrapper<V> implements Runnable, ScheduledFuture<V> {
        private final Task         task;
        private ScheduledFuture<?> future; // cannot be null !
        private boolean            cancelled=false;


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
                System.out.println("task will not get rescheduled as interval is " + next_interval);
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

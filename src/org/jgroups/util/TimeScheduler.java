
package org.jgroups.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Date;


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
 * @version $Id: TimeScheduler.java,v 1.14.2.2 2007/04/27 09:11:18 belaban Exp $
 */
public class TimeScheduler extends Timer {
    /**
     * The interface that submitted tasks must implement
     */
    public interface Task {
        /**
         * @return true if task is cancelled and shouldn't be scheduled
         *         again
         */
        boolean cancelled();

        /**
         * @return the next schedule interval
         */
        long nextInterval();

        /**
         * Execute the task
         */
        void run();
    }

    public interface CancellableTask extends Task {
        /**
         * Cancels the task. After calling this, {@link #cancelled()} return true. If the task was already cancelled,
         * this is a no-op
         */
        void cancel();
    }


    private int size=0; // maintains the number of tasks currently scheduled to execute

    protected static final Log log=LogFactory.getLog(TimeScheduler.class);


    public TimeScheduler() {
        super(true);
    }

    public TimeScheduler(boolean isDaemon) {
        super(isDaemon);
    }


    public String dumpTaskQueue() {
        return toString();
    }


    /**
     * Add a task for execution at adjustable intervals
     * @param task     the task to execute
     * @param relative scheduling scheme:
     *                 <p/>
     *                 <tt>true</tt>:<br>
     *                 Task is rescheduled relative to the last time it <i>actually</i>
     *                 started execution
     *                 <p/>
     *                 <tt>false</tt>:<br>
     *                 Task is scheduled relative to its <i>last</i> execution schedule. This
     *                 has the effect that the time between two consecutive executions of
     *                 the task remains the same.<p/>
     * April 07: the relative argument is ignored, will always be true
     */
    public void add(Task task, boolean relative) {
        TaskWrapper wrapper=new TaskWrapper(task);
        schedule(wrapper, task.nextInterval());
    }

    /**
     * Add a task for execution at adjustable intervals
     * @param t the task to execute
     */
    public void add(Task t) {
        add(t, true);
    }


    public void schedule(TimerTask task, long delay) {
        super.schedule(task, delay);
        size++;
    }

    public void schedule(TimerTask task, long delay, long period) {
        super.schedule(task, delay, period);
        size++;
    }

    public void schedule(TimerTask task, Date firstTime, long period) {
        super.schedule(task, firstTime, period);
        size++;
    }

    public void schedule(TimerTask task, Date time) {
        super.schedule(task, time);
        size++;
    }

    public void scheduleAtFixedRate(TimerTask task, long delay, long period) {
        super.scheduleAtFixedRate(task, delay, period);
        size++;
    }

    public void scheduleAtFixedRate(TimerTask task, Date firstTime, long period) {
        super.scheduleAtFixedRate(task, firstTime, period);
        size++;
    }


    public void cancel() {
        super.cancel();
        size=0;
    }

    /**
     * Returns the number of tasks currently scheduled. Note that this is an approximation.
     * @return The number of tasks currently in the queue.
     */
    public int size() {
        return size;
    }


    /**
     * Start the scheduler, if it's suspended or stopped
     */
    public void start() {
        ; // no-op
    }


    /**
     * Stop the scheduler if it's running. Switch to stopped, if it's
     * suspended. Clear the task queue.
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() throws InterruptedException {
    }



    
    private class TaskWrapper extends TimerTask {
        private final Task delegate; // points to the user-submitted task


        public TaskWrapper(Task delegate) {
            this.delegate=delegate;
        }

        public void run() {
            if(delegate.cancelled()) {
                cancel();
                return;
            }
            try {
                delegate.run();
            }
            catch(Throwable t) {
                if(log.isWarnEnabled()) {
                    log.warn("exception executing task " + delegate, t);
                }
            }
            size=Math.max(size -1, 0);
            if(!delegate.cancelled()) {
                long next_interval=delegate.nextInterval();
                TimerTask new_task=new TaskWrapper(delegate);
                schedule(new_task, next_interval);
            }
        }


        public boolean cancel() {
            size=Math.max(0, size -1);
            return super.cancel();
        }
    }
}








package org.jgroups.util;


import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;



/**
 * Timer-like interface which allows for execution of tasks. Taks can be executed
 * <ul>
 * <li>one time only
 * <li>at recurring time intervals. Intervals can be fixed-delay or fixed-rate,
 * see {@link java.util.concurrent.ScheduledExecutorService} for details
 * <li>dynamic; at the end of the task execution, a task is asked what the next execution time should be. To do this,
 * method {@link org.jgroups.util.TimeScheduler.Task#nextInterval()} needs to be implemented.
 * </ul>
 * 
 * @author Bela Ban
 */
public interface TimeScheduler {

    /** The interface that dynamic tasks
     * ({@link TimeScheduler#scheduleWithDynamicInterval(org.jgroups.util.TimeScheduler.Task)}) must implement */
    interface Task extends Runnable {
        /** @return the next scheduled interval in ms. If <= 0 the task will not be re-scheduled */
        long nextInterval();
    }


    /**
     * Executes command with zero required delay. This has effect equivalent to <tt>schedule(command, 0, anyUnit)</tt>.
     *
     * @param command the task to execute
     * @throws java.util.concurrent.RejectedExecutionException at discretion of <tt>RejectedExecutionHandler</tt>,
     * if task cannot be accepted for execution because the executor has been shut down.
     * @throws NullPointerException if command is null
     */
    default void execute(Runnable command) {execute(command, true);}
    void execute(Runnable command, boolean can_block);


    /**
     * Creates and executes a one-shot action that becomes enabled after the given delay.
     *
     * @param command the task to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose <tt>get()</tt> method
     *         will return <tt>null</tt> upon completion
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException if command is null
     */
    default Future<?> schedule(Runnable command, long delay, TimeUnit unit) {return schedule(command, delay, unit, true);}
    Future<?> schedule(Runnable command, long delay, TimeUnit unit, boolean can_block);

    
    /**
     * Creates and executes a periodic action that becomes enabled first after the given initial delay, and
     * subsequently with the given delay between the termination of one execution and the commencement of the next.
     * If any execution of the task encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or termination of the executor.
     *
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param delay the delay between the termination of one execution and the commencement of the next
     * @param unit the time unit of the initialDelay and delay parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose <tt>get()</tt>
     *         method will throw an exception upon cancellation
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be scheduled for execution
     * @throws NullPointerException if command is null
     * @throws IllegalArgumentException if delay less than or equal to zero
     */
    default Future<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduleWithFixedDelay(command, initialDelay, delay, unit, true);
    }
    Future<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit, boolean can_block);


    /**
     * Creates and executes a periodic action that becomes enabled first
     * after the given initial delay, and subsequently with the given
     * period; that is executions will commence after
     * <tt>initialDelay</tt> then <tt>initialDelay+period</tt>, then
     * <tt>initialDelay + 2 * period</tt>, and so on.
     * If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or
     * termination of the executor.  If any execution of this task
     * takes longer than its period, then subsequent executions
     * may start late, but will not concurrently execute.
     *
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of
     *         the task, and whose <tt>get()</tt> method will throw an
     *         exception upon cancellation
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be
     *         scheduled for execution
     * @throws NullPointerException if command is null
     * @throws IllegalArgumentException if period less than or equal to zero
     */
    default  Future<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleAtFixedRate(command, initialDelay, period, unit, true);
    }
    Future<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit, boolean can_block);


    
    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler.Task#nextInterval()} milliseconds. This is delay-based and not
     * rate-based.
     * The task is never done until nextInterval() return a value <= 0 or the task is cancelled.
     * @param task the task to execute
     */
    default Future<?> scheduleWithDynamicInterval(Task task) {
        return scheduleWithDynamicInterval(task, true);
    }
    Future<?> scheduleWithDynamicInterval(Task task, boolean can_block);


    void setThreadFactory(ThreadFactory factory);

    /**
     * Returns a list of tasks currently waiting for execution. If there are a lot of tasks, the returned string
     * should probably only return the number of tasks rather than a full dump.
     * @return
     */
    String dumpTimerTasks();

    void removeCancelledTasks();

    /**
     * Returns the configured core threads, or -1 if not applicable
     * @return
     */
    int getMinThreads();

    /** Sets the core pool size. Can be ignored if not applicable */
    void setMinThreads(int size);


    /**
     * Returns the configured max threads, or -1 if not applicable
     * @return
     */
    int getMaxThreads();

    /** Sets the max pool size. Can be ignored if not applicable */
    void setMaxThreads(int size);

    /** Returns the keep alive time (in ms) of the thread pool, or -1 if not applicable */
    long getKeepAliveTime();

    /** Sets the keep alive time (in ms) of the thread pool. Can be ignored if not applicable */
    void setKeepAliveTime(long time);


    /**
     * Returns the current threads in the pool, or -1 if not applicable
     * @return
     */
    int getCurrentThreads();

    boolean getNonBlockingTaskHandling();
    void    setNonBlockingTaskHandling(boolean b);


    /**
     * Returns the number of tasks currently in the queue.
     * @return The number of tasks currently in the queue.
     */
    int size();

    /** Starts the runner thread */
    void start();

    /** Stops the scheduler if running, cancelling all pending tasks */
    void stop();
    

    /** Returns true if stop() has been called, false otherwise */
    boolean isShutdown();
}

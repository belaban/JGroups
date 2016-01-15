
package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of {@link org.jgroups.util.TimeScheduler}. Uses a thread pool and a single thread which waits for the
 * next task to be executed. When ready, it passes the task to the associated pool to get executed. When multiple tasks
 * are scheduled to get executed at the same time, they're collected in a queue associated with the task execution
 * time, and are executed together.
 *
 * @author Bela Ban
 * @deprecated Use {@link org.jgroups.util.TimeScheduler3} instead
 */
@Deprecated
public class TimeScheduler2 implements TimeScheduler, Runnable  {
    private final ThreadPoolExecutor pool;

    private final ConcurrentSkipListMap<Long,Entry> tasks=new ConcurrentSkipListMap<>();

    private Thread runner=null;

    private final Lock lock=new ReentrantLock();

    private final Condition tasks_available=lock.newCondition();

    @GuardedBy("lock")
    private long next_execution_time=0;

    /** Needed to signal going from 0 tasks to non-zero (we cannot use tasks.isEmpty() here ...) */
    protected final AtomicBoolean no_tasks=new AtomicBoolean(true);

    protected volatile boolean running=false;

    protected static final Log log=LogFactory.getLog(TimeScheduler2.class);

    protected ThreadFactory timer_thread_factory=null;

    protected static final long SLEEP_TIME=10000;


    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler2() {
        pool=new ThreadPoolExecutor(4, 10,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        init();
    }


    public TimeScheduler2(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time, int max_queue_size,
                          String rejection_policy) {
        timer_thread_factory=factory;
        RejectedExecutionHandler tmp=Util.parseRejectionPolicy(rejection_policy);
        pool=new ThreadPoolExecutor(min_threads, max_threads,keep_alive_time, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(max_queue_size),
                                    factory, tmp);
        init();
    }

    public TimeScheduler2(int corePoolSize) {
        pool=new ThreadPoolExecutor(corePoolSize, corePoolSize * 2,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        init();
    }


    public void setThreadFactory(ThreadFactory f) {pool.setThreadFactory(f);}
    public int  getMinThreads()                   {return pool.getCorePoolSize();}
    public void setMinThreads(int size)           {pool.setCorePoolSize(size);}
    public int  getMaxThreads()                   {return pool.getMaximumPoolSize();}
    public void setMaxThreads(int size)           {pool.setMaximumPoolSize(size);}
    public long getKeepAliveTime()                {return pool.getKeepAliveTime(TimeUnit.MILLISECONDS);}
    public void setKeepAliveTime(long time)       {pool.setKeepAliveTime(time, TimeUnit.MILLISECONDS);}
    public int  getCurrentThreads()               {return pool.getPoolSize();}
    public int  getQueueSize()                    {return pool.getQueue().size();}


    public String dumpTimerTasks() {
        StringBuilder sb=new StringBuilder();
        for(Entry entry: tasks.values()) {
            sb.append(entry.dump()).append("\n");
        }
        return sb.toString();
    }




    public void execute(Runnable task) {
        schedule(task, 0, TimeUnit.MILLISECONDS);
    }


    public Future<?> schedule(Runnable work, long delay, TimeUnit unit) {
        if(work == null)
            return null;

        Future<?> retval=null;

        long key=System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(delay, unit); // execution time
        Entry task=new Entry(work);
        while(!isShutdown()) {
            Entry existing=tasks.putIfAbsent(key, task);
            if(existing == null) {
                retval=task.getFuture();
                break; // break out of the while loop
            }
            if((retval=existing.add(work)) != null)
                break;

            // Else the existing entry is completed.  It'll be removed shortly, so we just loop around again.
            // Don't remove the entry ourselves - see JGRP-1457.
        }

        if(key < next_execution_time || no_tasks.compareAndSet(true, false)) {
            if(key >= next_execution_time)
                key=0L;
            taskReady(key);
        }

        return retval;
    }



    public Future<?> scheduleWithFixedDelay(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown())
            return null;
        RecurringTask wrapper=new FixedIntervalTask(task, delay);
        wrapper.doSchedule(initial_delay);
        return wrapper;
    }


    public Future<?> scheduleAtFixedRate(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown())
            return null;
        RecurringTask wrapper=new FixedRateTask(task, delay);
        wrapper.doSchedule(initial_delay);
        return wrapper;
    }


    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler2.RecurringTask#nextInterval()} milliseconds. The task is never done until nextInterval()
     * return a value <= 0 or the task is cancelled.
     * @param task the task to execute
     * Task is rescheduled relative to the last time it <i>actually</i> started execution<p/>
     * <tt>false</tt>:<br> Task is scheduled relative to its <i>last</i> execution schedule. This has the effect
     * that the time between two consecutive executions of the task remains the same.<p/>
     * Note that relative is always true; we always schedule the next execution relative to the last *actual*
     */
    public Future<?> scheduleWithDynamicInterval(Task task) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown())
            return null;
        RecurringTask task_wrapper=new DynamicIntervalTask(task);
        task_wrapper.doSchedule(); // calls schedule() in ScheduledThreadPoolExecutor
        return task_wrapper;
    }




    /**
     * Returns the number of tasks currently in the timer
     * @return The number of tasks currently in the timer
     */
    public int size() {
        int retval=0;
        Collection<Entry> values=tasks.values();
        for(Entry entry: values)
            retval+=entry.size();
        return retval;
    }


    public String toString() {
        return getClass().getSimpleName();
    }


    /**
     * Stops the timer, cancelling all tasks
     *
     * @throws InterruptedException if interrupted while waiting for thread to return
     */
    public void stop() {
        stopRunner();

        java.util.List<Runnable> remaining_tasks=pool.shutdownNow();
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

        for(Entry entry: tasks.values())
            entry.cancel();
        tasks.clear();
    }


    public boolean isShutdown() {
        return pool.isShutdown();
    }


    public void run() {
        while(running) {
            try {
                _run();
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedExecutingTasksS"), t);
            }
        }
    }


    protected void _run() {
        ConcurrentNavigableMap<Long,Entry> head_map; // head_map = entries which are <= curr time (ready to be executed)
        head_map=tasks.headMap(System.currentTimeMillis(), true);
        if(!head_map.isEmpty()) {
            final List<Long> keys=new LinkedList<>();
            for(Map.Entry<Long,Entry> entry: head_map.entrySet()) {
                final Long key=entry.getKey();
                final Entry val=entry.getValue();
                Runnable task=new Runnable() {public void run() {val.execute();}};
                try {
                    pool.execute(task);
                }
                catch(RejectedExecutionException rejected) { // only thrown if rejection policy is "abort"
                    Thread thread=timer_thread_factory != null?
                      timer_thread_factory.newThread(task, "Timer temp thread")
                      : new Thread(task, "Timer temp thread");
                    thread.start();
                }
                keys.add(key);
            }
            // we cannot use headMap.clear(); removed performance hotspot (https://issues.jboss.org/browse/JGRP-1490)
            for(Long key: keys)
                tasks.remove(key);
        }

        if(tasks.isEmpty()) {
            no_tasks.compareAndSet(false, true);
            waitFor(); // sleeps until time elapses, or a task with a lower execution time is added
        }
        else
            waitUntilNextExecution(); // waits until next execution, or a task with a lower execution time is added
    }


    protected void init() {
        startRunner();
    }

    /**
     * Sleeps until the next task in line is ready to be executed
     */
    protected void waitUntilNextExecution() {
        lock.lock();
        try {
            if(!running)
                return;
            next_execution_time=tasks.firstKey();
            long sleep_time=next_execution_time - System.currentTimeMillis();
            tasks_available.await(sleep_time, TimeUnit.MILLISECONDS);
        }
        catch(InterruptedException e) {
        }
        finally {
            lock.unlock();
        }
    }

    protected void waitFor() {
        lock.lock();
        try {
            if(!running)
                return;
            tasks_available.await(SLEEP_TIME, TimeUnit.MILLISECONDS);
        }
        catch(InterruptedException e) {
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Signals that a task with a lower execution time than next_execution_time is ready
     */
    protected void taskReady(long trigger_time) {
        lock.lock();
        try {
            if(trigger_time > 0)
                next_execution_time=trigger_time;
            tasks_available.signal();
        }
        finally {
            lock.unlock();
        }
    }

    protected void startRunner() {
        running=true;
        runner=timer_thread_factory != null? timer_thread_factory.newThread(this, "Timer runner") : new Thread(this, "Timer runner");
        runner.start();
    }

    protected void stopRunner() {
        lock.lock();
        try {
            running=false;
            tasks_available.signal();
        }
        finally {
            lock.unlock();
        }
    }




    private static class Entry {
        private final MyTask     task; // the task (wrapper) to execute
        private MyTask           last; // points to the last task
        private final Lock       lock=new ReentrantLock();

        @GuardedBy("lock")
        private boolean          completed=false; // set to true when the task has been executed


        private Entry(Runnable task) {
            last=this.task=new MyTask(task);
        }

        Future<?> getFuture() {
            return task;
        }

        Future<?> add(Runnable task) {
            lock.lock();
            try {
                if(completed)
                    return null;
                MyTask retval=new MyTask(task);
                last.next=retval;
                last=last.next;
                return retval;
            }
            finally {
                lock.unlock();
            }
        }

        void execute() {
            lock.lock();
            try {
                if(completed)
                    return;
                completed=true;

                for(MyTask tmp=task; tmp != null; tmp=tmp.next) {
                    if(!(tmp.isCancelled() || tmp.isDone())) {
                        try {
                            tmp.run();
                        }
                        catch(Throwable t) {
                            log.error(Util.getMessage("TaskExecutionFailed"), t);
                        }
                        finally {
                            tmp.done=true;
                        }
                    }
                }
            }
            finally {
                lock.unlock();
            }
        }

        void cancel() {
            lock.lock();
            try {
                if(completed)
                    return;
                for(MyTask tmp=task; tmp != null; tmp=tmp.next)
                    tmp.cancel(true);
            }
            finally {
                lock.unlock();
            }
        }

        int size() {
            int retval=1;
            for(MyTask tmp=task.next; tmp != null; tmp=tmp.next)
                retval++;
            return retval;
        }

        public String toString() {
            return size() + " tasks";
        }

        public String dump() {
            StringBuilder sb=new StringBuilder();
            boolean first=true;
            for(MyTask tmp=task; tmp != null; tmp=tmp.next) {
                if(!first)
                    sb.append(", ");
                else
                    first=false;
                sb.append(tmp);
            }
            return sb.toString();
        }

    }


    /**
     * Simple task wrapper, always executed by at most 1 thread.
     */
    protected static class MyTask implements Future, Runnable {
        protected final Runnable   task;
        protected volatile boolean cancelled=false;
        protected volatile boolean done=false;
        protected MyTask           next;

        protected MyTask(Runnable task) {
            this.task=task;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean retval=!isDone();
            cancelled=true;
            return retval;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isDone() {
            return done || cancelled;
        }

        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        public void run() {
            if(isDone())
                return;
            try {
                task.run();
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedExecutingTask") + task, t);
            }
            finally {
                done=true;
            }
        }

        public String toString() {
            return task.toString();
        }
    }


    /**
     * Task which executes multiple times. An instance of this class wraps the real task and intercepts run(): when
     * called, it forwards the call to task.run() and then schedules another execution (until cancelled). The
     * {@link #nextInterval()} method determines the time to wait until the next execution.
     * @param <V>
     */
    private abstract class RecurringTask<V> implements Runnable, Future<V> {
        protected final Runnable      task;
        protected volatile Future<?>  future; // cannot be null !
        protected volatile boolean    cancelled=false;


        private RecurringTask(Runnable task) {
            this.task=task;
        }

        /**
         * The time to wait until the next execution
         * @return Number of milliseconds to wait until the next execution is scheduled
         */
        protected abstract long nextInterval();

        protected boolean rescheduleOnZeroDelay() {return false;}

        public void doSchedule() {
            long next_interval=nextInterval();
            if(next_interval <= 0 && !rescheduleOnZeroDelay()) {
                if(log.isTraceEnabled())
                    log.trace("task will not get rescheduled as interval is " + next_interval);
                return;
            }

            future=schedule(this, next_interval, TimeUnit.MILLISECONDS);
            if(cancelled)
                future.cancel(true);
        }

        public void doSchedule(long next_interval) {
            future=schedule(this, next_interval, TimeUnit.MILLISECONDS);
            if(cancelled)
                future.cancel(true);
        }


        public void run() {
            if(cancelled) {
                if(future != null)
                    future.cancel(true);
                return;
            }

            try {
                task.run();
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedRunningTask") + task, t);
            }
            if(!cancelled)
                doSchedule();
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

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(task + ", cancelled=" + isCancelled());
            return sb.toString();
        }
    }


    private class FixedIntervalTask<V> extends RecurringTask<V> {
        final long interval;

        private FixedIntervalTask(Runnable task, long interval) {
            super(task);
            this.interval=interval;
        }

        protected long nextInterval() {
            return interval;
        }
    }

    private class FixedRateTask<V> extends RecurringTask<V> {
        final long interval;
        final long first_execution;
        int num_executions=0;

        private FixedRateTask(Runnable task, long interval) {
            super(task);
            this.interval=interval;
            this.first_execution=System.currentTimeMillis();
        }

        protected long nextInterval() {
            long target_time=first_execution + (interval * ++num_executions);
            return target_time - System.currentTimeMillis();
        }

        protected boolean rescheduleOnZeroDelay() {return true;}
    }


   private class DynamicIntervalTask<V> extends RecurringTask<V> {

       private DynamicIntervalTask(Task task) {
           super(task);
       }

       protected long nextInterval() {
           if(task instanceof Task)
               return ((Task)task).nextInterval();
           return 0;
       }
   }



}

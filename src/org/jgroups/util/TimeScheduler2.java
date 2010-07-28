
package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.Collection;
import java.util.LinkedList;
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
 * @version $Id: TimeScheduler2.java,v 1.13 2010/07/28 14:20:33 belaban Exp $
 */
@Experimental
public class TimeScheduler2 implements TimeScheduler, Runnable  {
    private ThreadManagerThreadPoolExecutor pool;

    private final ConcurrentSkipListMap<Long,Entry> tasks=new ConcurrentSkipListMap<Long,Entry>();

    private Thread runner=null;

    private final Lock lock=new ReentrantLock();

    private final Condition tasks_available=lock.newCondition();

    @GuardedBy("lock")
    private long next_execution_time=0;

    protected final AtomicBoolean no_tasks=new AtomicBoolean(true);

    protected volatile boolean running=false;

    protected static final Log log=LogFactory.getLog(TimeScheduler2.class);

    protected ThreadDecorator threadDecorator=null;

    protected ThreadFactory timer_thread_factory=null;


    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    public TimeScheduler2() {
        pool=new ThreadManagerThreadPoolExecutor(1, 4,
                                                 5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                                 Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        if(threadDecorator != null)
            pool.setThreadDecorator(threadDecorator);
    }


    public TimeScheduler2(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time, int max_queue_size) {
        pool=new ThreadManagerThreadPoolExecutor(min_threads, max_threads,keep_alive_time, TimeUnit.MILLISECONDS,
                                                 new LinkedBlockingQueue<Runnable>(max_queue_size),
                                                 factory, new ThreadPoolExecutor.CallerRunsPolicy());
        if(threadDecorator != null)
            pool.setThreadDecorator(threadDecorator);
    }

    public TimeScheduler2(int corePoolSize) {
        pool=new ThreadManagerThreadPoolExecutor(corePoolSize, corePoolSize,
                                                 5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                                 Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        if(threadDecorator != null)
            pool.setThreadDecorator(threadDecorator);
    }

    public ThreadDecorator getThreadDecorator() {
        return threadDecorator;
    }

    public void setThreadDecorator(ThreadDecorator threadDecorator) {
        this.threadDecorator=threadDecorator;
        pool.setThreadDecorator(threadDecorator);
    }

    public void setThreadFactory(ThreadFactory factory) {
        pool.setThreadFactory(factory);
    }

    public void setTimerThreadFactory(ThreadFactory factory) {
        timer_thread_factory=factory;
    }

    public ThreadFactory getTimerThreadFactory() {
        return timer_thread_factory;
    }

    public int getMinThreads() {
        return pool.getCorePoolSize();
    }

    public void setMinThreads(int size) {
        pool.setCorePoolSize(size);
    }

    public int getMaxThreads() {
        return pool.getMaximumPoolSize();
    }

    public void setMaxThreads(int size) {
        pool.setMaximumPoolSize(size);
    }

    public long getKeepAliveTime() {
        return pool.getKeepAliveTime(TimeUnit.MILLISECONDS);
    }

    public void setKeepAliveTime(long time) {
        pool.setKeepAliveTime(time, TimeUnit.MILLISECONDS);
    }

    public int getCurrentThreads() {
        return pool.getPoolSize();
    }

    public int getQueueSize() {
        return pool.getQueue().size();
    }


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

        long key=unit.convert(delay, TimeUnit.MILLISECONDS) + System.currentTimeMillis(); // execution time
        Future<?> task=new Entry(work);
        while(!isShutdown()) {
            Entry existing=tasks.putIfAbsent(key, (Entry)task);
            if(existing == null)
                break; // break out of the while loop
            Future<?> tmp;
            if((tmp=existing.add(work)) != null) {
                task=tmp;
                break;
            }
        }

        if(!running)
            startRunner();

        if(key < next_execution_time || no_tasks.compareAndSet(true, false)) {
            if(key >= next_execution_time)
                key=0L;
            taskReady(key);
        }

        return task;
    }



    public Future<?> scheduleWithFixedDelay(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown())
            return null;
        RecurringTask wrapper=new FixedIntervalTask(task, delay);
        wrapper.doSchedule();
        return wrapper;
    }


    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.TimeScheduler2.Task#nextInterval()} milliseconds. The task is neve done until nextInterval()
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
            entry.cancel(true);
        tasks.clear();
    }


    public boolean isShutdown() {
        return pool.isShutdown();
    }

    public void run() {
        try {
            _run();
        }
        finally {
            running=false;
        }
    }

    private void _run() {
        int cnt=0;
        ConcurrentNavigableMap<Long,Entry> head_map;
        while(running) {
            while(running && !tasks.isEmpty()) {
                
                // head_map = entries which are <= curr time (ready to be executed)
                if((head_map=tasks.headMap(System.currentTimeMillis(), true)).isEmpty())
                    break;

                for(final Entry entry: head_map.values()) {
                    pool.execute(new Runnable() {
                        public void run() {
                            entry.execute();
                        }
                    });
                }
                head_map.clear();
            }

            if(tasks.isEmpty()) {
                no_tasks.compareAndSet(false, true);
                if(++cnt >= 10)
                    break;    // terminates the thread - will be restarted on the next task submission
                waitFor(100); // sleeps until time elapses, or a task with a lower execution time is added
            }
            else {
                cnt=0;
                waitUntilNextExecution(); // waits until next execution, or a task with a lower execution time is added
            }
        }
    }

    /**
     * Sleeps until the next task in line is ready to be executed
     */
    protected void waitUntilNextExecution() {
        lock.lock();
        try {
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

    protected void waitFor(long sleep_time) {
        lock.lock();
        try {
            tasks_available.await(sleep_time, TimeUnit.MILLISECONDS);
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
        try {
            if(lock.tryLock(10, TimeUnit.MILLISECONDS)) {
                try {
                    if(trigger_time > 0)
                        next_execution_time=trigger_time;
                    tasks_available.signal();
                }
                finally {
                    lock.unlock();
                }
            }
        }
        catch(InterruptedException e) {
        }
    }

    protected void startRunner() {
        synchronized(this) {
            if(runner == null || !runner.isAlive()) {
                running=true;
                runner=timer_thread_factory != null?
                        timer_thread_factory.newThread(this, "Timer runner") :
                        new Thread(this, "Timer thread");
                runner.start();
            }
        }
    }

    protected void stopRunner() {
        synchronized(this) {
            running=false;
        }

        lock.lock();
        try {
            tasks_available.signal();
        }
        finally {
            lock.unlock();
        }
    }


    

    private static class Entry implements Future {
        final Runnable task;

        private Collection<MyTask> queue=null;

        @GuardedBy("lock")
        private final Lock lock=new ReentrantLock();

        private volatile boolean cancelled=false;
        private volatile boolean done=false;


        public Entry(Runnable task) {
            this.task=task;
        }

        Future<?> add(Runnable task) {
            if(done)
                return null;
            lock.lock();
            try {
                if(done)
                    return null;
                if(queue == null)
                    queue=new LinkedList<MyTask>(); // queue is protected by lock anyway
                MyTask retval=new MyTask(task);
                queue.add(retval);
                return retval;
            }
            finally {
                lock.unlock();
            }
        }

        void execute() {
            if(done)
                return;
            if(!cancelled) {
                try {
                    task.run();
                }
                catch(Throwable t) {
                    log.error("task execution failed", t);
                }
            }

            lock.lock();
            try {
                if(queue != null) {
                    for(MyTask tmp: queue) {
                        if(tmp.isCancelled() || tmp.isDone())
                            continue;
                        try {
                            tmp.run();
                        }
                        catch(Throwable t) {
                            log.error("task execution failed", t);
                        }
                        finally {
                            tmp.done=true;
                        }
                    }
                }
            }
            finally {
                done=true;
                lock.unlock();
            }
        }

        int size() {
            return 1 + (queue != null? queue.size() : 0);
        }

        public String toString() {
            return size() + " tasks";
        }

        public String dump() {
            StringBuilder sb=new StringBuilder();
            sb.append(task);
            if(queue != null) {
                for(MyTask task: queue)
                    sb.append(", ").append(task);
            }
            return sb.toString();
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            if(cancelled || done)
                return false;
            cancelled=true;
            return true;
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
    }

    protected static class MyTask implements Future, Runnable {
        protected final Runnable   task;
        protected volatile boolean cancelled=false;
        protected volatile boolean done=false;

        public MyTask(Runnable task) {
            this.task=task;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            if(cancelled || done)
                return false;
            cancelled=done=true;
            return true;
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public boolean isDone() {
            return done;
        }

        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        public void run() {
            if(cancelled || done)
                return;
            try {
                task.run();
            }
            catch(Throwable t) {
                log.error("failed executing task " + task, t);
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


        public RecurringTask(Runnable task) {
            this.task=task;
        }

        /**
         * The time to wait until the next execution
         * @return Number of milliseconds to wait until the next execution is scheduled
         */
        protected abstract long nextInterval();

        public void doSchedule() {
            long next_interval=nextInterval();
            if(next_interval <= 0) {
                if(log.isTraceEnabled())
                    log.trace("task will not get rescheduled as interval is " + next_interval);
                return;
            }
            
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
                log.error("failed running task " + task, t);
            }
            if(!cancelled)
                doSchedule();
        }


        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled=true;
            if(future != null)
                future.cancel(mayInterruptIfRunning);
            return cancelled;
        }

        public boolean isCancelled() {
            return cancelled;
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

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(getClass().getSimpleName() + ": task=" + task + ", cancelled=" + cancelled);
            return sb.toString();
        }
    }


    private class FixedIntervalTask<V> extends RecurringTask<V> {
        final long interval;

        public FixedIntervalTask(Runnable task, long interval) {
            super(task);
            this.interval=interval;
        }

        protected long nextInterval() {
            return interval;
        }
    }


   private class DynamicIntervalTask<V> extends RecurringTask<V> {

       public DynamicIntervalTask(Task task) {
           super(task);
       }

       protected long nextInterval() {
           if(task instanceof Task)
               return ((Task)task).nextInterval();
           return 0;
       }
   }



}

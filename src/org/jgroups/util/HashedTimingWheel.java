package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of {@link TimeScheduler}. Uses a hashed timing wheel [1].
 *
 * [1] http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt
 *
 * @author Bela Ban
 */
@Experimental @Unsupported
public class HashedTimingWheel implements TimeScheduler, Runnable  {
    private final ThreadPoolExecutor pool;

    private Thread runner=null;

    private final Lock lock=new ReentrantLock();

    protected volatile boolean running;

    protected static final Log log=LogFactory.getLog(HashedTimingWheel.class);

    protected ThreadFactory timer_thread_factory=null;

    protected int wheel_size=200;   // number of ticks on the timing wheel

    protected long tick_time=50L;  // number of milliseconds a tick has

    protected final long ROTATION_TIME;// time for 1 lap

    protected final List<MyTask>[] wheel;

    protected int wheel_position=0; // current position of the wheel, run() advances it by one (every TICK_TIME ms)


    /**
     * Create a scheduler that executes tasks in dynamically adjustable intervals
     */
    @SuppressWarnings("unchecked")
    public HashedTimingWheel() {
        ROTATION_TIME=wheel_size * tick_time;
        wheel=new List[wheel_size];
        pool=new ThreadPoolExecutor(4, 10,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        init();
    }


    @SuppressWarnings("unchecked")
    public HashedTimingWheel(ThreadFactory factory, int min_threads, int max_threads, long keep_alive_time, int max_queue_size,
                             int wheel_size, long tick_time) {
        this.wheel_size=wheel_size;
        this.tick_time=tick_time;
        ROTATION_TIME=wheel_size * tick_time;
        wheel=new List[this.wheel_size];
        timer_thread_factory=factory;
        pool=new ThreadPoolExecutor(min_threads, max_threads,keep_alive_time, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(max_queue_size),
                                    factory, new ThreadPoolExecutor.CallerRunsPolicy());
        init();
    }


    @SuppressWarnings("unchecked")
    public HashedTimingWheel(int corePoolSize) {
        ROTATION_TIME=wheel_size * tick_time;
        wheel=(List<MyTask>[])new List[wheel_size];
        pool=new ThreadPoolExecutor(corePoolSize, corePoolSize * 2,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
        init();
    }


    public void setThreadFactory(ThreadFactory factory) {
        pool.setThreadFactory(factory);
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

        lock.lock();
        try {
            for(List<MyTask> list: wheel) {
                if(!list.isEmpty()) {
                    sb.append(list).append("\n");
                }
            }
        }
        finally {
            lock.unlock();
        }

        return sb.toString();
    }




    public void execute(Runnable task) {
        schedule(task, 0, TimeUnit.MILLISECONDS);
    }


    public Future<?> schedule(Runnable work, long delay, TimeUnit unit) {
        if(work == null)
            throw new NullPointerException();
        if (isShutdown() || !running)
            return null;

        MyTask retval=null;
        long time=TimeUnit.MILLISECONDS.convert(delay, unit); // execution time

        lock.lock();
        try {
            int num_ticks=(int)Math.max(1, ((time % ROTATION_TIME) / tick_time));
            int position=(wheel_position + num_ticks) % wheel_size;
            int rounds=(int)(time / ROTATION_TIME);
            List<MyTask> list=wheel[position];
            retval=new MyTask(work, rounds);
            list.add(retval);
        }
        finally {
            lock.unlock();
        }

        return retval;
    }



    public Future<?> scheduleWithFixedDelay(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown() || !running)
            return null;
        RecurringTask wrapper=new FixedIntervalTask(task, delay);
        wrapper.doSchedule(initial_delay);
        return wrapper;
    }


    public Future<?> scheduleAtFixedRate(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        if(task == null)
            throw new NullPointerException();
        if (isShutdown() || !running)
            return null;
        RecurringTask wrapper=new FixedRateTask(task, delay);
        wrapper.doSchedule(initial_delay);
        return wrapper;
    }


    /**
     * Schedule a task for execution at varying intervals. After execution, the task will get rescheduled after
     * {@link org.jgroups.util.HashedTimingWheel.Task#nextInterval()} milliseconds. The task is neve done until nextInterval()
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
        if (isShutdown() || !running)
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

        lock.lock();
        try {
            for(List<MyTask> list: wheel)
                retval+=list.size();
            return retval;
        }
        finally {
            lock.unlock();
        }
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


    public boolean isShutdown() {
        return pool.isShutdown();
    }


    public void run() {
        final long base_time=System.currentTimeMillis();
        long next_time, sleep_time;
        long cnt=0;

        while(running) {
            try {
                _run();
                next_time=base_time + (++cnt * tick_time);
                sleep_time=Math.max(0, next_time - System.currentTimeMillis());
                Util.sleep(sleep_time);
            }
            catch(Throwable t) {
                log.error("failed executing tasks(s)", t);
            }
        }
    }


    protected void _run() {
        lock.lock();
        try {
            wheel_position=(wheel_position +1) % wheel_size;
            List<MyTask> list=wheel[wheel_position];
            if(list.isEmpty())
                return;
            for(Iterator<MyTask> it=list.iterator(); it.hasNext();) {
                MyTask tmp=it.next();
                if(tmp.getAndDecrementRound() <= 0) {
                    try {
                        pool.execute(tmp);
                    }
                    catch(Throwable t) {
                        log.error("failure submitting task to thread pool", t);
                    }
                    it.remove();
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    protected void init() {
        for(int i=0; i < wheel.length; i++)
            wheel[i]=new LinkedList<>();
        startRunner();
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
            for(List<MyTask> list: wheel) {
                if(!list.isEmpty()) {
                    for(MyTask task: list)
                        task.cancel(true);
                    list.clear();
                }
            }
        }
        finally {
            lock.unlock();
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
        protected int              round;

        public MyTask(Runnable task, int round) {
            this.task=task;
            this.round=round;
        }

        public int getRound() {
            return round;
        }

        public int getAndDecrementRound() {
            return round--;
        }

        public void setRound(int round) {
            this.round=round;
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
                log.error("failed running task " + task, t);
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
            sb.append(getClass().getSimpleName() + ": task=" + task + ", cancelled=" + isCancelled());
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

    private class FixedRateTask<V> extends RecurringTask<V> {
        final long interval;
        final long first_execution;
        int num_executions=0;

        public FixedRateTask(Runnable task, long interval) {
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


package org.jgroups.util;


import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Implementation of {@link org.jgroups.util.TimeScheduler}. Uses a thread pool and a single thread which waits for the
 * next task to be executed. When ready, it passes the task to the associated pool to get executed.
 *
 * @author Bela Ban
 * @version $Id: TimeScheduler2.java,v 1.2 2010/07/20 10:36:46 belaban Exp $
 */
public class TimeScheduler2 implements TimeScheduler, Runnable  {


    private ThreadPoolExecutor pool;

    private final ConcurrentSkipListMap<Long,Entry> tasks=new ConcurrentSkipListMap<Long,Entry>();

    private volatile Thread runner=null;

    protected volatile boolean running=false;


    /** How many core threads */
    private static int TIMER_DEFAULT_NUM_THREADS=3;

    private static long INTERVAL=100;


    protected static final Log log=LogFactory.getLog(TimeScheduler2.class);



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
    public TimeScheduler2() {
       // todo: wrap ThreadPoolExecutor with ThreadManagerThreadPoolExecutor and invoke setThreadDecorator()
       pool=new ThreadPoolExecutor(TIMER_DEFAULT_NUM_THREADS, TIMER_DEFAULT_NUM_THREADS,
                                   5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                   Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
   }

    public TimeScheduler2(ThreadFactory factory) {
        pool=new ThreadPoolExecutor(TIMER_DEFAULT_NUM_THREADS, TIMER_DEFAULT_NUM_THREADS,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    factory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public TimeScheduler2(ThreadFactory factory, int max_threads) {
        pool=new ThreadPoolExecutor(TIMER_DEFAULT_NUM_THREADS, max_threads,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    factory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public TimeScheduler2(int corePoolSize) {
        pool=new ThreadPoolExecutor(corePoolSize, corePoolSize,
                                    5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(5000),
                                    Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public ThreadDecorator getThreadDecorator() {
        return threadDecorator;
    }

    public void setThreadDecorator(ThreadDecorator threadDecorator) {
        this.threadDecorator=threadDecorator;
    }

    public void setThreadFactory(ThreadFactory factory) {
        pool.setThreadFactory(factory);
    }

    public int getMinThreads() {
        return pool.getCorePoolSize();
    }

    public int getMaxThreads() {
        return pool.getMaximumPoolSize();
    }

    public int getActiveThreads() {
        return pool.getActiveCount();
    }

    public BlockingQueue<Runnable> getQueue() {
        return pool.getQueue();
    }

    public String dumpTaskQueue() {
        return pool.getQueue().toString();
    }





    public void execute(Runnable task) {
        schedule(task, 0, TimeUnit.MILLISECONDS);
    }


    public Future<?> schedule(Runnable work, long delay, TimeUnit unit) {
        if(work == null)
            return null;

        long key=unit.convert(delay, TimeUnit.MILLISECONDS) + System.currentTimeMillis(); // execution time

        // key=322649L; // todo: remove !

        Entry task=new Entry(work);
        Entry existing=tasks.putIfAbsent(key, task);
        if(existing != null) {
            existing.add(work);
            task=existing;
        }

        if(!running)
            startRunner();

        return task;
    }



    // todo: cancellation doesn't work with FixedDelayTask !!
    public Future<?> scheduleWithFixedDelay(Runnable task, long initial_delay, long delay, TimeUnit unit) {
        FixedDelayTask wrapper=new FixedDelayTask(task, this, delay);
        return schedule(wrapper, initial_delay, unit);
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

        if (pool.isShutdown())
            return null;

        TaskWrapper task_wrapper=new TaskWrapper(task);
        task_wrapper.doSchedule(); // calls schedule() in ScheduledThreadPoolExecutor
        return task_wrapper;
    }




    /**
     * Answers the number of tasks currently in the queue.
     * @return The number of tasks currently in the queue.
     */
    public int size() {
        return tasks.size();
    }



    /**
     * Stop the scheduler if it's running. Switch to stopped, if it's
     * suspended. Clear the task queue, cancelling all un-executed tasks
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() {
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
        stopRunner();
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
        while(running && !tasks.isEmpty()) {
            while(running && !tasks.isEmpty()) {
                long current_time=System.currentTimeMillis();
                long execution_time=tasks.firstKey();
                if(execution_time <= current_time) {
                    final Entry entry=tasks.remove(execution_time);
                    if(entry != null) {
                        pool.execute(new Runnable() {
                            public void run() {
                                entry.execute();
                            }
                        });
                    }
                }
                else
                    break;
            }

            // todo: get rid of the fixed sleep, always maintain a variable to sleep till the next task ! 
            Util.sleep(INTERVAL);
        }
    }

    protected void startRunner() {
        synchronized(this) {
            if(runner == null || !runner.isAlive()) {
                runner=new Thread(this, "Timer runner");
                runner.start();
                running=true;
            }
        }
    }

    protected void stopRunner() {
        synchronized(this) {
            running=false;
        }
    }


    
    protected void afterExecute(Runnable r, Throwable t)
    {
//        try {
//            pool.afterExecute(r, t);
//        }
//        finally {
//            if(threadDecorator != null)
//                threadDecorator.threadReleased(Thread.currentThread());
//        }
    }



    private static class Entry implements Future {
        final Runnable task;

        private ConcurrentLinkedQueue<Runnable> queue=null;

        private final AtomicBoolean cancelled=new AtomicBoolean(false);
        volatile boolean done=false;


        public Entry(Runnable task) {
            this.task=task;
        }

        void add(Runnable task) {
            if(task == null)
                return;

            if(cancelled.get())
                System.err.println("add(): task has been cancelled !");

            synchronized(this) {
                if(queue == null)
                    queue=new ConcurrentLinkedQueue<Runnable>();
            }
            queue.add(task);
        }

        void execute() {
            if(!cancelled.compareAndSet(false, true))
                return;
            try {
                task.run();
            }
            catch(Throwable t) {
                log.error("task execution failed", t);
            }

            if(queue != null) {
                for(Runnable tmp: queue) {
                    try {
                        tmp.run();
                    }
                    catch(Throwable t) {
                        log.error("task execution failed", t);
                    }
                }
            }
            done=true;
        }

        int size() {
            return 1 + (queue != null? queue.size() : 0);
        }

        public String toString() {
            return size() + " tasks";
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            return cancelled.compareAndSet(false, true);
        }

        public boolean isCancelled() {
            return cancelled.get();
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
    }


    private static class FixedDelayTask implements Runnable {
        private final Runnable task;
        private final TimeScheduler2 timer;
        private final long delay;

        public FixedDelayTask(Runnable task, TimeScheduler2 timer, long delay) {
            this.task=task;
            this.timer=timer;
            this.delay=delay;
        }

        public void run() {
            if(task != null) {
                task.run();
                timer.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        }
    }

   private class TaskWrapper<V> implements Runnable, Future<V> {
        private final Task          task;
        private volatile Future<?>  future; // cannot be null !
        private volatile boolean    cancelled=false;


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

//        public int compareTo(Delayed o) {
//            long my_delay=future.getDelay(TimeUnit.MILLISECONDS), their_delay=o.getDelay(TimeUnit.MILLISECONDS);
//            return my_delay < their_delay? -1 : my_delay > their_delay? 1 : 0;
//        }
//
//        public long getDelay(TimeUnit unit) {
//            return future != null? future.getDelay(unit) : -1;
//        }

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

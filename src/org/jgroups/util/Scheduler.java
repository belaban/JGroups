// $Id: Scheduler.java,v 1.19 2008/04/08 14:49:05 belaban Exp $

package org.jgroups.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Global;
import org.jgroups.annotations.Unsupported;


/**
 * Implementation of a priority scheduler. The scheduler maintains a queue to the end of which
 * all tasks are added. It continually looks at the first queue element, assigns a thread to
 * it, runs the thread and waits for completion. When a new <em>priority task</em> is added,
 * it will be added to the head of the queue and the scheduler will be interrupted. In this
 * case, the currently handled task is suspended, and the one at the head of the queue
 * handled. This is recursive: a priority task can always be interrupted by another priority
 * task.  Resursion ends when no more priority tasks are added, or when the thread pool is
 * exhausted.
 * @deprecated This class will be removed in 3.0
 * @author Bela Ban
 */
@Unsupported
public class Scheduler implements Runnable {
    final Queue              queue=new Queue();
    Thread             sched_thread=null;
    Task               current_task=null;
    ThreadPool         pool=null;
    SchedulerListener  listener=null;

    protected static final Log log=LogFactory.getLog(Scheduler.class);

    /** Process items on the queue concurrently. The default is to wait until the processing of an item
     * has completed before fetching the next item from the queue. Note that setting this to true
     * may destroy the properties of a protocol stack, e.g total or causal order may not be
     * guaranteed. Set this to true only if you know what you're doing ! */
    boolean            concurrent_processing=false;

    /** max number of threads, will only be allocated when needed */
    int                NUM_THREADS=128;

    static final int          WAIT_FOR_THREAD_AVAILABILITY=3000;






    public Scheduler() {
        String tmp=Util.getProperty(new String[]{Global.SCHEDULER_MAX_THREADS}, null, null, false, "128");
        this.NUM_THREADS=Integer.parseInt(tmp);
    }


    public Scheduler(int num_threads) {
        this.NUM_THREADS=num_threads;
    }


    public void setListener(SchedulerListener l) {
        listener=l;
    }


    public boolean getConcurrentProcessing() {
        return concurrent_processing;
    }

    public void setConcurrentProcessing(boolean process_concurrently) {
        this.concurrent_processing=process_concurrently;
    }

    public void run() {
        while(sched_thread != null) {
            if(queue.closed()) break;
            try {
                current_task=(Task)queue.peek(); // get the first task in the queue (blocks until available)
                if(current_task == null) { // @remove
                    if(log.isWarnEnabled()) log.warn("current task is null, queue.size()=" + queue.size() +
                            ", queue.closed()=" + queue.closed() + ", continuing");
                    continue;
                }

                if(current_task.suspended) {
                    current_task.suspended=false;
                    current_task.thread.resume();
                    if(listener != null) listener.resumed(current_task.target);
                }
                else {
                    if(current_task.thread == null) {
                        current_task.thread=pool.getThread();
                        if(current_task.thread == null) { // thread pool exhausted
                            if(log.isWarnEnabled()) log.warn("thread pool exhausted, waiting for " +
                                    WAIT_FOR_THREAD_AVAILABILITY + "ms before retrying");
                            Util.sleep(WAIT_FOR_THREAD_AVAILABILITY);
                            continue;
                        }
                    }

                    // if we get here, current_task.thread and current_task.target are guaranteed to be non-null
                    if(listener != null) listener.started(current_task.target);
                    if(current_task.thread.assignTask(current_task.target) == false)
                        continue;
                }

                /*
                 * http://jira.jboss.com/jira/browse/JGRP-690 
                 * Thread interrupt status is not always cleared by default
                 */
                if(Thread.interrupted())
                    throw new InterruptedException(); 

                if(concurrent_processing == false) { // this is the default: process serially
                    synchronized(current_task.thread) {
                        while(!current_task.thread.done() && !current_task.thread.suspended)
                            current_task.thread.wait();
                    }
                    if(listener != null) listener.stopped(current_task.target);
                }
                queue.removeElement(current_task);
            }
            catch(InterruptedException interrupted) {
                if(sched_thread == null || queue.closed()) break;
                if(current_task.thread != null) {
                    current_task.thread.suspend();
                    if(listener != null) listener.suspended(current_task.target);
                    current_task.suspended=true;
                }
            }
            catch(QueueClosedException closed_ex) {
                return;
            }
            catch(Throwable ex) {
                if(log.isErrorEnabled()) log.error("exception=" + Util.print(ex));
            }
        }
         if(log.isTraceEnabled()) log.trace("scheduler thread terminated");
    }


    public void addPrio(Runnable task) {
        Task new_task=new Task(task);
        boolean do_interrupt=false;

        try {
            synchronized(queue) { // sync against add()
                if(queue.size() == 0)
                    queue.add(new_task);
                else {
                    queue.addAtHead(new_task);
                    do_interrupt=true;
                }
            }
            if(do_interrupt) // moved out of 'synchronized(queue)' to minimize lock contention
                sched_thread.interrupt();
        }
        catch(Throwable e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
        }
    }


    public void add(Runnable task) {
        Task new_task=new Task(task);

        try {
            synchronized(queue) { // sync against addPrio()
                queue.add(new_task);
            }
        }
        catch(Exception e) {
            if(log.isErrorEnabled()) log.error("exception=" + e);
        }
    }


    public void start() {
        if(queue.closed())
            queue.reset();
        if(sched_thread == null) {
            pool=new ThreadPool(NUM_THREADS);
            sched_thread=new Thread(this, "Scheduler main thread");
            sched_thread.setDaemon(true);
            sched_thread.start();
        }
    }


    /**
     * Stop the scheduler thread. The thread may be waiting for its next task (queue.peek()) or it may be waiting on
     * the currently executing thread. In the first case, closing the queue will throw a QueueClosed exception which
     * terminates the scheduler thread. In the second case, after closing the queue, we interrupt the scheduler thread,
     * which then checks whether the queue is closed. If this is the case, the scheduler thread terminates.
     */
    public void stop() {
        Thread tmp=null;

        // 1. Close the queue
        queue.close(false); // will stop thread at next peek();

        // 2. Interrupt the scheduler thread
        if(sched_thread != null && sched_thread.isAlive()) {
            tmp=sched_thread;
            sched_thread=null;
            tmp.interrupt();
            try {
                tmp.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
            }
            catch(InterruptedException e) {
                Thread.currentThread().interrupt(); // set interrupt flag again
            }
            if(tmp.isAlive())
                if(log.isErrorEnabled()) log.error("scheduler thread is still not dead  !!!");
        }
        sched_thread=null;

        // 3. Delete the thread pool
        if(pool != null) {
            pool.destroy();
            pool=null;
        }
    }



    public static class Task {
        ReusableThread thread=null;
        Runnable target=null;
        boolean suspended=false;

        Task(Runnable target) {
            this.target=target;
        }

        public String toString() {
            return "[thread=" + thread + ", target=" + target + ", suspended=" + suspended + ']';
        }
    }


}

// $Id: Scheduler.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


import org.jgroups.log.Trace;


/**
 * Implementation of a priority scheduler. The scheduler maintains a queue to the end of which
 * all tasks are added. It continually looks at the first queue element, assigns a thread to
 * it, runs the thread and waits for completion. When a new <em>priority task</em> is added,
 * it will be added to the head of the queue and the scheduler will be interrupted. In this
 * case, the currently handled task is suspended, and the one at the head of the queue
 * handled. This is recursive: a priority task can always be interrupted by another priority
 * task.  Resursion ends when no more priority tasks are added, or when the thread pool is
 * exhausted.
 * @author Bela Ban
 */
public class Scheduler implements Runnable {
    Queue               queue=new Queue();
    Thread              sched_thread=null;
    Task                current_task=null;
    ThreadPool          pool=null;
    int                 NUM_THREADS=128; // max number, will only be allocated when needed
    final int           WAIT_FOR_THREAD_AVAILABILITY=3000;
    final int           THREAD_JOIN_TIMEOUT=1000;
    SchedulerListener   listener=null;
    boolean             trace=false;
    


    public class Task {
	ReusableThread   thread=null;
	Runnable         target=null;
	boolean          suspended=false;

	Task(Runnable target) {this.target=target;}

	public String toString() {
	    return "[thread=" + thread + ", target=" + target + ", suspended=" + suspended + "]";
	}
    }


    public Scheduler() {}


    public Scheduler(int num_threads) {this.NUM_THREADS=num_threads;}

    public Scheduler(int num_threads, boolean trace) {this(num_threads); this.trace=trace;}
    

    public void setListener(SchedulerListener l) {
	listener=l;
    }



    public void run() {
	while(sched_thread != null) {
	    if(queue.closed()) break;
	    try {
		current_task=(Task)queue.peek(); // get the first task in the queue

		if(current_task == null) { // @remove
		    Trace.error("Scheduler.run()", "current task is null, queue.size()=" + queue.size() +
				", queue.closed()=" + queue.closed() + ", continuing");
		    // System.exit(1);
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
			    Util.sleep(WAIT_FOR_THREAD_AVAILABILITY);
			    continue;
			}
			// The notification has to be *before* handling the message, otherwise 
			// the header will already have been removed !
			if(listener != null) listener.started(current_task.target);
			if(current_task.thread.assignTask(current_task.target) == false)
			    continue;
		    }
		    else {
			if(listener != null) listener.started(current_task.target);
			if(current_task.thread.assignTask(current_task.target) == false)
			    continue;
		    }
		}

		if(sched_thread.isInterrupted()) { // will continue at "catch(InterruptedException)" below
		    sched_thread.interrupt();
		}
		
		synchronized(current_task.thread) {
		    while(!current_task.thread.done() && !current_task.thread.suspended)
			current_task.thread.wait();
		}

		if(listener != null) listener.stopped(current_task.target);
		queue.removeElement(current_task);
	    }
	    catch(InterruptedException interrupted) {
		if(sched_thread == null || queue.closed()) break;
		if(current_task.thread != null) {
		    current_task.thread.suspend();
		    if(listener != null) listener.suspended(current_task.target);
		    current_task.suspended=true;
		}
		Thread.interrupted(); // clear the interrupt-flag
		continue;
	    }
	    catch(QueueClosedException closed_ex) {
		return;
	    }
	    catch(Throwable ex) {
		Trace.error("Scheduler.run()", "exception=" + Util.print(ex));
		continue;
	    }
	}
	if(Trace.trace) Trace.info("Scheduler.run()", "scheduler thread treminated");
    }



    
    public void addPrio(Runnable task) {
	Task    new_task=new Task(task);
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
	    Trace.error("Scheduler.addPrio()", "exception=" + e);
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
	    Trace.error("Scheduler.add()", "exception=" + e);
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
	    try {tmp.join(THREAD_JOIN_TIMEOUT);}
	    catch(Exception ex) {}
	    
	    if(tmp.isAlive())
		Trace.error("Scheduler.stop()", "scheduler thread is still not dead  !!!");
	}
	sched_thread=null;

	// 3. Delete the thread pool
	if(pool != null) {
	    pool.destroy();
	    pool=null;
	}
    }







    
}

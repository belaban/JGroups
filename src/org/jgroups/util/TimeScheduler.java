// $Id: TimeScheduler.java,v 1.2 2004/03/30 06:47:28 belaban Exp $

package org.jgroups.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;



/**
 * Fixed-delay & fixed-rate single thread scheduler
 * <p>
 * The scheduler supports varying scheduling intervals by asking the task
 * every time for its next preferred scheduling interval. Scheduling can
 * either be <i>fixed-delay</i> or <i>fixed-rate</i>. The notions are
 * borrowed from <tt>java.util.Timer</tt> and retain the same meaning.
 * I.e. in fixed-delay scheduling, the task's new schedule is calculated
 * as:<br>
 * new_schedule = time_task_starts + scheduling_interval
 * <p>
 * In fixed-rate scheduling, the next schedule is calculated as:<br>
 * new_schedule = time_task_was_supposed_to_start + scheduling_interval
 * <p>
 * The scheduler internally holds a queue of tasks sorted in ascending order
 * according to their next execution time. A task is removed from the queue
 * if it is cancelled, i.e. if <tt>TimeScheduler.Task.isCancelled()</tt>
 * returns true.
 * <p>
 * The scheduler internally uses a <tt>java.util.SortedSet</tt> to keep tasks
 * sorted. <tt>java.util.Timer</tt> uses an array arranged as a binary heap
 * that doesn't shrink. It is likely that the latter arrangement is faster.
 * <p>
 * Initially, the scheduler is in <tt>SUSPEND</tt>ed mode, <tt>start()</tt>
 * need not be called: if a task is added, the scheduler gets started
 * automatically. Calling <tt>start()</tt> starts the scheduler if it's
 * suspended or stopped else has no effect. Once <tt>stop()</tt> is called,
 * added tasks will not restart it: <tt>start()</tt> has to be called to
 * restart the scheduler.
 */
public class TimeScheduler {
	/**
	 * The interface that submitted tasks must implement
	 */
	public interface Task {
		/**
		 * @return true if task is cancelled and shouldn't be scheduled
		 * again
		 */
		boolean cancelled();
		/** @return the next schedule interval */
		long nextInterval();
		/** Execute the task */
		void run();
	}


	/**
	 * Internal task class.
	 */
	private static class IntTask implements Comparable {
		/** The user task */
		public Task task;
		/** The next execution time */
		public long sched;
		/** Whether this task is scheduled fixed-delay or fixed-rate */
		public boolean relative;

		/**
		 * @param task the task to schedule & execute
		 * @param sched the next schedule
		 * @param relative whether scheduling for this task is soft or hard
		 * (see <tt>TimeScheduler.add()</tt>)
		 */
		public IntTask(Task task, long sched, boolean relative) {
			this.task     = task;
			this.sched    = sched;
			this.relative = relative;
		}

		/**
		 * @param obj the object to compare against
		 *
		 * <pre>
		 * If obj is not instance of <tt>IntTask</tt>, then return -1
		 * If obj is instance of <tt>IntTask</tt>, compare the
		 * contained tasks' next execution times. If these times are equal,
		 * then order them randomly <b>but</b> consistently!: return the diff
		 * of their <tt>hashcode()</tt> values
		 * </pre>
		 */
		public int compareTo(Object obj) {
			IntTask other;

			if (!(obj instanceof IntTask)) return(-1);

			other = (IntTask)obj;
			if (sched < other.sched) return(-1);
			if (sched > other.sched) return(1);
			return(task.hashCode()-other.task.hashCode());
		}

	        public String toString() {
		    if(task == null) return "<unnamed>";
		    else return task.getClass().getName();
		}
	}


	/**
	 * The scheduler thread's main loop
	 */
	private class Loop implements Runnable { public void run() { _run(); } }


	/**
	 * The task queue used by the scheduler. Tasks are ordered in increasing
	 * order of their next execution time
	 */
	private static class TaskQueue {
		/** Sorted list of <tt>IntTask</tt>s */
		private SortedSet set;

		public TaskQueue() { super();
			set = new TreeSet();
		}

		public void add(IntTask t) { set.add(t); }

		public IntTask getFirst() { return((IntTask)set.first()); }

		public void removeFirst() {
			Iterator it = set.iterator();
			IntTask t = (IntTask)it.next();
			it.remove();
		}

		public void rescheduleFirst(long sched) {
			Iterator it  = set.iterator();
			IntTask t = (IntTask)it.next();
			it.remove();
			t.sched = sched;
			set.add(t);
		}

		public boolean isEmpty() { return(set.isEmpty()); }

		public void clear() { set.clear(); }

	        public int size() { return set.size(); }

	        public String toString() {
		    return set.toString();
		}
	}


	/** Default suspend interval (ms) */
	private static final long SUSPEND_INTERVAL = 2000;
	/**
	 * Regular wake-up intervals for scheduler, in case all tasks have been
	 * cancelled and we are still waiting on the schedule time of the task
	 * at the top
	 */
	private static final long TICK_INTERVAL = 1000;

	/**
	 * Thread is running
	 * <p>
	 * A call to <code>start()</code> has no effect on the thread<br>
	 * A call to <code>stop()</code> will stop the thread<br>
	 * A call to <code>add()</code> has no effect on the thread
	 */
	private static final int RUN = 0;
	/**
	 * Thread is suspended
	 * <p>
	 * A call to <code>start()</code> will recreate the thread<br>
	 * A call to <code>stop()</code> will switch the state from suspended
	 * to stopped<br>
	 * A call to <code>add()</code> will recreate the thread <b>only</b>
	 * if it is suspended
	 */
	private static final int SUSPEND = 1;
	/**
	 * A shutdown of the thread is in progress
	 * <p>
	 * A call to <code>start()</code> has no effect on the thread<br>
	 * A call to <code>stop()</code> has no effect on the thread<br>
	 * A call to <code>add()</code> has no effect on the thread<br>
	 */
	private static final int STOPPING = 2;
	/**
	 * Thread is stopped
	 * <p>
	 * A call to <code>start()</code> will recreate the thread<br>
	 * A call to <code>stop()</code> has no effect on the thread<br>
	 * A call to <code>add()</code> has no effect on the thread<br>
	 */
	private static final int STOP = 3;

	/** TimeScheduler thread name */
	private static final String THREAD_NAME = "TimeScheduler.Thread";


	/** The scheduler thread */
	private Thread thread = null;
	/** The thread's running state */
	private int thread_state = SUSPEND;
	/**
	 * Time that task queue is empty before suspending the scheduling
	 * thread
	 */
	private long suspend_interval = SUSPEND_INTERVAL;
	/** The task queue ordered according to task's next execution time */
	private TaskQueue queue;

    protected static Log log=LogFactory.getLog(TimeScheduler.class);



	/**
	 * Convert exception stack trace to string
	 */
	private String _toString(Throwable ex) {
		StringWriter sw = new StringWriter();
		PrintWriter  pw = new PrintWriter(sw);
		ex.printStackTrace(pw);
		return(sw.toString());
	}


	/** Set the thread state to running, create and start the thread */
	private void _start() {
        thread_state = RUN;

        // only start if not yet running
        if(thread == null || !thread.isAlive()) {
            thread = new Thread(new Loop(), THREAD_NAME);
            thread.setDaemon(true);
            thread.start();
        }
	}

	/** Restart the suspended thread */
	private void _unsuspend() {
		thread_state = RUN;

        // only start if not yet running
        if(thread == null || !thread.isAlive()) {
            thread = new Thread(new Loop(), THREAD_NAME);
            thread.setDaemon(true);
            thread.start();
        }
	}

	/** Set the thread state to suspended */
	private void _suspend() {
		thread_state = SUSPEND;
		thread = null;
	}

	/** Set the thread state to stopping */
	private void _stopping() {
		thread_state = STOPPING;
	}

	/** Set the thread state to stopped */
	private void _stop() {
		thread_state = STOP;
		thread = null;
	}


	/**
	 * If the task queue is empty, sleep until a task comes in or if slept
	 * for too long, suspend the thread.
	 * <p>
	 * Get the first task, if the running time hasn't been
	 * reached then wait a bit and retry. Else reschedule the task and then
	 * run it.
	 */
	private void _run() {
		IntTask intTask;
		Task    task;
		long    currTime, execTime, waitTime, intervalTime, schedTime;

		while(true) {
			synchronized(this) {
			    if (thread == null || thread.isInterrupted()) return;
			}

			synchronized(queue) {
			    while(true) {
				if (!queue.isEmpty()) break;
				try { queue.wait(suspend_interval);
				} catch(InterruptedException ex) { return; }
				if (!queue.isEmpty()) break;
				_suspend();
				return;
			    }

			    intTask = queue.getFirst();
			    synchronized(intTask) {
				task = intTask.task;
				if (task.cancelled()) {
				    queue.removeFirst();
				    continue;
				}
				currTime = System.currentTimeMillis();
				execTime = intTask.sched;
				if ((waitTime = execTime - currTime) <= 0) {
				// Reschedule the task
				    intervalTime = task.nextInterval();
				    schedTime = intTask.relative?
					currTime+intervalTime : execTime+intervalTime;
				    queue.rescheduleFirst(schedTime);
				}
			    }
			    if (waitTime > 0) {
				//try { queue.wait(Math.min(waitTime, TICK_INTERVAL));
				try { queue.wait(waitTime);
				} catch(InterruptedException ex) { return; }
				continue;
			    }
			}

			try { task.run();
			} catch(Exception ex) {
                log.error(_toString(ex));
			}
		}
	}


	/**
	 * Create a scheduler that executes tasks in dynamically adjustable
	 * intervals
	 *
	 * @param suspend_interval the time that the scheduler will wait for
	 * at least one task to be placed in the task queue before suspending
	 * the scheduling thread
	 */
	public TimeScheduler(long suspend_interval) {
	    super();
	    queue = new TaskQueue();
	    this.suspend_interval = suspend_interval;
	}

	/**
	 * Create a scheduler that executes tasks in dynamically adjustable
	 * intervals
	 */
	public TimeScheduler() { this(SUSPEND_INTERVAL); }


	/**
	 * Add a task for execution at adjustable intervals
	 *
	 * @param t the task to execute
	 *
	 * @param relative scheduling scheme:
	 * <p>
	 * <tt>true</tt>:<br>
	 * Task is rescheduled relative to the last time it <i>actually</i>
	 * started execution
	 * <p>
	 * <tt>false</tt>:<br>
	 * Task is scheduled relative to its <i>last</i> execution schedule. This
	 * has the effect that the time between two consecutive executions of
	 * the task remains the same.
	 */
	public void add(Task t, boolean relative) {
		long interval, sched;

		if ((interval = t.nextInterval()) < 0) return;
		sched = System.currentTimeMillis() + interval;

		synchronized(queue) {
		    queue.add(new IntTask(t, sched, relative));
		    switch(thread_state) {
		    case RUN: queue.notifyAll(); break;
		    case SUSPEND: _unsuspend(); break;
		    case STOPPING: break;
		    case STOP: break;
		    }
		}
	}

	/**
	 * Add a task for execution at adjustable intervals
	 *
	 * @param t the task to execute
	 */
	public void add(Task t) { add(t, true); }


	/**
	 * Start the scheduler, if it's suspended or stopped
	 */
	public void start() {
		synchronized(queue) {
		    switch(thread_state) {
		    case RUN: break;
		    case SUSPEND: _unsuspend(); break;
		    case STOPPING: break;
		    case STOP: _start(); break;
		    }
		}
	}


	/**
	 * Stop the scheduler if it's running. Switch to stopped, if it's
	 * suspended. Clear the task queue.
	 *
	 * @throws InterruptedException if interrupted while waiting for thread
	 * to return
	 */
    public void stop() throws InterruptedException {
        // i. Switch to STOPPING, interrupt thread
        // ii. Wait until thread ends
        // iii. Clear the task queue, switch to STOPPED,
        synchronized(queue) {
            switch(thread_state) {
                case RUN:
                    _stopping();
                    break;
                case SUSPEND:
                    _stop();
                    return;
                case STOPPING:
                    return;
                case STOP:
                    return;
            }
            thread.interrupt();
        }

        thread.join();

        synchronized(queue) {
            queue.clear();
            _stop();
        }
    }
}

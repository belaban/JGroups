// $Id: ReusableThread.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.util;


import org.jgroups.log.Trace;


/**
   Reusable thread class. Instead of creating a new thread per task, this instance can be reused
   to run different tasks in turn. This is done by looping and assigning the Runnable task objects
   whose <code>run</code> method is then called.<br>
   Tasks are Runnable objects and should be prepared to terminate when they receive an
   InterruptedException. This is thrown by the stop() method.<br>

   The following situations have to be tested:
   <ol>
   <li>ReusableThread is started. Then, brefore assigning a task, it is stopped again
   <li>ReusableThread is started, assigned a long running task. Then, before task is done,
       stop() is called
   <li>ReusableThread is started, assigned a task. Then waitUntilDone() is called, then stop()
   <li>ReusableThread is started, assigned a number of tasks (waitUntilDone() called between tasks),
       then stopped
   <li>ReusableThread is started, assigned a task
   </ol>
   @author Bela Ban
*/
public class ReusableThread implements Runnable {
    volatile Thread    thread=null;  // thread that works on the task
    Runnable           task=null;    // task assigned to thread
    String             thread_name="ReusableThread";
    volatile boolean   suspended=false;
    long               TASK_JOIN_TIME=3000; // wait 3 secs for an interrupted thread to terminate



    public ReusableThread() {
    }


    public ReusableThread(String thread_name) {
	this.thread_name=thread_name;
    }



    public boolean done()                   {return task == null;}
    public boolean available()              {return done();}




    public void start() {
	synchronized(this) {
	    if(thread == null) {
		thread=new Thread(this, thread_name);
		thread.setDaemon(true);
		thread.start();
	    }
	}
    }


    /** Stops the thread by setting thread=null and interrupting it. The run() method catches the
	InterruptedException and checks whether thread==null. If this is the case, it will terminate */
    public void stop() {
	Thread  tmp=null;
	boolean ret=true;

	if(Trace.debug) Trace.info("ReusableThread.stop()", "entering THIS");
	synchronized(this) {
	    if(Trace.debug)
		Trace.info("ReusableThread.stop()", "entered THIS (thread=" + printObj(thread) +
			   ", task=" + printObj(task) + ", suspended=" + suspended + ")");
	    if(thread != null && thread.isAlive()) {
		tmp=thread;
		thread=null; // signals the thread to stop
		task=null;
		if(Trace.debug) Trace.info("ReusableThread.stop()", "notifying thread");
		notifyAll();
		if(Trace.debug) Trace.info("ReusableThread.stop()", "notifying thread completed");
	    }
	    thread=null;
	    task=null;
	}

	if(tmp != null && tmp.isAlive()) {
	    long s1=System.currentTimeMillis(), s2=0;
	    if(Trace.debug) Trace.info("ReusableThread.stop()", "join(" + TASK_JOIN_TIME + ")");
	    try {tmp.join(TASK_JOIN_TIME);} 
	    catch(Exception e) {
	    }
	    s2=System.currentTimeMillis();
	    if(Trace.debug) Trace.info("ReusableThread.stop()", "join(" + TASK_JOIN_TIME + 
				       ") completed in " + (s2-s1));
	    if(tmp.isAlive())
		Trace.error("ReusableThread.stop()", "thread is still alive");
	    tmp=null;
	}
    }



    
    /** Suspends the thread. Does nothing if already suspended. If a thread is waiting to be assigned a task, or
	is currently running a (possibly long-running) task, then it will be suspended the next time it
	waits for suspended==false (second wait-loop in run())  */

    public void suspend() {
	synchronized(this) {
	    if(Trace.debug) Trace.info("ReusableThread.suspend()", "suspended=" + suspended + ", task=" + printObj(task));
	    if(suspended)
		return; // already suspended
	    else
		suspended=true;
	}
    }


    /** Resumes the thread. Noop if not suspended */
    public void resume() {
	synchronized(this) {
	    suspended=false;
	    notifyAll();  // notifies run(): the wait on suspend() is released
	}
    }




    /**
     * Assigns a task to the thread. If the thread is not running, it will be started. It it is
     * already working on a task, it will reject the new task. Returns true if task could be
     * assigned auccessfully
     */
    public boolean assignTask(Runnable t) {
	synchronized(this) {
	    start(); // creates and starts the thread if not yet running
	    if(task == null) {
		task=t;
		notifyAll();	    // signals run() to start working (first wait-loop)
		return true;
	    }
	    else {
		Trace.error("ReusableThread.assignTask()",
			    "already working on a thread: current_task=" + task + ", new task=" + t +
			    ", thread=" + thread + ", is alive=" + (thread != null? "" + thread.isAlive() : "null"));
		return false;
	    }
	}
    }



    /**
       Delicate piece of code (means very important :-)). Works as follows: loops until stop is true.
       Waits in a loop until task is assigned. Then runs the task and notifies waiters that it's done
       when task is completed. Then returns to the first loop to wait for more work. Does so until
       stop() is called, which sets stop=true and interrupts the thread. If waiting for a task, the
       thread terminates. If running a task, the task is interrupted, and the thread terminates. If the
       task is not interrupible, the stop() method will wait for 3 secs (join on the thread), then return.
       This means that the run() method of the task will complete and only then will the thread be
       garbage-collected.
    */
    public void run() {
	while(thread != null) {     // Stop sets thread=null
	    try {
		if(Trace.debug) Trace.info("ReusableThread.run()", "entering ASSIGN");
		synchronized(this) {
		    if(Trace.debug) Trace.info("ReusableThread.run()", "entered ASSIGN (task=" +
					       printObj(task) + ", thread=" + printObj(thread) + ")");

		    while(task == null &&
			  thread != null) {   // first wait-loop: wait for task to be assigned (assignTask())
			if(Trace.debug) Trace.info("ReusableThread.run()", "wait ASSIGN");
			wait();
			if(Trace.debug) Trace.info("ReusableThread.run()", "wait ASSIGN completed");
		    }
		}
	    }
	    catch(InterruptedException ex) {  // on assignTask()
		if(Trace.debug) Trace.info("ReusableThread.run()", "interrupt on ASSIGN");
	    }
	    if(thread == null) return;  // we need to terminate
	    	    
	    try {
		if(Trace.debug) Trace.info("ReusableThread.run()", "entering SUSPEND");
		synchronized(this) {
		    if(Trace.debug) Trace.info("ReusableThread.run()", "entered SUSPEND (suspended=" +
					       suspended + ", task=" + printObj(task) + ")");
		    while(suspended && 
			  thread != null) {        // second wait-loop: wait for thread to resume (resume())
			if(Trace.debug) Trace.info("ReusableThread.run()", "wait SUSPEND");
			wait();
			if(Trace.debug) Trace.info("ReusableThread.run()", "wait SUSPEND completed");
		    }
		}
	    }
	    catch(InterruptedException ex) {  // on resume()
		if(Trace.debug) Trace.info("ReusableThread.run()", "interrupt on RESUME");
	    }
	    if(thread == null) return; // we need to terminate


	    if(task != null) {
		if(Trace.debug) Trace.info("ReusableThread.run()", "running task");
		try {
		    task.run();
		}
		catch(Throwable ex) {
		    Trace.error("ReusableThread().run()", "exception=" + Util.printStackTrace(ex));
		}
		if(Trace.debug) Trace.info("ReusableThread.run()", "task completed");
	    }

	    if(Trace.debug) Trace.info("ReusableThread.run()", "entering THIS");
	    synchronized(this) {
		if(Trace.debug) Trace.info("ReusableThread.run()", "entered THIS");
		task=null;
		if(Trace.debug) Trace.info("ReusableThread.run()", "notify THIS");
		notifyAll();
		if(Trace.debug) Trace.info("ReusableThread.run()", "notify THIS completed");
	    }
	}
	if(Trace.debug) Trace.info("ReusableThread.run()", "terminated");
    }





    String printObj(Object obj) {
	if(obj == null)
	    return "null";
	else
	    return "non-null";
    }





    public void waitUntilDone() {

	if(Trace.debug) Trace.info("ReusableThread.waitUntilDone()", "entering THIS");
	synchronized(this) {
	    if(Trace.debug) Trace.info("ReusableThread.waitUntilDone()", "entered THIS (task=" + printObj(task) + ")");
	    while(task != null) {
		try {
		    if(Trace.debug) Trace.info("ReusableThread.waitUntilDone()", "wait THIS");
		    wait();
		    if(Trace.debug) Trace.info("ReusableThread.waitUntilDone()", "wait THIS completed");
		}
		catch(InterruptedException interrupted) {}
	    }
	}
    }

    


    public String toString() {
	return "suspended=" + suspended;
    }


}








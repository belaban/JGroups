// $Id: ReusableThreadTest.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $

package org.jgroups.tests;

import junit.framework.*;
import org.jgroups.util.*;


public class ReusableThreadTest extends TestCase {


    public ReusableThreadTest(String name) {
	super(name);
    }


	
    static class MyThread implements Runnable {
	    int    num=0;
	    String name=null;
	    
	    public MyThread(int num) {this.num=num; this.name="Thread #" + num;}
	    
	    public void run() {
		
		System.out.println("Thread " + name + " is run");
		
		long sleep_time=(long)(Math.random() * 5000);
		System.out.print("Thread #" + num + ": sleeping " + sleep_time + ":");
		Util.sleep(sleep_time);
		//throw new Error("hello world");
		System.out.println(" -- done");
	    }
	}




    static class LongRunningThread extends MyThread {
	    long sleep_time=10000;
	    
	    
	    public LongRunningThread(int num) {super(num); name="LongRunningThread #" + num;}
	    
	    public void run() {
		System.out.println("LongRunningThread " + name + " is run");
		System.out.print("LongRunningThread #" + num + ": sleeping " + sleep_time + " msecs");
		Util.sleep(sleep_time);		
		//System.out.println("LongRunningThread -- done");
	    }
	}



    public void testReusableThread() {
	ReusableThread t=new ReusableThread("Demo ReusableThread");
	// t.start();

	MyThread           m1=new MyThread(1);
	MyThread           m2=new MyThread(2);
	MyThread           m3=new MyThread(3);
	MyThread           m5=new MyThread(5);
	MyThread           m6=new MyThread(6);
	MyThread           m7=new MyThread(7);
	MyThread           m8=new MyThread(8);


	LongRunningThread  m4=new LongRunningThread(4);


	// Util.sleep(3000);
	// t.stop();

	// t.assignTask(m4);
	// t.waitUntilDone();
	// t.stop();



	System.out.println("Assigning task");
	t.assignTask(m4);

	System.out.println("Sleeping 2 secs");
	Util.sleep(2000);

	System.out.println("stop()");
	t.stop();
	System.out.println("stop() -- done");


		
	Util.printThreads();
	
	System.out.println("\nAssigning task 1");
	t.assignTask(m1);
	t.waitUntilDone();  // passive wait
	System.out.println("done with task 1");
	
	Util.printThreads();

	System.out.println("\nAssigning task 2");
	t.assignTask(m2);
	t.waitUntilDone();
	System.out.println("done with task 2");

	System.out.println("Stopping thread");
	t.stop();
	System.out.println("done");

	Util.printThreads();
    }





    public static void main(String[] args) {
        String[] testCaseName = {ReusableThreadTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}

// $Id: SchedulerTest.java,v 1.2 2004/03/30 06:47:31 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.util.Scheduler;
import org.jgroups.util.SchedulerListener;
import org.jgroups.util.Util;




public class SchedulerTest extends TestCase {

    static class MyThread implements Runnable {
	String name;
	
	MyThread(String name) {this.name=name;}
	
	public void run() {
	    long sleep_time=(long)(Math.random() * 1000);
	    System.out.println("\n--> " + name + ": sleeping for " + sleep_time + " ms");
	    Util.sleep(sleep_time);
	    System.out.println("--> " + name + ": Done");
	}
	
	public String toString() {return "MyThread [name=" + name + "]";}
	
    }
    
    
    static class Listener implements SchedulerListener {
	public void started(Runnable t) {
	    System.out.println("--> Started: " + t);
	}
	public void stopped(Runnable t) {
	    System.out.println("--> Stopped: " + t);
	}
	public void suspended(Runnable t) {
	    System.out.println("--> Suspended: " + t);
	}
	public void resumed(Runnable t) {
	    System.out.println("--> Resumed: " + t);
	}
    }
    

    public SchedulerTest(String name) {
	super(name);
    }


    public void testScheduler() throws Exception {
	Scheduler sch=new Scheduler();
	sch.setListener(new Listener());
	sch.add(new MyThread("Bela"));
	sch.add(new MyThread("Janet"));
	sch.add(new MyThread("Ralph"));
	sch.start();
	sch.add(new MyThread("Frank"));
	sch.add(new MyThread("Marco"));

	Util.sleep(1000);
	sch.addPrio(new MyThread("Gabi"));
	sch.add(new MyThread("Rolf"));
	Util.sleep(100);
	sch.addPrio(new MyThread("Gabi2"));
	Util.sleep(100);
	sch.addPrio(new MyThread("Gabi3"));
	Util.sleep(100);
	sch.addPrio(new MyThread("Gabi4"));
	Util.sleep(1000);  // increase this if you want to see all thread running (and possibly completing)	
	sch.stop();
    }

    public static void main(String[] args) {
        String[] testCaseName = {SchedulerTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}

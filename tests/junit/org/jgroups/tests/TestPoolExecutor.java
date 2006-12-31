package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.util.Util;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Test playground for java.util.concurrent.ThreadPoolExecutor
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */

public class TestPoolExecutor extends TestCase{
    static int count=0;
    private final Object poolLock = new Object();
 
    public void testPool() throws InterruptedException {
    	long keepAlive = 30*1000;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 5, keepAlive, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(10));
        executor.setThreadFactory(new ThreadFactory() {
            public Thread newThread(final Runnable command) {
            	synchronized (poolLock) {
                    count++;
                }
                return new Thread("poolid=" +count) {
                	
                    public void run() { 
                    	 System.out.println("Thread " + Thread.currentThread() + " started");
                        command.run();
                        System.out.println("Thread " + Thread.currentThread() + " stopped");
                    }
                };
            }
        });
 
        for (int i = 0; i < 30; i++) {
            final int cnt = i;
            executor.execute(new Runnable() {
                public void run() {
                    System.out.println("Runnable " + cnt + " running");
                    //use timing here that approximates how long 
                    //this thread needs to run
                    Util.sleep(3000);
                }
            });
            
            //use timing here that approximates time 
            //between tasks arriving
            Util.sleep(1000);
        }
 
        executor.shutdown();
        //see if all threads are stop/recycled
        Util.sleep(keepAlive);
    }

	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp();
	}

	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		super.tearDown();
	}
	public static Test suite() {		  
	      return new TestSuite(TestPoolExecutor.class);
	}
	 
	public static void main(String[] args) {		
		 String[] testCaseName={TestPoolExecutor.class.getName()};
	     junit.textui.TestRunner.main(testCaseName);
	}
}
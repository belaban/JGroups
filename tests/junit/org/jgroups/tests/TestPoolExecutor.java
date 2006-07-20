package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jgroups.util.Util;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.ThreadFactory;
/**
 * Test playground for util.concurrent.PooledExecutor
 * 
 * 
 * @author Vladimir Blagojevic
 * @version $Id$ 
 *
 */

public class TestPoolExecutor extends TestCase{
    static int count;
    private final Object poolLock = new Object();
 
    public void testPool() throws InterruptedException {
    	long keepAlive = 30*1000;
        PooledExecutor executor = new PooledExecutor(5);
        executor.setMinimumPoolSize(1);
        executor.waitWhenBlocked();
        executor.setKeepAliveTime(keepAlive);
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
            final int count = i;
            executor.execute(new Runnable() {
                public void run() {
                    System.out.println("Runnable " + count + " running");
                    //use timing here that approximates how long 
                    //this thread needs to run
                    Util.sleep(3000);
                }
            });
            
            //use timing here that approximates time 
            //between tasks arriving
            Util.sleep(1000);
        }
 
        executor.shutdownAfterProcessingCurrentlyQueuedTasks(); 
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
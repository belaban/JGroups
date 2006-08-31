package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import EDU.oswego.cs.dl.util.concurrent.ReentrantLock;
import EDU.oswego.cs.dl.util.concurrent.Mutex;
import EDU.oswego.cs.dl.util.concurrent.Semaphore;


/**
 * Tests the ReentrantLock
 * @author Bela Ban
 * @version $Id: ReentrantLockTest.java,v 1.1 2006/08/31 14:08:32 belaban Exp $
 */
public class ReentrantLockTest extends TestCase {
    ReentrantLock lock;

    public ReentrantLockTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        lock=new ReentrantLock();
    }

    public void tearDown() throws Exception {
        releaseAll(lock);
        lock=null;
        super.tearDown();
    }



    public void testAcquireLock() {
        try {
            lock.acquire();
            assertEquals(1, lock.holds());
            lock.acquire();
            assertEquals(2, lock.holds());
            release(lock);
            assertEquals(1, lock.holds());
            release(lock);
            assertEquals(0, lock.holds());
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testAcquireLock2() {
        try {
            lock.acquire();
            assertEquals(1, lock.holds());
            lock.acquire();
            assertEquals(2, lock.holds());
            releaseAll(lock);
            assertEquals(0, lock.holds());
        }
        catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void release(ReentrantLock lock) {
        if(lock != null && lock.holds() > 0)
            lock.release();
    }

    private void releaseAll(ReentrantLock lock) {
        if(lock != null) {
            long holds=lock.holds();
            if(holds > 0)
                lock.release(holds);
        }
    }



    public static Test suite() {
        return new TestSuite(ReentrantLockTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ReentrantLockTest.suite());
    }
}

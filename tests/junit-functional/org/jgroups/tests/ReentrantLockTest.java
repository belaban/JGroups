package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.concurrent.locks.ReentrantLock;


/**
 * Tests the ReentrantLock
 * @author Bela Ban
 * @version $Id: ReentrantLockTest.java,v 1.1 2007/07/04 07:29:34 belaban Exp $
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
        lock.lock();
        assertEquals(1, lock.getHoldCount());
        lock.lock();
        assertEquals(2, lock.getHoldCount());
        release(lock);
        assertEquals(1, lock.getHoldCount());
        release(lock);
        assertEquals(0, lock.getHoldCount());
    }

    public void testAcquireLock2() {
        lock.lock();
        assertEquals(1, lock.getHoldCount());
        lock.lock();
        assertEquals(2, lock.getHoldCount());
        releaseAll(lock);
        assertEquals(0, lock.getHoldCount());
    }

    private void release(ReentrantLock lock) {
        if(lock != null && lock.getHoldCount() > 0)
            lock.unlock();
    }

    private void releaseAll(ReentrantLock lock) {
        if(lock != null) {
            long holds=lock.getHoldCount();
            if(holds > 0) {
                for(int i=0; i < holds; i++)
                    lock.unlock();
            }
        }
    }



    public static Test suite() {
        return new TestSuite(ReentrantLockTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ReentrantLockTest.suite());
    }
}

package org.jgroups.tests;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.jgroups.Global;

import java.util.concurrent.locks.ReentrantLock;


/**
 * Tests the ReentrantLock
 * @author Bela Ban
 * @version $Id: ReentrantLockTest.java,v 1.4 2008/04/08 12:41:58 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ReentrantLockTest {
    ReentrantLock lock;


    @BeforeMethod
    public void setUp() throws Exception {
        lock=new ReentrantLock();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        releaseAll(lock);
        lock=null;
    }


    public void testAcquireLock() {
        lock.lock();
        Assert.assertEquals(1, lock.getHoldCount());
        lock.lock();
        Assert.assertEquals(2, lock.getHoldCount());
        release(lock);
        Assert.assertEquals(1, lock.getHoldCount());
        release(lock);
        Assert.assertEquals(0, lock.getHoldCount());
    }


    public void testAcquireLock2() {
        lock.lock();
        Assert.assertEquals(1, lock.getHoldCount());
        lock.lock();
        Assert.assertEquals(2, lock.getHoldCount());
        releaseAll(lock);
        Assert.assertEquals(0, lock.getHoldCount());
    }

    private static void release(ReentrantLock lock) {
        if(lock != null && lock.getHoldCount() > 0)
            lock.unlock();
    }

    private static void releaseAll(ReentrantLock lock) {
        if(lock != null) {
            long holds=lock.getHoldCount();
            if(holds > 0) {
                for(int i=0; i < holds; i++)
                    lock.unlock();
            }
        }
    }

}

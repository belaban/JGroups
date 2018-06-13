package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Tests current access to the locks provided by {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups={Global.BYTEMAN,Global.EAP_EXCLUDED},singleThreaded=true)
public class LockServiceConcurrencyTest extends BMNGRunner {
    protected JChannel    ch;
    protected LockService lock_service;

    @BeforeMethod protected void init() throws Exception {
        ch=new JChannel(Util.getTestStack(new CENTRAL_LOCK())).name("A");
        lock_service=new LockService(ch);
        ch.connect("LockServiceConcurrencyTest");
    }

    @AfterMethod protected void destroy() {
        lock_service.unlockAll();
        Util.close(ch);
    }

    /** Tests JIRA https://issues.jboss.org/browse/JGRP-1679 */
    @BMScript(dir="conf/scripts/LockServiceConcurrencyTest", value="testConcurrentClientLocks")
    public void testConcurrentClientLocks() throws InterruptedException {
        Lock lock=lock_service.getLock("L");

        // we're suppressing (via byteman) the LOCK-GRANTED response (until the rendezvous later), so
        // this lock acquisition must fail
        boolean success=lock.tryLock(1, TimeUnit.MILLISECONDS);
        assert !success : "the lock acquisition should have failed";


        // the spurious LOCK-GRANTED response is received, so the lock can be acquired successfully, which is incorrect
        // as we're dropping the LOCK-GRANTED request
        // tryLock() works the same, with or without timeout
        success=lock.tryLock(10, TimeUnit.MILLISECONDS); // timeout needs to be greater than 1 (byteman rule fires on this) !
        assert !success : "lock was acquired successfully - this is incorrect";
    }
}

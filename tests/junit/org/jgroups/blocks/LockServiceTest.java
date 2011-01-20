package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/** Tests {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class LockServiceTest extends ChannelTestBase {
    protected JChannel c1, c2, c3;
    protected LockService s1, s2, s3;
    protected Lock lock;
    protected static final String LOCK="sample-lock";


    @BeforeClass
    protected void init() throws Exception {
        c1=createChannel(true, 3, "A");
        addLockingProtocol(c1);
        s1=new LockService(c1);
        c1.connect("LockServiceTest");

        c2=createChannel(c1, "B");
        s2=new LockService(c2);
        c2.connect("LockServiceTest");

        c3=createChannel(c1, "C");
        s3=new LockService(c3);
        c3.connect("LockServiceTest");
        
        lock=s1.getLock(LOCK);
    }


    @AfterClass
    protected void cleanup() {
        s3.unlockAll();
        s2.unlockAll();
        s1.unlockAll();
        Util.close(c3,c2,c1);
    }


    public void testSimpleLock() {
        lock(lock, LOCK);
        unlock(lock, LOCK);
    }

    public void testLockingOfAlreadyAcquiredLock() {
        lock(lock, LOCK);
        lock(lock, LOCK);
        unlock(lock, LOCK);
    }

    public void testUnsuccessfulTryLock() {
        Lock lock2=s2.getLock(LOCK);
        lock(lock2, LOCK);
        try {
            boolean rc=tryLock(lock, LOCK);
            assert !rc;
            unlock(lock, LOCK);
        }
        finally {
            unlock(lock2, LOCK);
        }
    }

    public void testUnsuccessfulTryLockTimeout() throws InterruptedException {
        Lock lock2=s2.getLock(LOCK);
        lock(lock2, LOCK);
        try {
            boolean rc=tryLock(lock, 1000, LOCK);
            assert !rc;
            unlock(lock, LOCK);
        }
        finally {
            unlock(lock2, LOCK);
        }
    }

    public void testSuccessfulTryLockTimeout() throws InterruptedException, BrokenBarrierException {
        final CyclicBarrier barrier=new CyclicBarrier(2);

        Thread locker=new Locker(barrier);
        locker.start();
        barrier.await();
        boolean rc=tryLock(lock, 10000, LOCK);
        assert rc;
    }

    
    protected class Locker extends Thread {
        protected final CyclicBarrier barrier;

        public Locker(CyclicBarrier barrier) {
            this.barrier=barrier;
        }

        public void run() {
            lock(lock, LOCK);
            try {
                barrier.await();
                Util.sleep(500);
            }
            catch(Exception e) {
            }
            finally {
                unlock(lock, LOCK);
            }
        }
    }


    protected static void lock(Lock lock, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] locking " + name);
        lock.lock();
        System.out.println("[" + Thread.currentThread().getId() + "] locked " + name);
    }

    protected static boolean tryLock(Lock lock, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] tryLocking " + name);
        boolean rc=lock.tryLock();
        System.out.println("[" + Thread.currentThread().getId() + "] locked " + name);
        return rc;
    }

    protected static boolean tryLock(Lock lock, long timeout, String name) throws InterruptedException {
        System.out.println("[" + Thread.currentThread().getId() + "] tryLocking " + name);
        boolean rc=lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        System.out.println("[" + Thread.currentThread().getId() + "] locked " + name);
        return rc;
    }

    protected static void unlock(Lock lock, String name) {
        if(lock == null)
            return;
        System.out.println("[" + Thread.currentThread().getId() + "] releasing " + name);
        lock.unlock();
        System.out.println("[" + Thread.currentThread().getId() + "] released " + name);
    }

    protected void addLockingProtocol(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        stack.insertProtocolAtTop(new CENTRAL_LOCK());
    }
}

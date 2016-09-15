package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/** Tests {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},singleThreaded=true)
public class LockServiceTest {
    protected JChannel    c1, c2, c3;
    protected LockService s1, s2, s3;
    protected Lock        lock;
    protected static final String LOCK="sample-lock";


    @BeforeClass
    protected void init() throws Exception {
        c1=createChannel("A");
        s1=new LockService(c1);
        c1.connect("LockServiceTest");

        c2=createChannel("B");
        s2=new LockService(c2);
        c2.connect("LockServiceTest");

        c3=createChannel("C");
        s3=new LockService(c3);
        c3.connect("LockServiceTest");

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, c1, c2, c3);
        lock=s1.getLock(LOCK);
    }


    @AfterClass
    protected void cleanup() {
        Util.close(c3,c2,c1);
    }

    @BeforeMethod
    protected void unlockAll() {
        s3.unlockAll();
        s2.unlockAll();
        s1.unlockAll();
        Thread.interrupted(); // clears any possible interrupts from the previous method
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
        System.out.println("s1:\n" + s1.printLocks() +
                             "\ns2:\n" + s2.printLocks() +
                             "\ns3:\n" + s3.printLocks());


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
        }
        finally {
            unlock(lock2, LOCK);
        }
    }


    public void testLockInterrupt() {
        // Interrupt ourselves before trying to acquire lock
        Thread.currentThread().interrupt();

        lock.lock();
        try {
            System.out.println("Locks we have: " + s1.printLocks());
            if(Thread.interrupted())
                System.out.println("We have the interrupt flag status, as it should be");
            else
                assert false : "Interrupt status was lost - we don't want this!";
        }
        finally {
            lock.unlock();
        }
    }

    @Test(expectedExceptions=InterruptedException.class)
    public void testTryLockInterruptibly() throws InterruptedException {
        // Interrupt ourselves before trying to acquire lock
        Thread.currentThread().interrupt();

        lock.lockInterruptibly();
        try {
            System.out.println("Locks we have: " + s1.printLocks());
            if(Thread.interrupted())
                System.out.println("We still have interrupt flag set, as it should be");
            else
                assert false : "Interrupt status was lost - we don't want this!";
        }
        finally {
            lock.unlock();
        }
    }


    public void testTryLockInterrupt() {
        Thread.currentThread().interrupt(); // interrupt myself before trying to acquire lock
        boolean status=lock.tryLock();
        try {
            System.out.println("Locks we have: " + s1.printLocks());
            if(Thread.interrupted())
                System.out.println("Interrupt was set - correct");
            else
                assert false : "interrupt should not be set on tryLock()";
            assert status;
        }
        finally {
            lock.unlock();
        }
    }

    @Test(expectedExceptions=InterruptedException.class)
    public void testTimedTryLockInterrupt() throws InterruptedException {
        Thread.currentThread().interrupt(); // interrupt myself before trying to acquire lock
        boolean status=lock.tryLock(5000, TimeUnit.MILLISECONDS);
        try {
            System.out.println("Locks we have: " + s1.printLocks());
            if(Thread.interrupted())
                System.out.println("Interrupt was set - correct");
            else
                assert false : "interrupt should not be set on tryLock()";
            assert status;
        }
        finally {
            lock.unlock();
        }
    }


    public void testSuccessfulSignalAllTimeout() throws InterruptedException, BrokenBarrierException {
        Lock lock2=s2.getLock(LOCK);
        Thread locker=new Signaller(true);
        boolean rc=tryLock(lock2, 5000, LOCK);
        assert rc;
        locker.start();
        assert awaitNanos(lock2.newCondition(), TimeUnit.SECONDS.toNanos(5), LOCK) > 0 : "Condition was not signalled";
        unlock(lock2, LOCK);
    }


    public void testSuccessfulTryLockTimeout() throws InterruptedException, BrokenBarrierException {
        final CyclicBarrier barrier=new CyclicBarrier(2);
        Thread locker=new Locker(barrier);
        locker.start();
        barrier.await();
        boolean rc=tryLock(lock, 10000, LOCK);
        assert rc;
        unlock(lock, LOCK);
    }


    public void testConcurrentLockRequests() throws Exception {
        int NUM=10;
        final CyclicBarrier barrier=new CyclicBarrier(NUM +1);
        TryLocker[] lockers=new TryLocker[NUM];
        for(int i=0; i < lockers.length; i++) {
            lockers[i]=new TryLocker(lock, barrier);
            lockers[i].start();
        }
        barrier.await();
        for(TryLocker locker: lockers)
            locker.join();
        int num_acquired=0;
        for(TryLocker locker: lockers) {
            if(locker.acquired)
                num_acquired++;
        }
        System.out.println("num_acquired = " + num_acquired);
        assert num_acquired == 1 : "expected 1 acquired bot got " + num_acquired;
    }

    public void testConcurrentLockRequestsFromDifferentMembers() throws Exception {
        int NUM=10;
        final CyclicBarrier barrier=new CyclicBarrier(NUM +1);
        TryLocker[] lockers=new TryLocker[NUM];
        LockService[] services={s1, s2, s3};

        for(int i=0; i < lockers.length; i++) {
            Lock mylock=services[i % services.length].getLock(LOCK);
            lockers[i]=new TryLocker(mylock, barrier);
            lockers[i].start();
        }
        barrier.await();
        for(TryLocker locker: lockers)
            locker.join();
        int num_acquired=0;
        for(TryLocker locker: lockers) {
            if(locker.acquired) {
                num_acquired++;
            }
        }
        System.out.println("num_acquired = " + num_acquired);
        assert num_acquired == 1 : "expected 1 but got " + num_acquired;
    }

    /** Tests locking by T1 and unlocking by T2 (https://issues.jboss.org/browse/JGRP-1886) */
    public void testLockUnlockByDiffentThreads() throws Exception {
        CyclicBarrier barrier=null;
        try {
            setProp(CENTRAL_LOCK.class, "use_thread_id_for_lock_owner", false, c1,c2,c3);
            barrier=new CyclicBarrier(2);
            Thread locker=new Locker(barrier);
            locker.start();
            Util.sleep(2000);
            boolean rc=tryLock(lock, 10000, LOCK);
            assert rc;
        }
        finally {
            setProp(CENTRAL_LOCK.class, "use_thread_id_for_lock_owner", true,c1,c2,c3);
            unlock(lock, LOCK);
        }
    }


    public void testSuccessfulSignalOneTimeout() throws InterruptedException, BrokenBarrierException {
        Lock lock2 = s2.getLock(LOCK);
        Thread locker = new Signaller(false);
        boolean rc = tryLock(lock2, 5000, LOCK);
        assert rc;
        locker.start();
        assert awaitNanos(lock2.newCondition(), TimeUnit.SECONDS.toNanos(5), LOCK) > 0 : "Condition was not signalled";
        unlock(lock2, LOCK);
    }

    public void testInterruptWhileWaitingForCondition() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Thread awaiter = new Thread(new InterruptAwaiter(latch));
        awaiter.start();
        Lock lock2 = s2.getLock(LOCK);
        assert tryLock(lock2, 5000, LOCK);
        awaiter.interrupt();
        // This should not hit, since we have the lock and the condition can't
        // come out yet then
        assert !latch.await(1, TimeUnit.SECONDS);
        assert awaiter.isAlive();
        lock2.unlock();
        assert latch.await(100, TimeUnit.MILLISECONDS);
    }
    
    public void testSignalAllAwakesAllForCondition() throws InterruptedException {
        final int threadCount = 5;
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        try {
            
            for (int i = 0; i < threadCount; ++i) {
                service.submit(new SyncAwaiter(latch));
            
            }
            // Wait for all the threads to be waiting on condition
            latch.await(2, TimeUnit.SECONDS);
            
            Lock lock2 = s2.getLock(LOCK);
            assert tryLock(lock2, 5000, LOCK);
            lock2.newCondition().signalAll();
            lock2.unlock();
            service.shutdown();
            service.awaitTermination(2, TimeUnit.SECONDS);
        }
        finally {
            service.shutdownNow();
        }
    }



    protected JChannel createChannel(String name) throws Exception {
        Protocol[] stack=Util.getTestStack(new CENTRAL_LOCK().level("trace"));
        return new JChannel(stack).name(name);
    }

    protected void setProp(Class<?> clazz, String prop_name, Object value, JChannel ... channels) {
        for(JChannel ch: channels) {
            Protocol prot=ch.getProtocolStack().findProtocol(clazz);
            prot.setValue(prop_name, value);
        }
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
    
    protected class Signaller extends Thread {
        protected final boolean all;

        public Signaller(boolean all) {
            this.all=all;
        }

        public void run() {
            lock(lock, LOCK);
            try {
                Util.sleep(500);
                
                if (all) {
                    signallingAll(lock.newCondition(), LOCK);
                }
                else {
                    signalling(lock.newCondition(), LOCK);
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                unlock(lock, LOCK);
            }
        }
    }

    protected abstract class AbstractAwaiter implements Runnable {
        public void afterLock() { }
        
        public void onInterrupt() { }
        
        public void run() {
            lock(lock, LOCK);
            try {
                afterLock();
                try {
                    lock.newCondition().await(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    onInterrupt();
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                unlock(lock, LOCK);
            }
        }
    }
    
    protected class InterruptAwaiter extends AbstractAwaiter {
        final CountDownLatch latch;
        
        public InterruptAwaiter(CountDownLatch latch) {
            this.latch = latch;
        }
        
        @Override
        public void onInterrupt() {
            latch.countDown();
        }
    }
    
    protected class SyncAwaiter extends AbstractAwaiter {
        final CountDownLatch latch;
        
        public SyncAwaiter(CountDownLatch latch) {
            this.latch = latch;
        }
        
        @Override
        public void afterLock() {
            latch.countDown();
        }
    }


    protected static class TryLocker extends Thread {
        protected final Lock          mylock;
        protected final CyclicBarrier barrier;
        protected boolean             acquired;

        public TryLocker(Lock mylock, CyclicBarrier barrier) {
            this.mylock=mylock;
            this.barrier=barrier;
        }

        public boolean isAcquired() {
            return acquired;
        }

        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }

            try {
                acquired=tryLock(mylock, LOCK);
                if(acquired)
                    Util.sleep(2000);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                if(acquired)
                    unlock(mylock, LOCK);
            }
        }
    }

    protected static class AcquireLockAndAwaitCondition extends Thread {
        private final Lock lock;
        
        public AcquireLockAndAwaitCondition(Lock lock) {
           this.lock = lock;
        }
        
        @Override
        public void run() {
            if (tryLock(lock, LOCK)) {
               try {
                  Condition condition = lock.newCondition();
                  try {
                     condition.await();
                  } catch (InterruptedException e) {
                     System.out.println("");
                  }
               }
               finally {
                  unlock(lock, LOCK);
               }
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
        System.out.println("[" + Thread.currentThread().getId() + "] " + (rc? "locked " : "failed locking ") + name);
        return rc;
    }

    protected static boolean tryLock(Lock lock, long timeout, String name) throws InterruptedException {
        System.out.println("[" + Thread.currentThread().getId() + "] tryLocking " + name);
        boolean rc=lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        System.out.println("[" + Thread.currentThread().getId() + "] " + (rc? "locked " : "failed locking ") + name);
        return rc;
    }

    protected static void unlock(Lock lock, String name) {
        if(lock == null)
            return;
        System.out.println("[" + Thread.currentThread().getId() + "] releasing " + name);
        lock.unlock();
        System.out.println("[" + Thread.currentThread().getId() + "] released " + name);
    }
    
    protected static long awaitNanos(Condition condition, long nanoSeconds, 
                                     String name) throws InterruptedException {
        System.out.println("[" + Thread.currentThread().getId() + "] waiting for signal - released lock " + name);
        long value = condition.awaitNanos(nanoSeconds);
        System.out.println("[" + Thread.currentThread().getId() + "] waited for signal - obtained lock " + name);
        return value;
    }
    
    protected static void signalling(Condition condition, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] signalling " + name);
        condition.signal();
        System.out.println("[" + Thread.currentThread().getId() + "] signalled " + name);
    }
    
    protected static void signallingAll(Condition condition, String name) {
        System.out.println("[" + Thread.currentThread().getId() + "] signalling all " + name);
        condition.signalAll();
        System.out.println("[" + Thread.currentThread().getId() + "] signalled " + name);
    }

}

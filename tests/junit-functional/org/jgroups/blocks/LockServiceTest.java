package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.protocols.CENTRAL_LOCK2;
import org.jgroups.protocols.Locking;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/** Tests {@link org.jgroups.blocks.locking.LockService}
 * @author Bela Ban
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},singleThreaded=true,dataProvider="createLockingProtocol")
public class LockServiceTest {
    protected JChannel                              c1, c2, c3;
    protected LockService                           s1, s2, s3;
    protected Lock                                  lock;
    protected static final String                   LOCK="sample-lock";
    protected static final Class<? extends Locking> LOCK_CLASS=Locking.class;
    protected static final String                   CLUSTER=LockServiceTest.class.getSimpleName();
    protected static final int                      NUM_ITERATIONS=1_000;

    @DataProvider(name="createLockingProtocol")
    Object[][] createLockingProtocol() {
        return new Object[][] {
          {CENTRAL_LOCK.class},
          {CENTRAL_LOCK2.class}
        };
    }


    protected void init(Class<? extends Locking> locking_class) throws Exception {
        c1=createChannel("A", locking_class);
        s1=new LockService(c1);
        c1.connect(CLUSTER);

        c2=createChannel("B", locking_class);
        s2=new LockService(c2);
        c2.connect(CLUSTER);

        c3=createChannel("C", locking_class);
        s3=new LockService(c3);
        c3.connect(CLUSTER);

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, c1, c2, c3);
        lock=s1.getLock(LOCK);
    }


    @AfterMethod
    protected void cleanup() {
        Util.close(c3,c2,c1);
    }


    @Test(dataProvider="createLockingProtocol")
    public void testSimpleLock(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        lock(lock, LOCK);
        unlock(lock, LOCK);
    }

    public void testLockingOfAlreadyAcquiredLock(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        lock(lock, LOCK);
        lock(lock, LOCK);
        unlock(lock, LOCK);
    }

    public void testUnsuccessfulTryLock(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        System.out.printf("s1:\n%s\ns2:\n%s\ns3:\n%s\n", s1.printLocks(), s2.printLocks(), s3.printLocks());

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

    public void testUnsuccessfulTryLockTimeout(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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


    public void testLockInterrupt(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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

    @Test(expectedExceptions=InterruptedException.class,dataProvider="createLockingProtocol")
    public void testTryLockInterruptibly(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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


    public void testTryLockInterrupt(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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

    @Test(expectedExceptions=InterruptedException.class,dataProvider="createLockingProtocol")
    public void testTimedTryLockInterrupt(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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

    /** Multiple lock-unlock cycles */
    public void testLockMultipleTimes(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);

        int print=NUM_ITERATIONS / 10;
        for(int i=0; i < NUM_ITERATIONS; i++) {
            lock(lock, LOCK);
            try {
                assert true: "lock not acquired!";
            }
            finally {
                unlock(lock, LOCK);
            }
            if(i > 0 && i % print == 0)
                System.out.printf("-- %d iterations\n", i);
        }
    }


    /** Multiple trylock-unlock cycles */
    public void testTryLockMultipleTimes(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);

        int print=NUM_ITERATIONS / 10;
        for(int i=0; i < NUM_ITERATIONS; i++) {
            boolean rc=tryLock(lock, 10000, LOCK);
            try {
                assert rc : "lock not acquired!";
            }
            finally {
                unlock(lock, LOCK);
            }
            if(i > 0 && i % print == 0)
                System.out.printf("-- %d iterations\n", i);
        }
    }


    public void testSuccessfulSignalAllTimeout(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        Lock lock2=s2.getLock(LOCK);
        Thread locker=new Signaller(true);
        boolean rc=tryLock(lock2, 5000, LOCK);
        assert rc;
        locker.start();
        assert awaitNanos(lock2.newCondition(), TimeUnit.SECONDS.toNanos(5), LOCK) > 0 : "Condition was not signalled";
        unlock(lock2, LOCK);
    }


    public void testSuccessfulTryLockTimeout(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        final CyclicBarrier barrier=new CyclicBarrier(2);
        Thread locker=new Locker(barrier);
        locker.start();
        barrier.await();
        boolean rc=tryLock(lock, 10000, LOCK);
        assert rc;
        unlock(lock, LOCK);
    }


    public void testConcurrentLockRequests(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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

    public void testConcurrentLockRequestsFromDifferentMembers(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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
    public void testLockUnlockByDiffentThreads(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        CyclicBarrier barrier=null;
        try {
            setProp(LOCK_CLASS, "use_thread_id_for_lock_owner", false, c1,c2,c3);
            barrier=new CyclicBarrier(2);
            Thread locker=new Locker(barrier);
            locker.start();
            Util.sleep(2000);
            boolean rc=tryLock(lock, 10000, LOCK);
            assert rc;
        }
        finally {
            setProp(LOCK_CLASS, "use_thread_id_for_lock_owner", true,c1,c2,c3);
            unlock(lock, LOCK);
        }
    }


    public void testSuccessfulSignalOneTimeout(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
        Lock lock2 = s2.getLock(LOCK);
        Thread locker = new Signaller(false);
        boolean rc = tryLock(lock2, 5000, LOCK);
        assert rc;
        locker.start();
        assert awaitNanos(lock2.newCondition(), TimeUnit.SECONDS.toNanos(5), LOCK) > 0 : "Condition was not signalled";
        unlock(lock2, LOCK);
    }

    public void testInterruptWhileWaitingForCondition(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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
    
    public void testSignalAllAwakesAllForCondition(Class<? extends Locking> locking_class) throws Exception {
        init(locking_class);
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



    protected static JChannel createChannel(String name, Class<? extends Locking> locking_class) throws Exception {
        Protocol[] stack=Util.getTestStack(locking_class.getDeclaredConstructor().newInstance().level("trace"));
        return new JChannel(stack).name(name);
    }

    protected static void setProp(Class<? extends Protocol> clazz, String prop_name, Object value, JChannel... channels) {
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

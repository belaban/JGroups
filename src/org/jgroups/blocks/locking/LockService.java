package org.jgroups.blocks.locking;

import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.protocols.Locking;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * LockService is the main class for to use for distributed locking functionality. LockService needs access to a
 * {@link JChannel} and interacts with a locking protocol (e.g. {@link org.jgroups.protocols.CENTRAL_LOCK}) via events.<p/>
 * When no locking protocol is seen on the channel's stack, LockService will throw an exception at startup. An example
 * of using LockService is:
 * <pre>
   JChannel ch=new JChannel("/home/bela/locking.xml); // locking.xml needs to have a locking protocol towards the top
   LockService lock_service=new LockService(ch);
   ch.connect("lock-cluster");
   Lock lock=lock_service.getLock("mylock");
   lock.lock();
   try {
      // do something with the lock acquired
   }
   finally {
      lock.unlock();
   }
 * </pre>
 * <p/>
 * The exact semantics of this lock implemantation are defined in {@link LockImpl}.
 * <p/>
 * Note that, contrary to the semantics of {@link java.util.concurrent.locks.Lock}, unlock() can be called multiple
 * times; after a lock has been released, future calls to unlock() have no effect.
 * @author Bela Ban
 * @since 2.12
 */
public class LockService {
    protected JChannel ch;
    protected Locking  lock_prot;


    public LockService() {
        
    }

    public LockService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        lock_prot=ch.getProtocolStack().findProtocol(Locking.class);
        if(lock_prot == null)
            throw new IllegalStateException("Channel configuration must include a locking protocol " +
                                              "(subclass of " + Locking.class.getName() + ")");
    }

    public Lock getLock(String lock_name) {
        return new LockImpl(lock_name);
    }

    public void unlockAll() {
        ch.down(new Event(Event.UNLOCK_ALL));
    }

    public void unlockForce(String lock_name) {
        ch.down(new Event(Event.UNLOCK_FORCE, lock_name));
    }

    public void addLockListener(LockNotification listener) {
        lock_prot.addLockListener(listener);
    }

    public void removeLockListener(LockNotification listener) {
        lock_prot.removeLockListener(listener);
    }
    
    public String printLocks() {
        return lock_prot.printLocks();
    }


    /**
     * Implementation of {@link Lock}. This is a client stub communicates with a server equivalent. The semantics are
     * more or less those of {@link Lock}, but may differ slightly.<p/>
     * There is no reference counting of lock owners, so acquisition of a lock already held by a thread is a no-op.
     * Also, releasing the lock after it was already released is a no-op as well.
     * <p/>
     * An exact description is provided below.
     */
    protected class LockImpl implements Lock {
        protected final String name;
        protected final AtomicReference<Thread> holder=new AtomicReference<>();

        public LockImpl(String name) {
            this.name=name;
        }

        /**
         * {@inheritDoc}
         * Blocks until the lock has been acquired. Masks interrupts; if an interrupt was received on entry or while
         * waiting for the lock acquisition, it won't cause the call to return. However, the thread's status will be
         * set to interrupted when the call returns.
         */
        @Override
        public void lock() {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
            holder.set(Thread.currentThread());
        }

        /**
         * {@inheritDoc}
         * If the thread is interrupted at entry, the call will throw an InterruptedException immediately and the lock
         * won't be acquired. If the thread is interrupted while waiting for the lock acquition, an InterruptedException
         * will also be thrown immediately. The thread's interrupt status will not be set after the call returns.
         * @throws InterruptedException
         */
        public void lockInterruptibly() throws InterruptedException {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, true, false, 0, TimeUnit.MILLISECONDS)));
            Thread currentThread = Thread.currentThread();
            if(currentThread.isInterrupted())
                throw new InterruptedException();
            else
                holder.set(currentThread);
        }

        /**
         * {@inheritDoc}
         * If the thread is interrupted at entry or during the call, no InterruptedException will be thrown, but the
         * thread's status will be set to interrupted upon return. An interrupt has therefore no impact on the
         * return value (success or failure).
         */
        public boolean tryLock() {
            Boolean retval=(Boolean)ch.down(new Event(Event.LOCK, new LockInfo(name, true, false, false, 0, TimeUnit.MILLISECONDS)));
            if(retval != null && retval)
                holder.set(Thread.currentThread());
            return retval == null ? false : retval;
        }

        /**
         * {@inheritDoc}
         * * If the thread is interrupted at entry, the call will throw an InterruptedException immediately and the lock
         * won't be acquired. If the thread is interrupted while waiting for the lock acquition, an InterruptedException
         * will also be thrown immediately. The thread's interrupt status will not be set after the call returns.
         * @param time
         * @param unit
         * @return
         * @throws InterruptedException
         */
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Boolean retval=(Boolean)ch.down(new Event(Event.LOCK, new LockInfo(name, true, true, true, time, unit)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            if(retval != null && retval)
                holder.set(Thread.currentThread());
            return retval == null ? false : retval;
        }

        /**
         * {@inheritDoc}
         * Releases a lock. Contrary to the parent's implementation, this method can be called more than once:
         * the release of a lock that has already been released, or is not owned by this thread is a no-op.
         */
        public void unlock() {
            ch.down(new Event(Event.UNLOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
            holder.compareAndSet(Thread.currentThread(), null); // only set if the current thread is actually the holder
        }

        /**
         * This condition object is only allowed to work 1 for each lock.
         * If more than 1 condition is created for this lock, they both will
         * be awaiting/signalling on the same lock
         */
        public Condition newCondition() {
            return new ConditionImpl(name, holder);
        }

        public String toString() {
            return name + (holder.get() == null? " [unlocked]" : " [held by " + holder.get() + "]");
        }
    }
    
    private class ConditionImpl implements Condition {
        protected final String name;
        protected final AtomicReference<Thread> holder;

        public ConditionImpl(String name, AtomicReference<Thread> holder) {
            this.name=name;
            this.holder=holder;
        }
        
        @Override
        public void await() throws InterruptedException {
            ch.down(new Event(Event.LOCK_AWAIT, new LockInfo(name, false, 
                true, false, 0, TimeUnit.MILLISECONDS)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
        }

        @Override
        public void awaitUninterruptibly() {
            ch.down(new Event(Event.LOCK_AWAIT, new LockInfo(name, false, 
                false, false, 0, TimeUnit.MILLISECONDS)));
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            Long waitLeft = (Long)ch.down(new Event(Event.LOCK_AWAIT,
                new LockInfo(name, false, true, true, nanosTimeout, 
                    TimeUnit.NANOSECONDS)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            return waitLeft;
        }

        @Override
        public boolean await(long time, TimeUnit unit)
                throws InterruptedException {
            return awaitNanos(unit.toNanos(time)) > 0;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            long waitUntilTime = deadline.getTime();
            long currentTime = System.currentTimeMillis();
            
            long waitTime = waitUntilTime - currentTime;
            return waitTime > 0 && await(waitTime, TimeUnit.MILLISECONDS);
        }

        @Override
        public void signal() {
            if (holder.get() != Thread.currentThread()) {
                throw new IllegalMonitorStateException();
            }
            ch.down(new Event(Event.LOCK_SIGNAL, new AwaitInfo(name, false)));
        }

        @Override
        public void signalAll() {
            if (holder.get() != Thread.currentThread()) {
                throw new IllegalMonitorStateException();
            }
            ch.down(new Event(Event.LOCK_SIGNAL, new AwaitInfo(name, true)));
        }
    }
}

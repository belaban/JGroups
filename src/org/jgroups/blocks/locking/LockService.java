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
 * Note that, contrary to the semantics of {@link java.util.concurrent.locks.Lock}, unlock() can be called multiple
 * times; after a lock has been released, future calls to unlock() have no effect.
 * @author Bela Ban
 * @since 2.12
 */
public class LockService {
    protected JChannel ch;
    protected Locking lock_prot;


    public LockService() {
        
    }

    public LockService(JChannel ch) {
        setChannel(ch);
    }

    public void setChannel(JChannel ch) {
        this.ch=ch;
        lock_prot=(Locking)ch.getProtocolStack().findProtocol(Locking.class);
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

    public void addLockListener(LockNotification listener) {
        lock_prot.addLockListener(listener);
    }

    public void removeLockListener(LockNotification listener) {
        lock_prot.removeLockListener(listener);
    }
    
    public String printLocks() {
        return lock_prot.printLocks();
    }



    protected class LockImpl implements Lock {
        protected final String name;
        protected final AtomicReference<Thread> holder = new AtomicReference<Thread>();

        public LockImpl(String name) {
            this.name=name;
        }

        public void lock() {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
            holder.set(Thread.currentThread());
        }

        public void lockInterruptibly() throws InterruptedException {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, true, false, 0, TimeUnit.MILLISECONDS)));
            Thread currentThread = Thread.currentThread();
            if(currentThread.isInterrupted())
                throw new InterruptedException();
            else
                holder.set(Thread.currentThread());
        }

        public boolean tryLock() {
            Boolean retval=(Boolean)ch.down(new Event(Event.LOCK, new LockInfo(name, true, false, false, 0, TimeUnit.MILLISECONDS)));
            if (retval == Boolean.TRUE) {
                holder.set(Thread.currentThread());
            }
            return retval.booleanValue();
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Boolean retval=(Boolean)ch.down(new Event(Event.LOCK, new LockInfo(name, true, true, true, time, unit)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            if (retval == Boolean.TRUE) {
                holder.set(Thread.currentThread());
            }
            return retval.booleanValue();
        }

        public void unlock() {
            ch.down(new Event(Event.UNLOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
            holder.set(null);
        }

        /**
         * This condition object is only allowed to work 1 for each lock.
         * If more than 1 condition is created for this lock, they both will
         * be awaiting/signalling on the same lock
         */
        public Condition newCondition() {
            return new ConditionImpl(name, holder);
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
            return waitLeft.longValue();
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

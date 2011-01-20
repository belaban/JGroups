package org.jgroups.blocks.locking;

import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.annotations.Experimental;
import org.jgroups.protocols.Locking;

import java.util.concurrent.TimeUnit;
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
@Experimental
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

        public LockImpl(String name) {
            this.name=name;
        }

        public void lock() {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
        }

        public void lockInterruptibly() throws InterruptedException {
            ch.down(new Event(Event.LOCK, new LockInfo(name, false, true, false, 0, TimeUnit.MILLISECONDS)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
        }

        public boolean tryLock() {
            Boolean retval=(Boolean)ch.downcall(new Event(Event.LOCK, new LockInfo(name, true, false, false, 0, TimeUnit.MILLISECONDS)));
            return retval.booleanValue();
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            Boolean retval=(Boolean)ch.downcall(new Event(Event.LOCK, new LockInfo(name, true, true, true, time, unit)));
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();
            return retval.booleanValue();
        }

        public void unlock() {
            ch.down(new Event(Event.UNLOCK, new LockInfo(name, false, false, false, 0, TimeUnit.MILLISECONDS)));
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }
}

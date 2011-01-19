package org.jgroups.blocks.locking;

import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.protocols.Locking;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author Bela Ban
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


package org.jgroups.util;

import org.jgroups.TimeoutException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows a thread to submit an asynchronous request and to wait for the result. The caller may choose to check
 * for the result at a later time, or immediately and it may block or not. Both the caller and responder have to
 * know the promise.<p/>
 * When the result is available, {@link #hasResult()} will always return true and {@link #getResult()} will return the
 * same result. In order to block for a different result, {@link #reset()} has to be called first.
 * @author Bela Ban
 */
public class Promise<T> {
    private final Lock        lock=new ReentrantLock();
    private final Condition   cond=lock.newCondition();
    private T                 result=null;
    private volatile boolean  hasResult=false;

    
    public Lock      getLock() {return lock;}
    public Condition getCond() {return cond;}

    
    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed
     * @param timeout in ms
     * @return An object
     * @throws TimeoutException If a timeout occurred (implies that timeout > 0)
     */
    public T getResultWithTimeout(long timeout) throws TimeoutException {
        lock.lock();
        try {
            return _getResultWithTimeout(timeout);
        }
        finally {
            cond.signalAll();
            lock.unlock();
        }
    }

    public T getResult() {
        try {
            return getResultWithTimeout(0);
        }
        catch(TimeoutException e) {
            return null;
        }
    }

    /**
     * Returns the result, but never throws a TimeoutException; returns null instead.
     * @param timeout in ms
     * @return T
     */
    public T getResult(long timeout) {
        try {
            return getResultWithTimeout(timeout);
        }
        catch(TimeoutException e) {
            return null;
        }
    }

    /**
     * Checks whether result is available. Does not block.
     */
    public boolean hasResult() {
        lock.lock();
        try {
            return hasResult;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Sets the result and notifies any threads waiting for it
     */
    public void setResult(T obj) {
        lock.lock();
        try {
            result=obj;
            hasResult=true;
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Causes all waiting threads to return
     */
    public void reset() {
        lock.lock();
        try {
            result=null;
            hasResult=false;
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        return "hasResult=" + Boolean.valueOf(hasResult) + ", result=" + result;
    }


    

    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed. Needs to be called with lock held
     * @param timeout in ms
     * @return An object
     * @throws TimeoutException If a timeout occurred (implies that timeout > 0)
     */
    protected T _getResultWithTimeout(final long timeout) throws TimeoutException {
        if(timeout <= 0) {
            while(!hasResult) { /* Wait for responses: */
                try {cond.await();} catch(Exception e) {}
            }
        }
        else {
            long wait_time=TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
            final long target_time=System.nanoTime() + wait_time;
            while(wait_time > 0 && !hasResult) { /* Wait for responses: */
                wait_time=target_time - System.nanoTime();
                if(wait_time > 0) {
                    try {cond.await(wait_time, TimeUnit.NANOSECONDS);} catch(Exception e) {}
                }
            }
            if(!hasResult && wait_time <= 0)
                throw new TimeoutException();
        }
        return result;
    }



}
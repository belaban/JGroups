
package org.jgroups.util;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows a thread to submit an asynchronous request and to wait for the result. The caller may choose to check
 * for the result at a later time, or immediately and it may block or not. Both the caller and responder have to
 * know the promise.<p/>
 * When the result is available, {@link #hasResult()} will always return true and {@link #getResult()} will return the
 * result. In order to block for a different result, {@link #reset()} has to be called first.
 * @author Bela Ban
 */
public class Promise<T> {
    protected final Lock        lock=new ReentrantLock();
    protected final CondVar     cond=new CondVar(lock);
    protected T                 result;
    protected volatile boolean  hasResult=false; // condition


    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed
     * @param timeout in ms
     * @return An object
     * @throws TimeoutException If a timeout occurred (implies that timeout > 0)
     */
    public T getResultWithTimeout(long timeout) throws TimeoutException {
        return _getResultWithTimeout(timeout);
    }

    public T getResultWithTimeout(long timeout, boolean reset) throws TimeoutException {
        if(!reset)
            return _getResultWithTimeout(timeout);

        // the lock is acquired because we want to get the result and reset the promise in the same lock scope; if we had
        // to re-acquire the lock for reset(), some other thread could possibly set a new result before reset() is called !
        lock.lock();
        try {
            return _getResultWithTimeout(timeout);
        }
        finally {
            reset();
            lock.unlock();
        }
    }

    /** Returns when the result is available (blocking until tthe result is available) */
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
        return getResult(timeout, false);
    }

    public T getResult(long timeout, boolean reset) {
        try {
            return getResultWithTimeout(timeout, reset);
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
            cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Causes all waiting threads to return
     */
    public void reset() {
        reset(true);
    }

    public void reset(boolean signal) {
        lock.lock();
        try {
            result=null;
            hasResult=false;
            if(signal)
                cond.signal(true);
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        return String.format("hasResult=%b, result=%s", hasResult, result);
    }


    

    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed. Needs to be called with lock held
     * @param timeout in ms
     * @return An object
     * @throws TimeoutException If a timeout occurred (implies that timeout > 0)
     */
    protected T _getResultWithTimeout(final long timeout) throws TimeoutException {
        if(timeout <= 0)
            cond.waitFor(this::hasResult);
        else if(!cond.waitFor(this::hasResult, timeout, TimeUnit.MILLISECONDS))
            throw new TimeoutException();
        return result;
    }



}

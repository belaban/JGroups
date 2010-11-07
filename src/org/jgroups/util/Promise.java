
package org.jgroups.util;

import org.jgroups.TimeoutException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows a thread to submit an asynchronous request and to wait for the result. The caller may choose to check
 * for the result at a later time, or immediately and it may block or not. Both the caller and responder have to
 * know the promise.
 * @author Bela Ban
 * @version $Id: Promise.java,v 1.15 2009/06/22 14:34:26 belaban Exp $
 */
public class Promise<T> {
    private final Lock lock=new ReentrantLock();
    private final Condition cond=lock.newCondition();
    private T result=null;
    private volatile boolean hasResult=false;

    public Lock getLock() {
        return lock;
    }

    public Condition getCond() {
        return cond;
    }

    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed
     * @param timeout
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


    /**
     * Blocks until a result is available, or timeout milliseconds have elapsed. Needs to be called with lock held
     * @param timeout
     * @return An object
     * @throws TimeoutException If a timeout occurred (implies that timeout > 0)
     */
    private T _getResultWithTimeout(long timeout) throws TimeoutException {
        T       ret=null;
        long    time_to_wait=timeout, start;
        boolean timeout_occurred=false;

        start=System.currentTimeMillis();
        while(hasResult == false) {
            if(timeout <= 0) {
                doWait();
            }
            else {
                if(time_to_wait <= 0) {
                    timeout_occurred=true;
                    break; // terminate the while loop
                }
                else {
                    doWait(time_to_wait);
                    time_to_wait=timeout - (System.currentTimeMillis() - start);
                }
            }
        }

        ret=result;
        result=null;
        hasResult=false;
        if(timeout_occurred)
            throw new TimeoutException();
        else
            return ret;
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
     * @param timeout
     * @return Object
     */
    public T getResult(long timeout) {
        try {
            return getResultWithTimeout(timeout);
        }
        catch(TimeoutException e) {
            return null;
        }
    }


    private void doWait() {
        try {cond.await();} catch(InterruptedException e) {}
    }

    private void doWait(long timeout) {
        try {cond.await(timeout, TimeUnit.MILLISECONDS);} catch(InterruptedException e) {}
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
     * Sets the result and notifies any threads
     * waiting for it
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
        return "hasResult=" + Boolean.valueOf(hasResult) + ",result=" + result;
    }


}
package org.jgroups.util;

import org.jgroups.TimeoutException;


/**
 * Class that checks on a condition and - if condition doesn't match the expected result - waits until the result
 * matches the expected result, or a timeout occurs. First version used WaitableBoolean from util.concurrent, but
 * that class would not allow for timeouts.
 * @author Bela Ban
 * @version $Id: CondVar.java,v 1.1 2004/09/22 10:32:26 belaban Exp $
 */
public class CondVar {
    Object cond;
    String name;

    public CondVar(String name, Object cond) {
        this.name=name;
        this.cond=cond;
    }

    public Object get() {
        synchronized(this) {
            return cond;
        }
    }

    /** Sets the result */
    public void set(Object result) {
        synchronized(this) {
            cond=result;
            notifyAll();
        }
    }


    /**
     * Waits until the condition matches the expected result. Returns immediately if they match, otherwise waits
     * for timeout milliseconds or until the results match.
     * @param result The result, needs to match the condition (using equals()).
     * @param timeout Number of milliseconds to wait. A value of <= 0 means to wait forever
     * @throws TimeoutException Thrown if the result still doesn't match the condition after timeout
     * milliseconds have elapsed
     */
    public void waitUntil(Object result, long timeout) throws TimeoutException {
        long    time_to_wait=timeout, start;
        boolean timeout_occurred=false;
        synchronized(this) {
            if(result == null && cond == null) return;

            start=System.currentTimeMillis();
            while(match(result, cond) == false) {
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
                        time_to_wait-=System.currentTimeMillis()-start;
                    }
                }
            }
            if(timeout_occurred)
                throw new TimeoutException();
        }
    }


    private boolean match(Object o1, Object o2) {
        if(o1 != null)
            return o1.equals(o2);
        else
            return o2.equals(o1);
    }


    public void waitUntil(Object result) {
        try {waitUntil(result, 0);} catch(TimeoutException e) {}
    }

    void doWait() {
        try {wait();} catch(InterruptedException e) {}
    }

    void doWait(long timeout) {
        try {wait(timeout);} catch(InterruptedException e) {}
    }


    public String toString() {
        return name + "=" + cond;
    }
}

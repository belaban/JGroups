// $Id: Promise.java,v 1.4 2004/01/18 15:14:06 tsorgie Exp $

package org.jgroups.util;


/**
 * Allows a thread to submit an asynchronous request and to wait for the result. The caller may choose to check
 * for the result at a later time, or immediately and it may block or not. Both the caller and responder have to
 * know the promise.
 */
public class Promise {
    Object result=null;
    boolean hasResult=false;


    /**
     * Gets result. If result was already submitted,
     * returns it immediately (resetting it), else blocks
     * until
     * results get available.
     * 
     * @param timeout Max time to wait for result. If it
     *                is 0, we wait indefinitely
     */
    public Object getResult(long timeout) {
        Object ret=null;

        synchronized(this) {
            if(hasResult) {
                ret=result;
                result=null;
                hasResult=false;
                return ret;
            }
            try {
                if(timeout <= 0) {
                    wait();
                }
                else {
                    wait(timeout);
                }
            }
            catch(Exception ex) {

            }
            if(hasResult) {
                ret=result;
                result=null;
                hasResult=false;
                return ret;
            }
            return null;
        }
    }


    /**
     * Checks whether result is available. Does not block.
     */
    public boolean hasResult() {
        synchronized(this) {
            return hasResult;
        }
    }

    /**
     * Sets the result and notifies any threads
     * waiting for it
     */
    public void setResult(Object obj) {
        synchronized(this) {
            result=obj;
            hasResult=true;
            notifyAll();
        }
    }


    /**
     * Causes all waiting threads to return
     */
    public void reset() {
        synchronized(this) {
            result=null;
            hasResult=false;
            notifyAll();
        }
    }


    public String toString() {
        return "hasResult=" + hasResult + ",result=" + result;
    }


}
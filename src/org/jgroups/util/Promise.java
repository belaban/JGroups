// $Id: Promise.java,v 1.2 2003/12/15 22:30:20 belaban Exp $

package org.jgroups.util;


/**
 Allows a thread to submit an asynchronous request and to wait for the result. The caller may choose to check
 for the result at a later time, or immediately and it may block or not. Both the caller and responder have to
 know the promise.
 */
public class Promise {
    Object result=null;


    /** Gets result. If result was already submitted, returns it immediately (resetting it), else blocks until
     results get available. <em>Note that the result has to be non-null</em>
     @param timeout Max time to wait for result. If it is 0, we wait indefinitely
     */
    public Object getResult(long timeout) {
        Object ret=null;

        synchronized(this) {
            if(result != null) {
                ret=result;
                result=null;
                return ret;
            }
            if(timeout <= 0) {
                try {
                    wait();
                }
                catch(Exception ex) {
                }
            }
            else {
                try {
                    wait(timeout);
                }
                catch(Exception ex) {
                }
            }
            if(result != null) {
                ret=result;
                result=null;
                return ret;
            }
            return null;
        }
    }


    /** Checks whether result is available. Does not block. */
    public Object checkForResult() {
        synchronized(this) {
            return result;
        }
    }

    /** Sets the result and notifies any threads waiting for it */
    public void setResult(Object obj) {
        synchronized(this) {
            result=obj;
            notifyAll();
        }
    }


    /** Causes all waiting threads to return */
    public void reset() {
        synchronized(this) {
            result=null;
            notifyAll();
        }
    }


    public String toString() {
        return "result=" + result;
    }


}

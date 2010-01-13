package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Transport;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Command;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Abstract class for a unicast or multicast request
 *
 * @author Bela Ban
 * @version $Id: Request.java,v 1.2 2010/01/13 13:13:32 belaban Exp $
 */
public abstract class Request implements RspCollector, Command {
    /** return only first response */
    public static final int GET_FIRST=1;

    /** return all responses */
    public static final int GET_ALL=2;

    /** return majority (of all non-faulty members) */
    public static final int GET_MAJORITY=3;

    /** return majority (of all members, may block) */
    public static final int GET_ABS_MAJORITY=4;

    /** return n responses (may block) */
    @Deprecated public static final int GET_N=5;

    /** return no response (async call) */
    public static final int GET_NONE=6;


    protected static final Log log=LogFactory.getLog(Request.class);

    /** To generate unique request IDs (see getRequestId()) */
    protected static final AtomicLong REQUEST_ID=new AtomicLong(1);

    protected final Lock lock=new ReentrantLock();

    /** Is set as soon as the request has received all required responses */
    protected final Condition completed=lock.newCondition();

    protected final  Message          request_msg;
    protected final RequestCorrelator corr;         // either use RequestCorrelator or ...
    protected final Transport         transport;    // Transport (one of them has to be non-null)

    protected RspFilter               rsp_filter;

    protected final int               rsp_mode;
    protected volatile boolean        done;
    protected boolean                 block_for_results=true;
    protected final long              timeout;
    protected final long              req_id; // request ID for this request


    

    public Request(Message request, RequestCorrelator corr, Transport transport, RspFilter filter, int mode, long timeout) {
        this.request_msg=request;
        this.corr=corr;
        this.transport=transport;
        this.rsp_filter=filter;
        this.rsp_mode=mode;
        this.timeout=timeout;
        this.req_id=getRequestId();
    }


    public void setResponseFilter(RspFilter filter) {
        rsp_filter=filter;
    }

    public boolean getBlockForResults() {
        return block_for_results;
    }

    public void setBlockForResults(boolean block_for_results) {
        this.block_for_results=block_for_results;
    }
    


    public boolean execute() throws Exception {
        if(corr == null && transport == null) {
            if(log.isErrorEnabled()) log.error("both corr and transport are null, cannot send group request");
            return false;
        }

        sendRequest();
        if(!block_for_results || rsp_mode == GET_NONE)
            return true;

        lock.lock();
        try {
            done=false;
            boolean retval=responsesComplete(timeout);
            if(retval == false && log.isTraceEnabled())
                log.trace("call did not execute correctly, request is " + toString());
            return retval;
        }
        finally {
            done=true;
            lock.unlock();
        }
    }

    protected abstract void sendRequest() throws Exception;

    public abstract void receiveResponse(Object response_value, Address sender);

    public abstract void viewChange(View new_view);

    public abstract void suspect(Address mbr);

    protected void adjustMembership() {}
    
    protected abstract boolean responsesComplete();


    public boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            boolean retval=!done;
            done=true;
            if(corr != null)
                corr.done(req_id);
            completed.signalAll();
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isCancelled() {
        lock.lock();
        try {
            return done;
        }
        finally {
            lock.unlock();
        }
    }


    public boolean isDone() {
        return done;
    }


    public String toString() {
        StringBuilder ret=new StringBuilder(128);
        ret.append(super.toString());
        ret.append("req_id=").append(req_id).append(", mode=" + modeToString(rsp_mode));
        return ret.toString();
    }


    /* --------------------------------- Private Methods -------------------------------------*/



    /** Generates a new unique request ID */
    protected static long getRequestId() {
        return REQUEST_ID.incrementAndGet();
    }

    /** This method runs with lock locked (called by <code>execute()</code>). */
    @GuardedBy("lock")
    protected boolean responsesComplete(long timeout) throws InterruptedException {
        if(timeout <= 0) {
            while(true) { /* Wait for responses: */
                adjustMembership(); // may not be necessary, just to make sure...
                if(responsesComplete()) {
                    if(corr != null) {
                        corr.done(req_id);
                    }
                    if(log.isTraceEnabled() && rsp_mode != GET_NONE) {
                        log.trace("received all responses: " + toString());
                    }
                    return true;
                }
                completed.await();
            }
        }
        else {
            long start_time=System.currentTimeMillis();
            long timeout_time=start_time + timeout;
            while(timeout > 0) { /* Wait for responses: */
                if(responsesComplete()) {
                    if(corr != null)
                        corr.done(req_id);
                    if(log.isTraceEnabled() && rsp_mode != GET_NONE) {
                        log.trace("received all responses: " + toString());
                    }
                    return true;
                }
                timeout=timeout_time - System.currentTimeMillis();
                if(timeout > 0) {
                    completed.await(timeout, TimeUnit.MILLISECONDS);
                }
            }
            if(corr != null) {
                corr.done(req_id);
            }
            if(log.isTraceEnabled())
                log.trace("timed out waiting for responses");

            return false;
        }
    }

    protected boolean waitForResults(long timeout)  {
        if(timeout <= 0) {
            while(true) { /* Wait for responses: */
                adjustMembership(); // may not be necessary, just to make sure...
                if(responsesComplete())
                    return true;
                try {completed.await();} catch(Exception e) {}
            }
        }
        else {
            long start_time=System.currentTimeMillis();
            long timeout_time=start_time + timeout;
            while(timeout > 0) { /* Wait for responses: */
                if(responsesComplete())
                    return true;
                timeout=timeout_time - System.currentTimeMillis();
                if(timeout > 0) {
                    try {completed.await(timeout, TimeUnit.MILLISECONDS);} catch(Exception e) {}
                }
            }
            return false;
        }
    }



    public static String modeToString(int m) {
        switch(m) {
            case GET_FIRST: return "GET_FIRST";
            case GET_ALL: return "GET_ALL";
            case GET_MAJORITY: return "GET_MAJORITY";
            case GET_ABS_MAJORITY: return "GET_ABS_MAJORITY";
            case GET_N: return "GET_N";
            case GET_NONE: return "GET_NONE";
            default: return "<unknown> (" + m + ")";
        }
    }


}
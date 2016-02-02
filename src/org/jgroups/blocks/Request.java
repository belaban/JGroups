package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.CondVar;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Util;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Abstract class for a unicast or multicast request
 *
 * @author Bela Ban
 */
public abstract class Request implements NotifyingFuture {
    protected static final Log        log=LogFactory.getLog(Request.class);
    protected long                    req_id;
    protected final Lock              lock=new ReentrantLock();
    /** Is set as soon as the request has received all required responses */
    protected final CondVar           cond=new CondVar(lock);
    protected final RequestCorrelator corr;         // either use RequestCorrelator or ...
    protected final RequestOptions    options;
    protected volatile boolean        done;
    protected volatile FutureListener listener;


    
    public Request(RequestCorrelator corr, RequestOptions options) {
        this.corr=corr;
        this.options=options;
    }

    public Request requestId(long req_id) {this.req_id=req_id; return this;}
    public long    requestId()            {return req_id;}

    public Request setResponseFilter(RspFilter filter) {
        options.setRspFilter(filter);
        return this;
    }


    public Request setListener(FutureListener listener) {
        this.listener=listener;
        if(done)
            listener.futureDone(this);
        return this;
    }

    public boolean execute(final Message req, boolean block_for_results) throws Exception {
        if(corr == null) {
            log.error(Util.getMessage("CorrIsNullCannotSendRequest"));
            return false;
        }

        sendRequest(req);
        if(!block_for_results || options.getMode() == ResponseMode.GET_NONE)
            return true;

        lock.lock();
        try {
            return responsesComplete(options.getTimeout());
        }
        finally {
            done=true;
            lock.unlock();
        }
    }

    protected abstract void sendRequest(final Message request_msg) throws Exception;

    public abstract void receiveResponse(Object response_value, Address sender, boolean is_exception);

    public abstract void viewChange(View new_view);

    public abstract void suspect(Address mbr);

    public abstract void siteUnreachable(String site);

    public abstract void transportClosed();

    protected abstract boolean responsesComplete();


    public boolean getResponsesComplete() {
        lock.lock();
        try {
            return responsesComplete();
        }
        finally {
            lock.unlock();
        }
    }


    public boolean cancel(boolean mayInterruptIfRunning) {
        lock.lock();
        try {
            boolean retval=!done;
            done=true;
            if(corr != null && this.req_id > 0)
                corr.done(this.req_id);
            cond.signal(true);
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
        return String.format("%s, mode=%s", getClass().getSimpleName(), options.getMode());
    }


    /* --------------------------------- Private Methods -------------------------------------*/


    protected void checkCompletion(Future future) {
        if(listener != null && responsesComplete())
            listener.futureDone(future);
    }


    /** This method runs with lock locked (called by {@code execute()}). */
    @GuardedBy("lock")
    protected boolean responsesComplete(final long timeout) throws InterruptedException {
        try {
            return waitForResults(timeout);
        }
        finally {
            if(corr != null && this.req_id > 0)
                corr.done(this.req_id);
        }
    }

    @GuardedBy("lock")
    protected boolean waitForResults(final long timeout)  {
        if(timeout <= 0) {
            cond.waitFor(this::responsesComplete);
            return true;
        }
        return cond.waitFor(this::responsesComplete, timeout, TimeUnit.MILLISECONDS);
    }


}
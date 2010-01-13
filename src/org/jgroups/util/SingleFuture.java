package org.jgroups.util;

import org.jgroups.blocks.UnicastRequest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements a future with a single result (versus a RspList)
 * @author Bela Ban
 * @version $Id: SingleFuture.java,v 1.2 2010/01/13 13:18:24 belaban Exp $
 */
public class SingleFuture<T> implements Future<T>  {
    protected final UnicastRequest req;

    public SingleFuture(UnicastRequest req) {
        if(req == null)
            throw new IllegalArgumentException("argument cannot be null");
        this.req=req;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return req.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
        return req.isCancelled();
    }

    public boolean isDone() {
        return req.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
        Rsp rsp=req.get();
        return rsp != null? (T)rsp.getValue() : null;
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Rsp rsp=req.get(timeout, unit);
        return rsp != null? (T)rsp.getValue() : null;
    }

}

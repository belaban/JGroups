package org.jgroups.util;

import org.jgroups.blocks.GroupRequest;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implements a future with a single result (versus a RspList)
 * @author Bela Ban
 * @version $Id: SingleFuture.java,v 1.1 2009/06/16 08:25:18 belaban Exp $
 */
public class SingleFuture<T> implements Future<T>  {
    protected final GroupRequest req;

    public SingleFuture(GroupRequest req) {
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
        RspList rsps=req.get();
        return _get(rsps);
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        RspList rsps=req.get(timeout, unit);
        return _get(rsps);
    }

    private T _get(RspList rsps) {
        if(rsps == null || rsps.isEmpty())
            return null;
        return (T)rsps.getFirst();
    }
}

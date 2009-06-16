package org.jgroups.util;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Bela Ban
 * @version $Id: NullFuture.java,v 1.2 2009/06/16 08:29:14 belaban Exp $
 */
public class NullFuture<T> implements Future<RspList> {
    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }

    public boolean isCancelled() {
        return true;
    }

    public boolean isDone() {
        return true;
    }

    public RspList get() throws InterruptedException, ExecutionException {
        return new RspList();
    }

    public RspList get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return new RspList();
    }
}

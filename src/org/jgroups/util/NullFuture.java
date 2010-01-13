package org.jgroups.util;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Bela Ban
 * @version $Id: NullFuture.java,v 1.3 2010/01/13 13:18:00 belaban Exp $
 */
public class NullFuture<T> implements Future<T> {
    final T retval;

    public NullFuture(T retval) {
        this.retval=retval;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }

    public boolean isCancelled() {
        return true;
    }

    public boolean isDone() {
        return true;
    }

    public T get() throws InterruptedException, ExecutionException {
        return retval;
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return retval;
    }
}

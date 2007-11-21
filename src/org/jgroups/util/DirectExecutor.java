package org.jgroups.util;

import java.util.concurrent.*;
import java.util.List;
import java.util.Collection;

/**
 * @author Bela Ban
 * @version $Id: DirectExecutor.java,v 1.4 2007/11/21 13:02:00 belaban Exp $
 */
public class DirectExecutor implements ExecutorService {
    public void execute(Runnable command) {
        command.run();
    }


    public void shutdown() {
    }

    public List<Runnable> shutdownNow() {
        return null;
    }

    public boolean isShutdown() {
        return false;
    }

    public boolean isTerminated() {
        return false;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public <T> Future<T> submit(Callable<T> task) {
        return null;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        return null;
    }

    public Future<?> submit(Runnable task) {
        return null;
    }

    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks) throws InterruptedException {
        return null;
    }

    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    public <T> T invokeAny(Collection<Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return null;
    }

    public <T> T invokeAny(Collection<Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }
}

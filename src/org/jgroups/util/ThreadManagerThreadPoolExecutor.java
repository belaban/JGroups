package org.jgroups.util;

import java.util.concurrent.*;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadPoolExecutor subclass that implements @{link ThreadManager}.
 * @author Brian Stansberry
 * @version $Id: ThreadManagerThreadPoolExecutor.java,v 1.1.2.1 2008/05/26 09:14:39 belaban Exp $
 */
public class ThreadManagerThreadPoolExecutor extends ThreadPoolExecutor implements ThreadManager {
    private ThreadDecorator decorator;

    public ThreadManagerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public ThreadManagerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public ThreadManagerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public ThreadManagerThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                           BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    public ThreadDecorator getThreadDecorator() {
        return decorator;
    }

    public void setThreadDecorator(ThreadDecorator decorator) {
        this.decorator=decorator;
    }

    /**
     * Invokes {@link ThreadDecorator#threadReleased(Thread)} on the current thread.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            super.afterExecute(r, t);
        }
        finally {
            if(decorator != null)
                decorator.threadReleased(Thread.currentThread());
        }
    }

}

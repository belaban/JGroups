package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ThreadPool;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Tests {@link ThreadPool}
 * @author Radoslav Husar
 */
@Test(groups=Global.FUNCTIONAL)
public class ThreadPoolTest {

    /**
     * A rejection policy of "discard" drops a task without raising an exception; execute() must not report such a
     * task as accepted, or else callers (e.g. MaxOneThreadPerSender) wait forever for a task which will never run
     */
    public void testDiscardingRejectionPolicy() throws Exception {
        ThreadPool pool=new ThreadPool().setMinThreads(1).setMaxThreads(1);
        pool.setRejectionPolicy("discard");
        pool.init();
        final CountDownLatch latch=new CountDownLatch(1);
        try {
            Runnable sleeper=() -> {
                try {
                    latch.await();
                }
                catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            assert pool.execute(sleeper);
            assert !pool.execute(() -> {}) : "the task was discarded, but execute() returned true";
            assert pool.numberOfRejectedMessages() == 1 : "rejected messages: " + pool.numberOfRejectedMessages();
        }
        finally {
            latch.countDown();
            pool.destroy();
        }
    }

    /**
     * Tests the case when a {@link java.util.concurrent.RejectedExecutionHandler} itself throws an exception
     */
    public void testExceptionThrowingRejectionHandler() throws Exception {
        ThreadPool pool=new ThreadPool().setMinThreads(1).setMaxThreads(1);
        pool.init();
        RejectedExecutionHandler h=new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                throw new RuntimeException("boom");
            }
        };
        final CountDownLatch latch=new CountDownLatch(1);
        try {
            Runnable sleeper=() -> {
                try {
                    latch.await();
                }
                catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            assert pool.execute(sleeper);
            assert !pool.execute(() -> {}, h) : "the task was discarded, but execute() returned true";
            assert pool.numberOfRejectedMessages() == 1 : "rejected messages: " + pool.numberOfRejectedMessages();
        }
        finally {
            latch.countDown();
            pool.destroy();
        }
    }


    /**
     * A shut-down pool rejects every task, but ShutdownRejectedExecutionHandler swallows the rejection rather than
     * raising an exception; execute() must not report such a task as accepted
     */
    public void testExecuteAfterShutdown() throws Exception {
        ThreadPool pool=new ThreadPool();
        pool.init();
        pool.destroy();
        assert !pool.execute(() -> {}) : "the pool is shut down, but execute() returned true";
    }
}

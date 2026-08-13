package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ThreadPool;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
        final CountDownLatch started=new CountDownLatch(1), release=new CountDownLatch(1);
        try {
            assert pool.execute(() -> {
                started.countDown();
                try {release.await(30, TimeUnit.SECONDS);}
                catch(InterruptedException ignored) {}
            });
            assert started.await(10, TimeUnit.SECONDS);
            assert !pool.execute(() -> {}) : "the task was discarded, but execute() returned true";
            assert pool.numberOfRejectedMessages() == 1 : "rejected messages: " + pool.numberOfRejectedMessages();
        }
        finally {
            release.countDown();
            pool.destroy();
        }
    }
}

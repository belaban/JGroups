package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.NoProgressException;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RejectionPoliciesTest {
    public void testCustomPolicy() {
        BlockingQueue<Runnable> queue = new SynchronousQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        RejectedExecutionHandler handler = Util.parseRejectionPolicy("custom=org.jgroups.tests.RejectionPoliciesTest$FooPolicy");
        executor.setRejectedExecutionHandler(handler);

        BlockingRunnable blocker = new BlockingRunnable();
        executor.execute(blocker);
        try {
            executor.execute(new NorunRunnable());
            assert false;
        } catch (FooException foo) {

        } catch (Throwable t) {
            assert false;
        } finally {
            blocker.stop = true;
        }
    }

    public void testDeadlockDetectionPolicy1() {
        BlockingQueue<Runnable> queue = new SynchronousQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        RejectedExecutionHandler handler = Util.parseRejectionPolicy("progress_check");
        executor.setRejectedExecutionHandler(handler);

        BlockingRunnable blocker = new BlockingRunnable();
        executor.execute(blocker);
        executor.execute(new NorunRunnable()); // should silently fail
        try {
            Thread.sleep(11000);
        } catch (InterruptedException e) {
            assert false;
        }
        try {
            executor.execute(new NorunRunnable());
            assert false;
        } catch (NoProgressException e) {

        } catch (Throwable t) {
            assert false;
        } finally {
            blocker.stop = true;
        }
    }

    public void testDeadlockDetectionPolicy2() {
        BlockingQueue<Runnable> queue = new SynchronousQueue<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, queue);
        RejectedExecutionHandler handler = Util.parseRejectionPolicy(
                "progress_check=period:15000,fallback:custom=org.jgroups.tests.RejectionPoliciesTest$FooPolicy");
        executor.setRejectedExecutionHandler(handler);

        BlockingRunnable blocker = new BlockingRunnable();
        executor.execute(blocker);

        try {
            executor.execute(new NorunRunnable()); // should fail with fallback = FooPolicy
            assert false;
        } catch (FooException foo) {
        } catch (Throwable t) {
            assert false;
        }

        try {
            Thread.sleep(11000);
        } catch (InterruptedException e) {
            assert false;
        }

        try {
            executor.execute(new NorunRunnable()); // non-default period: still FooPolicy
            assert false;
        } catch (FooException foo) {
        } catch (Throwable t) {
            assert false;
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            assert false;
        }

        try {
            executor.execute(new NorunRunnable());
            assert false;
        } catch (NoProgressException e) {

        } catch (Throwable t) {
            assert false;
        } finally {
            blocker.stop = true;
        }
    }

    public static class FooPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            throw new FooException();
        }
    }

    public static class FooException extends RuntimeException {}

    public static class BlockingRunnable implements Runnable {
        public volatile boolean stop = false;
        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    public static class NorunRunnable implements Runnable {
        @Override
        public void run() {
            assert false; // this shouldn't be executed
        }
    }
}

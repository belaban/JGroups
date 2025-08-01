package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.FastArray;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jgroups.util.Runner.State.*;

/**
 * @author Bela Ban
 * @since  4.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RunnerTest {
    protected Runner              runner;
    protected final AtomicInteger count=new AtomicInteger();
    protected final List<Thread>  threads=new FastArray<>();

    @BeforeMethod protected void init() {count.set(0); threads.clear();}

    @AfterMethod protected void destroy() {
        Util.close(runner); // calls runner.stop() if non-null
    }

    protected void increment() {
        int old_val=count.incrementAndGet();
        System.out.printf("[%s] incremented count from %d\n", Thread.currentThread(), old_val);
        Util.sleep(60_000);
    }

    protected void delay(final List<String> msgs) {
        Thread curr=Thread.currentThread();
        synchronized(threads) {
            boolean contains=threads.stream().anyMatch(t -> t == curr);
            if(!contains)
                threads.add(curr);
        }
        sleep(60_000, msgs);
        sleep(2000, msgs);
    }

    protected static void sleep(long timeout, List<String> msgs) {
        long start=System.nanoTime();
        try {
            Thread.sleep(timeout);
            long time=System.nanoTime()-start;
            msgs.add(String.format("slept(%s), actual(%s)", Util.printTime(timeout, TimeUnit.MILLISECONDS), Util.printTime(time)));
        }
        catch(InterruptedException e) {
            long time=System.nanoTime()-start;
            msgs.add(String.format("sleep(%s), actual(%s)",
                                   Util.printTime(timeout, TimeUnit.MILLISECONDS),
                                   Util.printTime(time)));
        }
    }

    public void testStartTwice() throws TimeoutException {
        runner=createRunner(this::increment, null);
        runner.start();
        runner.start(); // should not start a second runner
        Util.waitUntil(3000, 100, () -> count.get() == 1);
        assert runner.isRunning();
        runner.stop();
        Util.waitUntil(5000, 100, () -> !runner.isRunning());
    }

    //@Test(invocationCount=5)
    public void testTransitions() throws TimeoutException {
        final List<String> msgs=new FastArray<>(20);
        runner=createRunner(() -> delay(msgs), null);
        assertState(stopped);
        runner.stop();
        assertState(stopped);

        runner.start();
        assertState(running);
        runner.start();
        assertState(running);
        Util.waitUntilTrue(5000, 100, () -> !threads.isEmpty());

        runner.stop();
        assertState(stopping);
        runner.start();
        assertState(running);
        Thread worker=runner.getThread();
        assert worker != null && worker.isAlive();

        msgs.clear();
        runner.stop();
        Util.waitUntilTrue(5000, 100, () -> runner.state() == stopped);
        assertState(stopped);
        System.out.printf("-- msgs: %s\n", msgs);
    }

    public void testStop() throws TimeoutException {
        final List<String> msgs=new FastArray<>(20);
        runner=createRunner(() -> delay(msgs), null)
          .start();
        assert runner.state() == running;
        Util.waitUntilTrue(5000, 100, () -> !threads.isEmpty());
        runner.stop();
        Util.waitUntil(5000, 100, () -> runner.state() == stopped);
    }

    /**
     * Tests the ABA problem (https://issues.redhat.com/browse/JGRP-2919)
     */
    public void testStartStopStartProblem() throws TimeoutException {
        final List<String> msgs=new FastArray<>(20);
        runner=createRunner(() -> delay(msgs), null)
          .joinTimeout(0); // doesn't wait until the thread terminates
        runner.start();
        Util.waitUntil(3000, 100, () -> threads.size() == 1);
        assert runner.isRunning();
        runner.stop();
        runner.start();
        assert runner.isRunning();
        Util.waitUntilTrue(3000, 100, () -> threads.size() > 1);
        assert threads.size() == 1;
    }

    protected static Runner createRunner(Runnable func, Runnable stop_func) {
        return new Runner(new DefaultThreadFactory("RunnerTest", false, true),
                          "runner", func, stop_func);
    }

    protected void assertState(Runner.State expected) {
        assert expected == runner.state() : String.format("expected %s but got %s", expected, runner.state());
    }
}

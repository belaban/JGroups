package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
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

    /**
     * Tests stopping a runner that's in a method (e.g. ServerSocket.accept()) which cannot be interrupted
     */
    public void testStopWithUninterruptibleMethod() throws IOException, TimeoutException {
        final ServerSocket srv_sock=new ServerSocket(0);
        Runnable stop_fun=() -> Util.close(srv_sock);
        Runnable run=() -> {
            try(Socket client_sock=srv_sock.accept(); InputStream in=client_sock.getInputStream()) {
                System.out.printf("-- accepted connection from %s\n", client_sock.getRemoteSocketAddress());
                in.read();
            }
            catch(Throwable t) {
                System.out.printf("socket exception: %s", t.getMessage());
            }
        };

        try {
            ThreadFactory thread_factory=new DefaultThreadFactory("acceptor", false, true);
            runner=new Runner(thread_factory, "acceptor", run, stop_fun);
            runner.start();
            Util.waitUntilTrue(3000, 100, () -> runner.isRunning() && runner.getThread().isAlive());
            final Thread t=runner.getThread();
            runner.stop();
            assert !runner.isRunning();
            Util.waitUntil(3000, 500,
                           () -> !t.isAlive() && t.getState() == Thread.State.TERMINATED,
                           () -> String.format("thread.state: %s", t.getState()));
        }
        finally {
            Util.close(srv_sock);
        }
    }

    protected static Runner createRunner(Runnable func, Runnable stop_func) {
        return new Runner(new DefaultThreadFactory("RunnerTest", false, true),
                          "runner", func, stop_func);
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

    protected void assertState(Runner.State expected) {
        assert expected == runner.state() : String.format("expected %s but got %s", expected, runner.state());
    }
}

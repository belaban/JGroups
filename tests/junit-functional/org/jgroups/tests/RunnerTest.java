package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 * @since  4.0.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RunnerTest {
    protected Runner runner;
    protected final AtomicInteger count=new AtomicInteger();

    @BeforeMethod protected void init() {count.set(0);}

    @AfterMethod protected void destroy() {
        if(runner != null)
            runner.stop();
        count.set(0);
    }

    protected void increment() {
        count.incrementAndGet();
        Util.sleep(10);
    }

    public void testStartTwice() {
        runner=createRunner(this::increment, null);
        runner.start();
        runner.start(); // should not start a second runner
        for(int i=0; i < 10; i++) {
            if(count.get() >= 5)
                break;
            Util.sleep(100);
        }
        assert runner.isRunning();
        runner.stop();
        assert !runner.isRunning();
    }


    protected Runner createRunner(Runnable func, Runnable stop_func) {
        return new Runner(new DefaultThreadFactory("RunnerTest", false, true),
                          "runner", func, stop_func);
    }
}

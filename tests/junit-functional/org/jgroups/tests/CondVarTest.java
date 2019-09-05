package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.CondVar;
import org.jgroups.util.Condition;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @since  3.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class CondVarTest {
    protected CondVar          cond;
    protected volatile boolean done=false; // our condition
    protected Condition        condition=() -> done;

    @BeforeMethod protected void setup() {
        cond=new CondVar();
        done=false;
    }

    public void testWaitFor() {
        done=true;
        cond.waitFor(condition); // needs to return immediately
    }

    public void testInterruptedWaitFor() {
        Thread.currentThread().interrupt();
        done=true;
        cond.waitFor(condition); // needs to return immediately
        assert Thread.currentThread().isInterrupted();
    }

    public void testWaitForWithSignal() {
        signal(1000, true, true);
        cond.waitFor(condition);
    }

    public void testWaitForWithSignalAndInterrupt() {
        signal(2000, true, true);
        interrupt(Thread.currentThread(), 200);
        cond.waitFor(condition);
    }

    public void testTimedWaitForWithNegativeTimeout() {
        boolean result=cond.waitFor(() -> done, 0, TimeUnit.SECONDS);
        assert !done && !result;
        result=cond.waitFor(condition, -1, TimeUnit.SECONDS);
        assert !done && !result;
    }

    public void testTimedWaitFor() {
        boolean result=cond.waitFor(condition, 1, TimeUnit.SECONDS);
        assert !done && !result;
        interrupt(Thread.currentThread(), 100);
        result=cond.waitFor(condition, 1, TimeUnit.SECONDS);
        assert !done && !result;
        signal(1000, true, true);
        result=cond.waitFor(condition, 5, TimeUnit.SECONDS);
        assert done && result;

        done=false;
        interrupt(Thread.currentThread(), 1);
        signal(1000, true, false);
        result=cond.waitFor(condition, 5, TimeUnit.SECONDS);
        assert result && done && Thread.currentThread().isInterrupted();
    }



    protected void signal(final long after_ms, final boolean flag, boolean signal_all) {
        new Thread(() -> {
            Util.sleep(after_ms);
            done=flag;
            System.out.println("signalling cond-var");
            cond.signal(signal_all);
        }).start();
    }

    protected static void interrupt(final Thread target_thread, final long after_ms) {
        new Thread(() -> {
            Util.sleep(after_ms);
            System.out.println("interrupting " + target_thread);
            target_thread.interrupt();
        }).start();
    }
}

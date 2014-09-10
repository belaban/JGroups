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
public class CondVarTest implements Condition {
    protected CondVar          cond;
    protected volatile boolean done=false; // our condition

    @BeforeMethod protected void setup() {
        cond=new CondVar();
        done=false;
    }

    public void testWaitFor() {
        done=true;
        cond.waitFor(this); // needs to return immediately
    }

    public void testInterruptedWaitFor() {
        Thread.currentThread().interrupt();
        done=true;
        cond.waitFor(this); // needs to return immediately
        assert Thread.currentThread().isInterrupted();
    }

    public void testWaitForWithSignal() {
        signal(1000, true, true);
        cond.waitFor(this);
        assert isMet();
    }

    public void testWaitForWithSignalAndInterrupt() {
        signal(2000, true, true);
        interrupt(Thread.currentThread(), 200);
        cond.waitFor(this);
        assert isMet();
    }

    public void testTimedWaitForWithNegativeTimeout() {
        boolean result=cond.waitFor(this, 0, TimeUnit.SECONDS);
        assert !done && !result;
        result=cond.waitFor(this, -1, TimeUnit.SECONDS);
        assert !done && !result;
    }

    public void testTimedWaitFor() {
        boolean result=cond.waitFor(this, 1, TimeUnit.SECONDS);
        assert !done && !result;
        interrupt(Thread.currentThread(), 100);
        result=cond.waitFor(this, 1, TimeUnit.SECONDS);
        assert !done && !result;
        signal(1000, true, true);
        result=cond.waitFor(this, 5, TimeUnit.SECONDS);
        assert done && result;

        done=false;
        interrupt(Thread.currentThread(), 1);
        signal(1000, true, false);
        result=cond.waitFor(this, 5, TimeUnit.SECONDS);
        assert result && done && Thread.currentThread().isInterrupted();
    }




    public boolean isMet() {
        return done;
    }

    protected void signal(final long after_ms, final boolean flag, boolean signal_all) {
        new Thread() {
            public void run() {
                Util.sleep(after_ms);
                done=flag;
                System.out.println("signalling cond-var");
                cond.signal(true);
            }
        }.start();
    }

    protected void interrupt(final Thread target_thread, final long after_ms) {
        new Thread() {
            public void run() {
                Util.sleep(after_ms);
                System.out.println("interrupting " + target_thread);
                target_thread.interrupt();
            }
        }.start();
    }
}

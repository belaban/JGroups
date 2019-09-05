
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;


/**
 * Various test cases for Promise
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class PromiseTest {

    public void testGetWithResultAvailable() {
        final Promise<Integer> p=new Promise<>();
        p.setResult(22);
        assert p.getResult() == 22;
    }

    public void testGetResultNoTimeout() {
        final Promise p=new Promise();
        Object result;
        new ResultSetter(p, 500).start();
        result=p.getResult(0);
        Assert.assertEquals(Boolean.TRUE, result);
    }

    public void testGetResultWithReset() {
        final Promise<Integer> p=new Promise<>();
        p.setResult(22);
        assert p.getResult() == 22;
        assert p.getResult(1, true) == 22;
        assert !p.hasResult();
        assert p.getResult(1) == null;
    }


    public final void testRepeatedGet() {
        final Promise<Integer> p=new Promise<>();
        p.setResult(10);
        System.out.println("p: " + p);
        Integer result=p.getResult(100);
        System.out.println("result = " + result);

        result=p.getResult(100);
        System.out.println("result = " + result);
    }


    public void testGetResultNoTimeout_ResultAlreadySet() {
        final Promise p=new Promise();
        Object result;
        new ResultSetter(p, 1).start();
        Util.sleep(100);
        result=p.getResult(0);
        Assert.assertEquals(Boolean.TRUE, result);
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testGetResultWithTimeout() throws TimeoutException {
        final Promise p=new Promise();
        p.getResultWithTimeout(500);
    }



    public void testGetResultWithTimeoutNoException() {
        final Promise p=new Promise();
        Object ret=p.getResult(500);
        assert ret == null;
    }


    public void testGetResultWithTimeoutAndInterrupt() {
        final Promise p=new Promise();
        new Interrupter(Thread.currentThread(), 100).start();
        Object result=p.getResult(500);
        assert result == null;
    }


    public void testGetResultWithTimeoutAndResultSetter() {
        final Promise p=new Promise();
        Thread t=new Thread(() -> {
            Util.sleep(500);
            System.out.println("-- setting promise to \"Bela\"");
            p.setResult("Bela");
        });
        t.start();
        long start=System.currentTimeMillis(), stop;
        Object result=p.getResult(30000);
        stop=System.currentTimeMillis();
        System.out.println("-- waited for " + (stop-start) + "ms, result is " + result);
        assert result != null;
        Assert.assertEquals("Bela", result);
        assert p.hasResult();
        assert p.getResult().equals("Bela");
    }


    public void testReset() throws TimeoutException {
        final Promise p=new Promise();
        Resetter resetter=new Resetter(p, 2000);
        resetter.start();
        Object result=p.getResultWithTimeout(5000);
        System.out.println("result = " + result);
        assert result == null;
    }

    public void testReset2() throws TimeoutException {
        final Promise p=new Promise();
        Resetter resetter=new Resetter(p, 2000);
        resetter.start();
        Object result=p.getResultWithTimeout(5000, true);
        System.out.println("result = " + result);
        assert result == null;
    }



    static class ResultSetter extends Thread {
        long wait_time=2000;
        Promise target=null;

        ResultSetter(Promise target, long wait_time) {
            this.target=target;
            this.wait_time=wait_time;
        }

        public void run() {
            Util.sleep(wait_time);
            target.setResult(Boolean.TRUE);
        }
    }


    static class Interrupter extends Thread {
        long wait_time=2000;
        Thread target=null;

        Interrupter(Thread target, long wait_time) {
            this.target=target;
            this.wait_time=wait_time;
        }

        public void run() {
            Util.sleep(wait_time);
            target.interrupt();
        }
    }


    static class Resetter extends Thread {
        protected final Promise<?> target;
        protected final long       wait_time;

        public Resetter(Promise<?> target, long wait_time) {
            this.target=target;
            this.wait_time=wait_time;
        }

        public void run() {
            Util.sleep(wait_time);
            // target.reset();
            target.setResult(null);
        }
    }


 

}

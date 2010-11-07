// $Id: PromiseTest.java,v 1.4 2008/03/10 15:39:19 belaban Exp $

package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.TimeoutException;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Various test cases for Promise
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class PromiseTest {

    public static void testGetResultNoTimeout() {
        final Promise p=new Promise();
        Object result;
        new ResultSetter(p, 500).start();
        result=p.getResult(0);
        Assert.assertEquals(Boolean.TRUE, result);
    }


    public static void testGetResultNoTimeout_ResultAlreadySet() {
        final Promise p=new Promise();
        Object result;
        new ResultSetter(p, 1).start();
        Util.sleep(100);
        result=p.getResult(0);
        Assert.assertEquals(Boolean.TRUE, result);
    }

    @Test(expectedExceptions=TimeoutException.class)
    public static void testGetResultWithTimeout() throws TimeoutException {
        final Promise p=new Promise();
        p.getResultWithTimeout(500);
    }



    public static void testGetResultWithTimeoutNoException() {
        final Promise p=new Promise();
        Object ret=p.getResult(500);
        assert ret == null;
    }


    public static void testGetResultWithTimeoutAndInterrupt() {
        final Promise p=new Promise();
        new Interrupter(Thread.currentThread(), 100).start();
        Object result=p.getResult(500);
        assert result == null;
    }



    public static void testGetResultWithTimeoutAndResultSetter() {
        final Promise p=new Promise();
        Thread t=new Thread() {
            public void run() {
                Util.sleep(500);
                System.out.println("-- setting promise to \"Bela\"");
                p.setResult("Bela");
            }
        };
        t.start();
        long start=System.currentTimeMillis(), stop;
        Object result=p.getResult(100000);
        stop=System.currentTimeMillis();
        System.out.println("-- waited for " + (stop-start) + "ms, result is " + result);
        assert result != null;
        Assert.assertEquals("Bela", result);
        assert !(p.hasResult()) : "promise was reset after getResult()";
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


 

}

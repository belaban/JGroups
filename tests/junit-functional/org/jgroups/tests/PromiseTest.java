// $Id: PromiseTest.java,v 1.2.2.1 2007/11/20 08:37:23 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.TimeoutException;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;


/**
 * Various test cases for Promise
 * @author Bela Ban
 */
public class PromiseTest extends TestCase {
    Promise p;

    public PromiseTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        p=new Promise();
    }


    public void tearDown() throws Exception {
        p.reset();
        super.tearDown();
    }


    public void testGetResultNoTimeout() {
        Object result;
        new ResultSetter(p, 500).start();
        result=p.getResult(0);
        assertEquals(Boolean.TRUE, result);
    }

    public void testGetResultNoTimeout_ResultAlreadySet() {
        Object result;
        new ResultSetter(p, 1).start();
        Util.sleep(100);
        result=p.getResult(0);
        assertEquals(Boolean.TRUE, result);
    }

    public void testGetResultWithTimeout() {
        try {
            p.getResultWithTimeout(500);
            fail("this should throw a TimeoutException");
        }
        catch(TimeoutException e) {
            assertNotNull(e);
        }
    }


    public void testGetResultWithTimeoutNoException() {
        Object ret=p.getResult(500);
        assertNull(ret);
    }

    public void testGetResultWithTimeoutAndInterrupt() {
        new Interrupter(Thread.currentThread(), 100).start();
        Object result=p.getResult(500);
        assertNull(result);
    }


    public void testGetResultWithTimeoutAndResultSetter() {
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
        assertNotNull(result);
        assertEquals("Bela", result);
        assertFalse("promise was reset after getResult()", p.hasResult());
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

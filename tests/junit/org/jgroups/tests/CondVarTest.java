// $Id: CondVarTest.java,v 1.1 2004/09/22 10:32:41 belaban Exp $

package org.jgroups.tests;


import EDU.oswego.cs.dl.util.concurrent.FutureResult;
import EDU.oswego.cs.dl.util.concurrent.TimeoutException;
import junit.framework.TestCase;
import org.jgroups.util.Util;
import org.jgroups.util.CondVar;

import java.lang.reflect.InvocationTargetException;


/**
 * Various test cases for CondVar
 * @author Bela Ban
 */
public class CondVarTest extends TestCase {
    CondVar cond=new CondVar("blocking", Boolean.FALSE);


    public CondVarTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testConditionTrue() {
        try {
            cond.waitUntil(Boolean.FALSE, 500);
        }
        catch(org.jgroups.TimeoutException e) {
            fail("received TimeoutException");
        }
    }

    public void testConditionTrueWaitForever() {
        cond.waitUntil(Boolean.FALSE);
    }


    public void testWithTimeoutException() {
        try {
            cond.waitUntil(Boolean.TRUE, 500);
            fail("expected timeout exception");
        }
        catch(org.jgroups.TimeoutException e) {
        }
    }


    public void testWithResultSetter() throws org.jgroups.TimeoutException {
        new ResultSetter(cond, 500).start();
        cond.waitUntil(Boolean.TRUE,  2000);
    }

    public void testWithResultSetter_ResultSetBeforeAccess() throws org.jgroups.TimeoutException {
        new ResultSetter(cond, 10).start();
        Util.sleep(100);
        cond.waitUntil(Boolean.TRUE,  2000);
    }




    class ResultSetter extends Thread {
        long wait_time=2000;
        CondVar target=null;

        ResultSetter(CondVar target, long wait_time) {
            this.target=target;
            this.wait_time=wait_time;
        }

        public void run() {
            Util.sleep(wait_time);
            System.out.println("-- [ResultSetter] set result to true");
            target.set(Boolean.TRUE);
            System.out.println("-- [ResultSetter] set result to true -- DONE");
        }
    }



    public static void main(String[] args) {
        String[] testCaseName={CondVarTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}

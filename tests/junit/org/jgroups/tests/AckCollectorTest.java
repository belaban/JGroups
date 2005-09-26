// $Id: AckCollectorTest.java,v 1.1 2005/09/26 08:39:43 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.util.AckCollector;
import org.jgroups.util.Util;
import org.jgroups.TimeoutException;

import java.util.ArrayList;
import java.util.List;


public class AckCollectorTest extends TestCase {
    List l=new ArrayList(5);
    AckCollector ac;

    public AckCollectorTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        l.add("one");
        l.add("two");
        l.add("three");
        l.add("four");
        l.add("five");
        ac=new AckCollector(l);
    }


    public void tearDown() throws Exception {
        super.tearDown();
        l.clear();
    }

    public void testConstructor() {
        System.out.println("AckCollector is " + ac);
        assertEquals(5, ac.size());
    }


    public void testWaitForAllAcksNoTimeout() {
        new Thread() {
            public void run() {
                ac.ack("one");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("two");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("three");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("four");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("five");
                System.out.println("AckCollector: " + ac);
            }
        }.start();
        ac.waitForAllAcks();
        assertEquals(0, ac.size());
    }

    public void testWaitForAllAcksWithTimeoutException() {
        try {
            ac.waitForAllAcks(200);
            fail("we should get a timeout exception here");
        }
        catch(TimeoutException e) {
            System.out.println("received timeout exception, as expected");
        }
    }


    public void testWaitForAllAcksWithTimeout() {
        new Thread() {
            public void run() {
                ac.ack("one");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("two");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("three");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("four");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("five");
                System.out.println("AckCollector: " + ac);
            }
        }.start();
        try {
            ac.waitForAllAcks(1000);
            assertTrue("we should not get a timeout exception here", true);
        }
        catch(TimeoutException e) {
            fail("we should not get a timeout exception here");
        }
        assertEquals(0, ac.size());
    }

    public void testWaitForAllAcksWithTimeoutException2() {
        new Thread() {
            public void run() {
                ac.ack("one");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("two");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("three");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("four");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("five");
                System.out.println("AckCollector: " + ac);
            }
        }.start();
        try {
            ac.waitForAllAcks(300);
            fail("we should get a timeout exception here");
        }
        catch(TimeoutException e) {
            assertTrue("we should get a timeout exception here", true);
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={AckCollectorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}

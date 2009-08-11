// $Id: AckCollectorTest.java,v 1.1.4.1 2009/08/11 11:28:13 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.util.AckCollector;
import org.jgroups.util.Util;
import org.jgroups.TimeoutException;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

import java.util.ArrayList;
import java.util.List;
import java.net.UnknownHostException;


public class AckCollectorTest extends TestCase {
    List l=new ArrayList(5);
    AckCollector ac;
    private List new_list=new ArrayList(3);

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
        new_list.add("six");
        new_list.add("seven");
        new_list.add("eight");
    }


    public void tearDown() throws Exception {
        super.tearDown();
        l.clear();
        new_list.clear();
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


    public void testReset() {
        new Thread() {
            public void run() {
                Util.sleep(500);
                System.out.println("resetting AckCollector");
                ac.reset(new_list);
                System.out.println("reset AckCollector: " + ac);
            }
        }.start();
        System.out.println("initial AckCollector: " + ac);
        try {
            ac.waitForAllAcks(1000);
            fail("needs to throw TimeoutException");
        }
        catch(TimeoutException e) {
            assertTrue("expected TimeoutException", e != null);
        }
        System.out.println("new AckCollector: " + ac);
    }


    public void testReset2() throws TimeoutException {
        new Thread() {
            public void run() {
                Util.sleep(500);
                System.out.println("resetting AckCollector");
                ac.reset(new_list);
                System.out.println("reset AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("six");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("seven");
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack("eight");
                System.out.println("AckCollector: " + ac);
            }
        }.start();
        System.out.println("initial AckCollector: " + ac);
        ac.waitForAllAcks(2000);
        System.out.println("new AckCollector: " + ac);
    }

    public void testNullList() throws TimeoutException {
        AckCollector coll=new AckCollector();
        coll.waitForAllAcks(1000);
    }

    public void testOneList() throws TimeoutException, UnknownHostException {
        List tmp=new ArrayList();
        Address addr=new IpAddress("127.0.0.1", 5555);
        tmp.add(addr);
        AckCollector coll=new AckCollector(tmp);
        coll.ack(addr);
        coll.waitForAllAcks(1000);
    }


    public static void main(String[] args) {
        String[] testCaseName={AckCollectorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}

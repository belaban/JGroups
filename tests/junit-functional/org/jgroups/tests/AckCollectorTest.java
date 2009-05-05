
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.TimeoutException;
import org.jgroups.util.AckCollector;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups=Global.FUNCTIONAL)
public class AckCollectorTest {
    final List list=Arrays.asList("one", "two", "three", "four", "five");


    public void testConstructor() {
        AckCollector ac=new AckCollector(null, list);
        System.out.println("AckCollector is " + ac);
        Assert.assertEquals(5, ac.size());
    }


    public void testWaitForAllAcksNoTimeout() {
        final AckCollector ac=new AckCollector(null, list);
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
        Assert.assertEquals(0, ac.size());
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testWaitForAllAcksWithTimeoutException() throws TimeoutException {
        AckCollector ac=new AckCollector(null, list);
        ac.waitForAllAcks(200);
    }

    public void testWaitForAllAcksWithTimeout() {
        final AckCollector ac=new AckCollector(null, list);
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
            assert true : "we should not get a timeout exception here";
        }
        catch(TimeoutException e) {
            assert false : "we should not get a timeout exception here";
        }
        Assert.assertEquals(0, ac.size());
    }


    @Test(expectedExceptions=TimeoutException.class)
    public void testWaitForAllAcksWithTimeoutException2() throws TimeoutException {
        final AckCollector ac=new AckCollector(null, list);
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
        ac.waitForAllAcks(300);
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testReset() throws TimeoutException {
        final AckCollector ac=new AckCollector(null, list);
        final List new_list=Arrays.asList("six", "seven", "eight");
        new Thread() {
            public void run() {
                Util.sleep(500);
                System.out.println("resetting AckCollector");
                ac.reset(new_list);
                System.out.println("reset AckCollector: " + ac);
            }
        }.start();
        System.out.println("initial AckCollector: " + ac);
        ac.waitForAllAcks(1000);
        System.out.println("new AckCollector: " + ac);
    }


    public void testReset2() throws TimeoutException {
        final AckCollector ac=new AckCollector(null, list);
        final List new_list=Arrays.asList("six", "seven", "eight");

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

    public static void testNullList() throws TimeoutException {
        AckCollector coll=new AckCollector();
        coll.waitForAllAcks(1000);
    }

    public static void testOneList() throws TimeoutException, UnknownHostException {
        List tmp=new ArrayList();
        Address addr=Util.createRandomAddress();
        tmp.add(addr);
        AckCollector coll=new AckCollector(null, tmp);
        coll.ack(addr);
        coll.waitForAllAcks(1000);
    }




}

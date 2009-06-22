
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Map;


/**
 * @author Bela Ban
 * @version $Id: ResponseCollectorTest.java,v 1.5 2009/06/22 14:34:30 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class ResponseCollectorTest {
    static final Address a=Util.createRandomAddress(), b=Util.createRandomAddress(), c=Util.createRandomAddress();


    public static void testAdd() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b, c);
        coll.add(a, 1);
        System.out.println("coll = " + coll);
        assert coll.size() == 3;
        assert !coll.hasAllResponses();
        coll.add(c, 3);
        coll.add(b, 2);
        System.out.println("coll = " + coll);
        assert coll.size() == 3;
        assert coll.hasAllResponses();
    }

    public static void testAddNonExistentKeys() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b);
        coll.add(a, 1);
        System.out.println("coll = " + coll);
        assert coll.size() == 2;
        assert !coll.hasAllResponses();
        coll.add(c, 3); // will get dropped
        coll.add(b, 2);
        System.out.println("coll = " + coll);
        assert coll.size() == 2;
        assert coll.hasAllResponses();
    }


    public static void testWaitForAllResponses() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b, c);
        boolean rc=coll.waitForAllResponses(500);
        assert !rc;

        new Thread() {
            public void run() {
                coll.add(a, 1);
                Util.sleep(500);
                coll.add(b, 2);
                coll.add(c, 3);
            }
        }.start();

        rc=coll.waitForAllResponses(5000);
        System.out.println("coll = " + coll);
        assert rc;
        assert coll.hasAllResponses();
    }

    public static void testWaitForAllResponsesAndTimeout() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b, c);

        new Thread() {
            public void run() {
                coll.add(a, 1);
                Util.sleep(1000);
                coll.add(b, 2);
                Util.sleep(1000);
                coll.add(c, 3);
            }
        }.start();

        boolean rc=coll.waitForAllResponses(400);
        System.out.println("coll = " + coll);
        assert !rc;
        assert !coll.hasAllResponses() : "collector had all responses (not expected)";
    }

    public static void testWaitForAllResponsesAndReset() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b, c);

        new Thread() {
            public void run() {
                Util.sleep(1000);
                coll.add(a, 1);
                coll.reset();
            }
        }.start();

        boolean rc=coll.waitForAllResponses(5000);
        System.out.println("coll = " + coll);
        assert rc;
        assert coll.hasAllResponses();
    }


    public static void testWaitForAllResponsesAndGetResults() throws InterruptedException {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(a, b, c);

        coll.add(a, 1); coll.add(b, 2); coll.add(c, 3);
        Map<Address, Integer> results=coll.getResults();
        System.out.println("results = " + results);

        Thread thread=new Thread() {
            public void run() {
                coll.reset();
            }
        }; thread.start();

        thread.join();
        System.out.println("results = " + results);
        assert coll.size() == 0;
    }





}
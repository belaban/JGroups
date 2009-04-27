
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * @author Bela Ban
 * @version $Id: ResponseCollectorTest.java,v 1.2 2009/04/27 11:24:39 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class ResponseCollectorTest {



    public static void testAdd() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));
        coll.add(new IpAddress(1000), 1);
        System.out.println("coll = " + coll);
        assert coll.size() == 3;
        assert !coll.hasAllResponses();
        coll.add(new IpAddress(3000), 3);
        coll.add(new IpAddress(2000), 2);
        System.out.println("coll = " + coll);
        assert coll.size() == 3;
        assert coll.hasAllResponses();
    }

    public static void testAddNonExistentKeys() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(2));
        coll.add(new IpAddress(1000), 1);
        System.out.println("coll = " + coll);
        assert coll.size() == 2;
        assert !coll.hasAllResponses();
        coll.add(new IpAddress(3000), 3); // will get dropped
        coll.add(new IpAddress(2000), 2);
        System.out.println("coll = " + coll);
        assert coll.size() == 2;
        assert coll.hasAllResponses();
    }


    public static void testWaitForAllResponses() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));
        boolean rc=coll.waitForAllResponses(500);
        assert !rc;

        new Thread() {
            public void run() {
                coll.add(new IpAddress(1000), 1);
                Util.sleep(500);
                coll.add(new IpAddress(2000), 2);
                coll.add(new IpAddress(3000), 3);
            }
        }.start();

        rc=coll.waitForAllResponses(700);
        System.out.println("coll = " + coll);
        assert rc;
        assert coll.hasAllResponses();
    }

    public static void testWaitForAllResponsesAndTimeout() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));

        new Thread() {
            public void run() {
                coll.add(new IpAddress(1000), 1);
                Util.sleep(500);
                coll.add(new IpAddress(2000), 2);
                coll.add(new IpAddress(3000), 3);
            }
        }.start();

        boolean rc=coll.waitForAllResponses(400);
        System.out.println("coll = " + coll);
        assert !rc;
        assert !coll.hasAllResponses();
    }

    public static void testWaitForAllResponsesAndReset() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));

        new Thread() {
            public void run() {
                Util.sleep(500);
                coll.add(new IpAddress(1000), 1);
                coll.reset();
            }
        }.start();

        boolean rc=coll.waitForAllResponses(700);
        System.out.println("coll = " + coll);
        assert rc;
        assert coll.hasAllResponses();
    }


    public static void testWaitForAllResponsesAndGetResults() throws InterruptedException {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));

        coll.add(new IpAddress(1000), 1); coll.add(new IpAddress(2000), 2); coll.add(new IpAddress(3000), 3);
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



    private static Collection<Address> createMembers(int num) {
        List<Address> retval=new ArrayList<Address>(num);
        int cnt=1000;
        for(int i=0; i < num; i++) {
            retval.add(new IpAddress(cnt));
            cnt+=1000;
        }
        return retval;
    }


   
}

package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.TimeoutException;
import org.jgroups.util.AckCollector;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Test(groups=Global.FUNCTIONAL)
public class AckCollectorTest {
    final Address one=Util.createRandomAddress("one"), two=Util.createRandomAddress("two"),
      three=Util.createRandomAddress("three"), four=Util.createRandomAddress("four"),
      five=Util.createRandomAddress("five");
    final List<Address> list=Arrays.asList(one, two, three, four, five);


    public void testConstructor() {
        AckCollector ac=new AckCollector(list);
        System.out.println("AckCollector is " + ac);
        Assert.assertEquals(5,ac.size());
    }


    public void testWaitForAllAcksNoTimeout() {
        final AckCollector ac=new AckCollector(list);
        new Thread() {
            public void run() {
                for(Address member: list) {
                    Util.sleep(100);
                    ac.ack(member);
                    System.out.println("AckCollector: " + ac);
                }
            }
        }.start();
        ac.waitForAllAcks();
        Assert.assertEquals(0, ac.size());
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testWaitForAllAcksWithTimeoutException() throws TimeoutException {
        AckCollector ac=new AckCollector(list);
        ac.waitForAllAcks(200);
    }

    public void testWaitForAllAcksWithTimeout() {
        final AckCollector ac=new AckCollector(list);
        new Thread() {
            public void run() {
                for(Address member: list) {
                    Util.sleep(100);
                    ac.ack(member);
                    System.out.println("AckCollector: " + ac);
                }
            }
        }.start();
        try {
            ac.waitForAllAcks(30000);
            assert true : "we should not get a timeout exception here";
        }
        catch(TimeoutException e) {
            assert false : "we should not get a timeout exception here";
        }
        Assert.assertEquals(0, ac.size());
    }


    @Test(expectedExceptions=TimeoutException.class)
    public void testWaitForAllAcksWithTimeoutException2() throws TimeoutException {
        final AckCollector ac=new AckCollector(list);
        new Thread() {
            public void run() {
                for(Address member: list) {
                    Util.sleep(100);
                    ac.ack(member);
                    System.out.println("AckCollector: " + ac);
                }
            }
        }.start();
        ac.waitForAllAcks(10);
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testReset() throws TimeoutException {
        final AckCollector ac=new AckCollector(list);
        final Address six=Util.createRandomAddress("six"), seven=Util.createRandomAddress("seven"),
          eight=Util.createRandomAddress("eight");
        final List<Address> new_list=Arrays.asList(six, seven, eight);
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
        final AckCollector ac=new AckCollector(list);
        final Address six=Util.createRandomAddress("six"), seven=Util.createRandomAddress("seven"),
          eight=Util.createRandomAddress("eight");
        final List<Address> new_list=Arrays.asList(six, seven, eight);

        new Thread() {
            public void run() {
                Util.sleep(500);
                System.out.println("resetting AckCollector");
                ac.reset(new_list);
                System.out.println("reset AckCollector: " + ac);
                Util.sleep(100);
                ac.ack(six);
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack(seven);
                System.out.println("AckCollector: " + ac);
                Util.sleep(100);
                ac.ack(eight);
                System.out.println("AckCollector: " + ac);
            }
        }.start();
        System.out.println("initial AckCollector: " + ac);
        ac.waitForAllAcks(30000);
        System.out.println("new AckCollector: " + ac);
    }

    public void testResetWithDuplicateMembers() {
        List<Address> tmp_list=Arrays.asList(one,two,one,three,four,one,five);
        AckCollector ac=new AckCollector(tmp_list);
        System.out.println("ac = " + ac);
        assert ac.size() == 5;
        ac.reset(tmp_list);
        assert ac.size() == 5;
    }

    public void testDestroy() {
        List<Address> tmp_list=Arrays.asList(one,two,one,three,four,one,five);
        final AckCollector ac=new AckCollector(tmp_list);
        System.out.println("ac = " + ac);
        assert ac.size() == 5;
        Thread thread=new Thread() {
            public void run() {
                Util.sleep(2000);
                ac.destroy();
            }
        };
        thread.start();
        boolean result=ac.waitForAllAcks(10000);
        System.out.println("result = " + result);
        assert !result;
    }

    public static void testNullList() throws TimeoutException {
        AckCollector coll=new AckCollector();
        coll.waitForAllAcks(1000);
    }

    public static void testOneList() throws TimeoutException {
        Address addr=Util.createRandomAddress();
        AckCollector coll=new AckCollector(addr);
        coll.ack(addr);
        coll.waitForAllAcks(1000);
    }

    public void testSuspect() {
        final AckCollector ac=new AckCollector(list);
        Stream.of(one, four,five).forEach(ac::ack);
        System.out.println("ac = " + ac);
        Arrays.asList(two, three).forEach(ac::suspect);
        System.out.println("ac = " + ac);
        assert ac.size() == 0;
        assert ac.waitForAllAcks();
    }

    public void testRetainAll() {
        final AckCollector ac=new AckCollector(list);
        List<Address> members=Arrays.asList(one, two, three);
        ac.retainAll(members);
        System.out.println("ac=" + ac);
        assert ac.size() == 3;

        new Thread() {
            public void run() {
                Util.sleep(1000);
                ac.suspect(two);
                Util.sleep(500);
                ac.ack(three); ac.ack(one);
            }
        }.start();

        boolean received_all=ac.waitForAllAcks(30000);
        System.out.println("ac = " + ac);
        assert received_all;
    }

    public void testRetainAll2() {
        final AckCollector ac=new AckCollector(list);
        assert ac.size() == 5;
        System.out.println("ac = " + ac);
        ac.ack(five);
        ac.suspect(four);
        System.out.println("ac = " + ac);

        new Thread() {
            public void run() {
                Util.sleep(1000);
                ac.retainAll(Collections.singletonList(five));
                System.out.println("ac=" + ac);
            }
        }.start();

        boolean received_all=ac.waitForAllAcks(30000);
        System.out.println("ac = " + ac);
        assert received_all;
    }


}

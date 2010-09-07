package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.CreditMap;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;

/**
 * Tests CreditMap
 * @author Bela Ban
 * @version $Id: CreditMapTest.java,v 1.1 2010/09/07 13:24:43 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class CreditMapTest {
    static Address a=Util.createRandomAddress("A");
    static Address b=Util.createRandomAddress("B");
    static Address c=Util.createRandomAddress("C");
    static Address d=Util.createRandomAddress("D");
    static long MAX_CREDITS=1000;

    protected CreditMap map;

    @BeforeMethod
    void create() {
        map=new CreditMap(MAX_CREDITS);
    }

    @AfterMethod
    void destroy() {
        map.clear();
    }

    private void addAll() {
        map.putIfAbsent(a); map.putIfAbsent(b); map.putIfAbsent(c); map.putIfAbsent(d);
    }

    private void replenishAll(long credits) {
        map.replenish(a, credits);
        map.replenish(b, credits);
        map.replenish(c, credits);
        map.replenish(d, credits);
    }



    public void testSimpleDecrement() {
        addAll();

        System.out.println("map:\n" + map);

        boolean rc=map.decrement(200, 4000);
        System.out.println("rc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200;
        assert map.getAccumulatedCredits() == 200;

        rc=map.decrement(150, 100);
        System.out.println("\nrc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150;
        assert map.getAccumulatedCredits() == 200 + 150;

        rc=map.decrement(300, 100);
        System.out.println("\nrc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150 - 300;
        assert map.getAccumulatedCredits() == 200 + 150 + 300;

        rc=map.decrement(500, 100);
        System.out.println("\nrc=" + rc + ", map:\n" + map);
        assert !rc;
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150 - 300;
        assert map.getAccumulatedCredits() == 200 + 150 + 300;
    }

    public void testDecrementAndReplenish() {
        testSimpleDecrement();
        map.replenish(a, MAX_CREDITS);
        System.out.println("\nmap:\n" + map);
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150 - 300;
        assert map.getAccumulatedCredits() == 0;

        map.replenish(b, MAX_CREDITS);
        map.replenish(c, MAX_CREDITS);
        System.out.println("\nmap:\n" + map);
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150 - 300;
        assert map.getAccumulatedCredits() == 0;

        map.replenish(d, MAX_CREDITS);
        System.out.println("\nmap:\n" + map);
        assert map.getMinCredits() == MAX_CREDITS;
        assert map.getAccumulatedCredits() == 0;
    }

    public void testBlockingDecrementAndReplenishment() throws Exception {
        final CyclicBarrier barrier=new CyclicBarrier(2);

        Thread thread=new Thread() {
            public void run() {
                try {
                    barrier.await();
                    Util.sleep(1000);
                    replenishAll(100);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();

        addAll();
        map.decrement(800, 100);
        System.out.println("map:\n" + map);

        barrier.await();
        boolean rc=map.decrement(250, 5000);
        assert rc;
        System.out.println("map:\n" + map);
        assert map.getMinCredits() == 50;
        assert map.getAccumulatedCredits() == 250;
    }


    public void testBlockingDecrementAndReplenishment2() {
        long[] credit_sizes={500, 100, 100, 500, 300};
        Decrementer[] decrementers=new Decrementer[credit_sizes.length];

        addAll();
        map.decrement(800, 100);

        for(int i=0; i < credit_sizes.length; i++)
            decrementers[i]=new Decrementer(map, credit_sizes[i], 20000, true);

        for(Decrementer decr: decrementers)
            decr.start();

        Util.sleep(500);
        int alive=countAliveThreads(decrementers);
        assert alive == 3;

        replenishAll(400); // the 300 credit decr will succeed now
        Util.sleep(500);
        alive=countAliveThreads(decrementers);
        assert alive == 2;

        replenishAll(700); // one of the two 500 creds will succeed
        Util.sleep(500);
        alive=countAliveThreads(decrementers);
        assert alive == 1;

        replenishAll(300); // the other one of the 500 creds will succeed
        Util.sleep(500);
        alive=countAliveThreads(decrementers);
        assert alive == 0;
    }

    public void testClear() {
        addAll();
        map.decrement(800, 100);

        Decrementer decr1=new Decrementer(map, 300, 20000, false), decr2=new Decrementer(map, 500, 20000, false);
        decr1.start();
        decr2.start();

        Util.sleep(500);
        map.clear();

        Util.sleep(500);
        assert !decr1.isAlive();
        assert !decr2.isAlive();
    }


    protected int countAliveThreads(Thread[] threads) {
        int alive=0;
        for(Thread thread: threads)
            if(thread.isAlive())
                alive++;
        return alive;
    }


    protected static class Decrementer extends Thread {
        private final CreditMap map;
        private final long amount;
        private final long timeout;
        protected final boolean loop;

        public Decrementer(CreditMap map, long amount, long timeout, boolean loop) {
            this.map=map;
            this.amount=amount;
            this.timeout=timeout;
            this.loop=loop;
        }

        public void run() {
            while(true) {
                boolean rc=map.decrement(amount, timeout);
                if(rc) {
                    System.out.println("[" + getId() + "] decremented " + amount + " credits");
                    break;
                }
                if(!loop)
                    break;
            }
        }
    }

}

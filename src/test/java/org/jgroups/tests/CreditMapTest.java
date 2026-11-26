package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.CreditMap;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Tests CreditMap
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
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

        boolean rc=map.decrement(null, 200, 4000);
        System.out.println("rc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200;
        assert map.getAccumulatedCredits() == 200;

        rc=map.decrement(null, 150, 100);
        System.out.println("\nrc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150;
        assert map.getAccumulatedCredits() == 200 + 150;

        rc=map.decrement(null, 300, 100);
        System.out.println("\nrc=" + rc + ", map:\n" + map);
        assert rc;
        assert map.getMinCredits() == MAX_CREDITS - 200 - 150 - 300;
        assert map.getAccumulatedCredits() == 200 + 150 + 300;

        rc=map.decrement(null, 500, 100);
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

    public void testDecrementAndReplenish2() {
        map.putIfAbsent(a);
        map.decrement(null, 200, 100);

        map.putIfAbsent(b);
        map.decrement(null, 200, 100);

        map.putIfAbsent(c);
        map.decrement(null, 200, 100);

        map.putIfAbsent(d);
        map.decrement(null, 200, 100);

        // A: 200, B: 400, C: 600, D: 800
        System.out.println("map = " + map);

        assert map.getAccumulatedCredits() == 200;
        assert map.getMinCredits() == 200;

        map.replenish(d, 100);
        map.replenish(c, 100);
        map.replenish(a, 100);
        map.replenish(b, 100);

        assert map.getMinCredits() == 300;
    }

    /** Multiple threads block on credits from B, then B is removed. All threads should be unblocked */
    public void testDecrementAndRemoveOne() throws TimeoutException {
        addAll();
        boolean rc=map.decrement(null, 1000, 100);
        assert rc;
        System.out.println("map = " + map);

        for(Address addr: Arrays.asList(a,c,d))
            map.replenish(addr, 1000);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(()-> map.decrement(null, 100, 20000));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        map.remove(b);
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
    }

    /** Multiple threads block on credits from B, then the CreditMap is cleared. All threads should be unblocked */
    public void testDecrementAndClear() throws TimeoutException {
        addAll();
        boolean rc=map.decrement(null, 1000, 100);
        assert rc;
        System.out.println("map = " + map);

        for(Address addr: Arrays.asList(a,c,d))
            map.replenish(addr, 1000);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(()-> map.decrement(null, 1500, 60000));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        map.clear();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
    }

    /** Multiple threads block on credits from B, then the CreditMap is cleared. All threads should be unblocked */
    public void testDecrementAndReset() throws TimeoutException {
        addAll();
        boolean rc=map.decrement(null, 1000, 100);
        assert rc;
        System.out.println("map = " + map);

        for(Address addr: Arrays.asList(a,c,d))
            map.replenish(addr, 1000);
        Thread[] threads=new Thread[10];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new Thread(()-> map.decrement(null, 1500, 60000));
            threads[i].start();
        }

        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TIMED_WAITING),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));

        map.reset();
        Util.waitUntil(10000, 500,
                       () -> Arrays.stream(threads).allMatch(t -> t.getState() == Thread.State.TERMINATED),
                       () -> "threads:\n" + Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
                         .collect(Collectors.joining("\n")));
        System.out.printf("threads:\n%s\n", Arrays.stream(threads).map(t -> t.getId() + ": " + t.getState())
          .collect(Collectors.joining("\n")));
        rc=map.decrement(null, 1000, 500);
        assert !rc;
    }


    public void testBlockingDecrementAndReplenishment() throws Exception {
        final CyclicBarrier barrier=new CyclicBarrier(2);

        Thread thread=new Thread(() -> {
            try {
                barrier.await();
                Util.sleep(1000);
                replenishAll(100);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();

        addAll();
        boolean rc=map.decrement(null, 800, 100);
        assert rc;
        System.out.println("map:\n" + map);

        barrier.await();
        rc=map.decrement(null, 250, 5000);
        assert rc;
        System.out.println("map:\n" + map);
        assert map.getMinCredits() == 50;
        assert map.getAccumulatedCredits() == 250;
    }


    public void testBlockingDecrementAndReplenishment2() {
        int[] credit_sizes={500, 100, 100, 500, 300};
        Decrementer[] decrementers=new Decrementer[credit_sizes.length];

        addAll();
        boolean rc=map.decrement(null, 800, 100);
        assert rc;

        for(int i=0; i < credit_sizes.length; i++)
            decrementers[i]=new Decrementer(map, credit_sizes[i], 20000, true);

        for(Decrementer decr: decrementers)
            decr.start();

        for(int i=0; i < 10; i++) {
            if(countAliveThreads(decrementers) == 3)
                break;
            Util.sleep(500);
        }
        int alive=countAliveThreads(decrementers);
        assert alive == 3;

        replenishAll(400); // the 300 credit decr will succeed now
        for(int i=0; i < 10; i++) {
            if(countAliveThreads(decrementers) == 2)
                break;
            Util.sleep(500);
        }
        assert countAliveThreads(decrementers) == 2;

        replenishAll(700); // one of the two 500 creds will succeed
        for(int i=0; i < 10; i++) {
            if(countAliveThreads(decrementers) == 1)
                break;
            Util.sleep(500);
        }
        assert countAliveThreads(decrementers) == 1;

        replenishAll(300); // the other one of the 500 creds will succeed
        for(int i=0; i < 10; i++) {
            if(countAliveThreads(decrementers) == 0)
                break;
            Util.sleep(500);
        }
        assert countAliveThreads(decrementers) == 0;
    }

    public void testClear() {
        addAll();
        boolean rc=map.decrement(null, 800, 100);
        assert rc;

        Decrementer decr1=new Decrementer(map, 300, 20000, false), decr2=new Decrementer(map, 500, 20000, false);
        decr1.start();
        decr2.start();

        Util.sleep(500);
        map.clear();

        for(int i=0; i < 20; i++) {
            if(!decr1.isAlive() && !decr2.isAlive())
                break;
            else
                Util.sleep(1000);
        }
        
        assert !decr1.isAlive();
        assert !decr2.isAlive();
    }


    public void testGetMembersWithInsufficientCredits() {
        addAll();
        boolean rc=map.decrement(null, 800, 50);
        assert rc;
        List<Address> list=map.getMembersWithInsufficientCredits(100);
        assert list.isEmpty();

        list=map.getMembersWithInsufficientCredits(200);
        assert list.isEmpty();

        list=map.getMembersWithInsufficientCredits(250);
        assert list.size() == 4;
        assert list.contains(a) && list.contains(b) && list.contains(c) && list.contains(d);

        map.remove(b); map.remove(c);
        list=map.getMembersWithInsufficientCredits(250);
        assert list.size() == 2;
        assert list.contains(a) && list.contains(d);

        map.decrement(null, 100, 50);
        map.putIfAbsent(b); map.putIfAbsent(c);

        // Now A and D have 100 credits, B and C 1000
        list=map.getMembersWithInsufficientCredits(800);
        assert list.size() == 2;
        assert list.contains(a) && list.contains(d);

        list=map.getMembersWithInsufficientCredits(100);
        assert list.isEmpty();
    }


    protected static int countAliveThreads(Thread[] threads) {
        int alive=0;
        for(Thread thread: threads)
            if(thread.isAlive())
                alive++;
        return alive;
    }


    protected static class Decrementer extends Thread {
        private final CreditMap map;
        private final int       amount;
        private final long      timeout;
        protected final boolean loop;

        public Decrementer(CreditMap map, int amount, long timeout, boolean loop) {
            this.map=map;
            this.amount=amount;
            this.timeout=timeout;
            this.loop=loop;
        }

        public void run() {
            while(true) {
                boolean rc=map.decrement(null, amount, timeout);
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

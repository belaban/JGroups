package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.TimeService;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tests {@TimeService}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TimeServiceTest {
    protected TimeScheduler timer;
    protected TimeService   time_service;

    @BeforeClass  public void init()    {timer=new TimeScheduler3();}
    @BeforeMethod public void start()   {time_service=new TimeService(timer).start();}
    @AfterMethod  public void stop()    {time_service.stop();}
    @AfterClass   public void destroy() {timer.stop();}


    public void testSimpleGetTime() {
        List<Long> times=new ArrayList<>(20);
        for(int i=0; i < 20; i++)
            times.add(time_service.timestamp());

        System.out.println("times=" + times);

        Set<Long> set=new HashSet<>(times);
        System.out.println("set = " + set);

        assert set.size() < times.size();
        assert times.size() <= 20;

        set.clear();
        time_service.stop().interval(50).start();
        for(int i=0; i < 20; i++) {
            set.add(time_service.timestamp());
            Util.sleep(200);
        }

        System.out.println("set=" + set);

        assert set.size() >= 15;
    }


    public void testChangeInterval() {
        time_service.interval(1000).start();
        assert time_service.interval() == 1000;
    }

    public void testStartStop() {
        assert time_service.running();
        time_service.stop();
        Util.sleep(2000);
        assert !time_service.running();
    }


    public void testConcurrentReads() throws InterruptedException {
        final int NUM_READERS=100;
        final Map<Long,AtomicInteger> counts=new ConcurrentHashMap<>();
        final CountDownLatch latch=new CountDownLatch(1);
        final Thread[] readers=new Thread[NUM_READERS];
        for(int i=0; i < readers.length; i++) {
            readers[i]=new Thread(() -> {
                try {
                    latch.await();
                    for(int j=0; j < 100_000; j++) {
                        long time=time_service.timestamp();
                        AtomicInteger val=counts.computeIfAbsent(time, k -> new AtomicInteger());
                        val.incrementAndGet();
                    }
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            });
            readers[i].start();
        }

        long start=System.nanoTime();
        latch.countDown();
        for(int i=0; i < readers.length; i++)
            readers[i].join(30000);
        long time=System.nanoTime()-start;
        System.out.printf("time=%d ms\n", TimeUnit.MILLISECONDS.convert(time, TimeUnit.NANOSECONDS));

        SortedSet<Long> sorted_timestamps=new ConcurrentSkipListSet<>(counts.keySet()); // in ns
        long base=sorted_timestamps.first();

        System.out.printf("Timestamps:\n%s\n",
                          sorted_timestamps.stream().map(k -> nanosToMillis(k-base) + ": " + counts.get(k))
                            .collect(Collectors.joining("\n")));
    }

    protected static long nanosToMillis(long ns) {
        return TimeUnit.MILLISECONDS.convert(ns, TimeUnit.NANOSECONDS);
    }

}

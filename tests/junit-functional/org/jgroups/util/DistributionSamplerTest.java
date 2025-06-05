package org.jgroups.util;

import org.jgroups.Global;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @since  5.4.9
 */
@Test(groups=Global.FUNCTIONAL)
public class DistributionSamplerTest {

    public void testConstructor() {
        DistributionSampler d=new DistributionSampler(0,10,11,100,101,1000);
        assert d.size() == 3;

        try {
            d=new DistributionSampler(0, 10, 11);
        }
        catch(IllegalArgumentException iex) {
            System.out.printf("received exception (as expected): %s\n", iex);
        }

        try {
            d=new DistributionSampler(0, 10, 11, 11);
        }
        catch(IllegalArgumentException iex) {
            System.out.printf("received exception (as expected): %s\n", iex);
        }

        try {
            d=new DistributionSampler(0, 10, 10, 100);
        }
        catch(IllegalArgumentException iex) {
            System.out.printf("received exception (as expected): %s\n", iex);
        }
    }

    public void testAdd() {
        DistributionSampler d=new DistributionSampler(0,10,11,100,101,1000);
        for(long l: List.of(0,1,5,10))
            d.add(l);
        long total=d.total();
        assert total == 4;
        for(long l: List.of(11, 50, 99, 100, 101, 101, 500, 999, 1000))
            d.add(l);
        assert d.total() == 13;
    }

    public void testAdd2() {
        final int NUM_BUCKETS=1024, DELTA=100;
        List<Long> l=new ArrayList<>(NUM_BUCKETS);
        long num=0;
        for(int i=0; i < NUM_BUCKETS; i++) {
            l.add(num);
            num+=DELTA;
            l.add(num);
            num++;
        }
        DistributionSampler d=new DistributionSampler(l);
        final int OPS=1_000_000;
        long start=System.nanoTime();
        for(int i=0; i < OPS; i++) {
            long value=Util.random(num-1);
            d.add(value);
        }
        long time=System.nanoTime() - start;
        double ops_sec=(double)OPS / ((double)time / 1_000_000_000);
        // System.out.println("d = " + d);
        System.out.printf("time: %s, ops/s: %,.2f\n", Util.printTime(time, TimeUnit.NANOSECONDS), ops_sec);
        assert d.total() == OPS;
    }

}

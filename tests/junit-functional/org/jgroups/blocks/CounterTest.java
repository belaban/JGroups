package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.COUNTER;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Tests {@link org.jgroups.blocks.atomic.SyncCounter} and {@link org.jgroups.blocks.atomic.AsyncCounter}
 * @author Bela Ban
 * @since  5.2.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class CounterTest {
    protected JChannel       a, b, c;
    protected CounterService service_a, service_b, service_c;
    protected SyncCounter    ca, cb, cc;

    @BeforeMethod protected void init() throws Exception {
        a=create("A");
        service_a=new CounterService(a);
        a.connect(CounterTest.class.getSimpleName());
        ca=service_a.getOrCreateSyncCounter("counter", 0);

        b=create("B");
        service_b=new CounterService(b);
        b.connect(CounterTest.class.getSimpleName());
        cb=service_b.getOrCreateSyncCounter("counter", 0);

        c=create("C");
        service_c=new CounterService(c);
        c.connect(CounterTest.class.getSimpleName());
        cc=service_c.getOrCreateSyncCounter("counter", 0);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
        assert ca != null && cb != null && cc != null;
        assert ca.get() == 0 && cb.get() == 0 && cc.get() == 0;
    }

    @AfterMethod protected void cleanup() {
        Util.closeReverse(a,b,c);
    }


    public void testIncrement() throws TimeoutException {
        ca.incrementAndGet();
        assertValues(1);
        cb.incrementAndGet();
        assertValues(2);
        cc.incrementAndGet();
        assertValues(3);
    }

    public void testAsyncIncrement() throws TimeoutException {
        AverageMinMax avg=new AverageMinMax();
        long[] values=new long[1000];

        long total=System.currentTimeMillis();
        for(int i=1; i <= 1000; i++) {
            long start=Util.micros();
            long val=cc.incrementAndGet();
            if(val < 1000)
                values[(int)val]=val;
            long time=Util.micros()-start;
            avg.add(time);
        }
        Util.waitUntil(500, 1, () -> IntStream.rangeClosed(1, 999).allMatch(i -> values[i] > 0));
        long total_time=System.currentTimeMillis()-total;
        System.out.printf("sync: total time: %d ms, avg: %s us\n", total_time, avg);

        Arrays.fill(values, 0L);
        cc.set(0);
        AsyncCounter async=cc.async();
        avg.clear();

        total=System.currentTimeMillis();
        for(int i=1; i <= 1000; i++) {
            long start=Util.micros();
            async.incrementAndGet()
              .thenAccept(v -> {
                  values[(int)v.longValue()]=v;
                  long time=Util.micros()-start;
                  synchronized(avg) {
                      avg.add(time);
                  }
              });
        }
        Util.waitUntil(50000, 1, () -> IntStream.rangeClosed(1, 999).allMatch(i -> values[i] > 0));
        total_time=System.currentTimeMillis() - total;
        System.out.printf("async: total time: %d ms, avg: %s us\n", total_time, avg);
    }

    public void testAsyncIncrement2() throws TimeoutException {
        AsyncCounter async_c=cc.async();
        final int NUM=1000;
        final AtomicInteger val=new AtomicInteger(0);
        long start=System.currentTimeMillis();
        for(int i=0; i < NUM; i++)
            async_c.incrementAndGet().thenAccept(v -> val.set(v.intValue()));
        Util.waitUntil(1000, 1, () -> val.get() == NUM);
        long time=System.currentTimeMillis()-start;
        System.out.printf("val=%d, time=%d ms\n", val.get(), time);
    }

    public void testCompareAndSet() {
        boolean result=cb.compareAndSet(0, 5);
        assert result && cb.get() == 5;
        result=cc.compareAndSet(0, 5);
        assert !result && cb.get() == 5;
    }

    public void testCompareAndSwap() {
        long previous_value=cb.compareAndSwap(0, 5);
        assert previous_value == 0;
        long val=cb.get();
        assert val == 5;
        previous_value=cb.compareAndSwap(5, 10);
        assert previous_value == 5 && cb.get() == 10;
    }


    protected void assertValues(int expected_val) {
        assert Stream.of(ca,cb,cc).allMatch(c -> c.get() == expected_val);
    }

    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack(new COUNTER().setBypassBundling(false))).name(name);
    }
}

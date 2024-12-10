package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.CounterFunction;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.blocks.atomic.CounterView;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.COUNTER;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.LongSizeStreamable;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

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


    public void testIncrement() {
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
                  avg.add(time);
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

    public void testIncrementUsingFunction() {
        assertEquals(1, ca.incrementAndGet());
        assertEquals(1, cb.update(new GetAndAddFunction(1)).getAsLong());
        assertEquals(2, cc.update(new GetAndAddFunction(5)).getAsLong());
        assertValues(7);
    }

    public void testComplexFunction() {
        assertEquals(0, ca.get());
        AddWithLimitResult res = cb.update(new AddWithLimitFunction(2, 10));
        assertEquals(2, res.result);
        assertFalse(res.limitReached);
        assertValues(2);

        res = cc.update(new AddWithLimitFunction(8, 10));
        assertEquals(10, res.result);
        assertFalse(res.limitReached);
        assertValues(10);

        res = cc.update(new AddWithLimitFunction(1, 10));
        assertEquals(10, res.result);
        assertTrue(res.limitReached);
        assertValues(10);

        ca.set(0);
        assertValues(0);

        res = cb.update(new AddWithLimitFunction(20, 10));
        assertEquals(10, res.result);
        assertTrue(res.limitReached);
        assertValues(10);
    }

    protected void assertValues(int expected_val) {
        assert Stream.of(ca,cb,cc).allMatch(c -> c.get() == expected_val);
    }

    protected static JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack(new COUNTER().setBypassBundling(false))).name(name);
    }

    public static class GetAndAddFunction implements CounterFunction<LongSizeStreamable>, SizeStreamable {

        long delta;

        // for unmarshalling
        @SuppressWarnings("unused")
        public GetAndAddFunction() {}

        public GetAndAddFunction(long delta) {
            this.delta = delta;
        }

        @Override
        public LongSizeStreamable apply(CounterView counterView) {
            long ret = counterView.get();
            counterView.set(ret + delta);
            return new LongSizeStreamable(ret);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(delta);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            delta = in.readLong();
        }

        @Override
        public int serializedSize() {
            return Long.BYTES;
        }
    }

    public static class AddWithLimitFunction implements CounterFunction<AddWithLimitResult>, SizeStreamable {

        long delta;
        long limit;

        // for unmarshalling
        @SuppressWarnings("unused")
        public AddWithLimitFunction() {
        }

        public AddWithLimitFunction(long delta, long limit) {
            this.delta = delta;
            this.limit = limit;
        }

        @Override
        public AddWithLimitResult apply(CounterView counterView) {
            long newValue = counterView.get() + delta;
            if (newValue > limit) {
                counterView.set(limit);
                return new AddWithLimitResult(limit, true);
            } else {
                counterView.set(newValue);
                return new AddWithLimitResult(newValue, false);
            }
        }

        @Override
        public int serializedSize() {
            return Long.BYTES * 2;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(delta);
            out.writeLong(limit);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            delta = in.readLong();
            limit = in.readLong();
        }
    }

    public static class AddWithLimitResult implements SizeStreamable {

        long result;
        boolean limitReached;

        // for unmarshalling
        @SuppressWarnings("unused")
        public AddWithLimitResult() {}

        public AddWithLimitResult(long result, boolean limitReached) {
            this.result = result;
            this.limitReached = limitReached;
        }

        @Override
        public int serializedSize() {
            return Long.BYTES + 1;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeLong(result);
            out.writeBoolean(limitReached);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            result = in.readLong();
            limitReached = in.readBoolean();
        }
    }
}

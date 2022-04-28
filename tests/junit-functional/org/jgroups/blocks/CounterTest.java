package org.jgroups.blocks;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.COUNTER;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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


    public void testIncrement() {
        ca.incrementAndGet();
        assertValues(1);
        cb.incrementAndGet();
        assertValues(2);
        cc.incrementAndGet();
        assertValues(3);
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
        return new JChannel(Util.getTestStack(new COUNTER())).name(name);
    }
}

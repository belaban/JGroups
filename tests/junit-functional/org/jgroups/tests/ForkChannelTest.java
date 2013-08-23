package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.COUNTER;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests {@link org.jgroups.fork.ForkChannel}
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ForkChannelTest {
    protected JChannel                ch;
    protected ForkChannel             fc1, fc2;
    protected static final Protocol[] protocols;
    protected static final String     CLUSTER="ForkChannelTest";

    static {
        protocols=Util.getTestStack(new FORK());
    }

    @BeforeMethod protected void setup() throws Exception {
        ch=new JChannel(protocols);
    }

    @AfterMethod protected void destroy() {
        Util.close(ch, fc2, fc1);
    }



    public void testLifecycle() throws Exception {
        fc1=new ForkChannel(ch, "stack", "fc1");
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        fc1.connect("bla");
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        ch.connect(CLUSTER);
        assert fc1.isOpen() && fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        assert ch.getAddress().equals(fc1.getAddress());
        assert ch.getClusterName().equals(fc1.getClusterName());
        assert ch.getView().equals(fc1.getView());

        fc1.disconnect();
        assert fc1.isOpen() && !fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        fc1.connect("foobar");
        assert fc1.isOpen() && fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        Util.close(fc1);
        assert !fc1.isOpen() && !fc1.isConnected() && fc1.isClosed() : "state=" + fc1.getState();

        fc1.connect("whocares");
        assert fc1.isOpen() && fc1.isConnected() && !fc1.isClosed() : "state=" + fc1.getState();

        Util.close(ch);
        assert !fc1.isOpen() && !fc1.isConnected() && fc1.isClosed() : "state=" + fc1.getState();

        try {
            fc1.send(null, "hello");
            assert false: "sending on a fork-channel with a disconnected main-channel should throw an exception";
        }
        catch(Throwable t) {
            System.out.println("got an exception (as expected) sending on a fork-channel where the main-channel is disconnected: " + t);
        }
    }

    /** Tests the case where we don't add any fork-stack specific protocols */
    public void testNullForkStack() throws Exception {
        fc1=new ForkChannel(ch, "stack", "fc1");
        fc2=new ForkChannel(ch, "stack", "fc2");
        MyReceiver<Integer> r1=new MyReceiver<Integer>(), r2=new MyReceiver<Integer>();
        fc1.setReceiver(r1); fc2.setReceiver(r2);
        ch.connect(CLUSTER);
        fc1.connect("foo");
        fc2.connect("bar");

        for(int i=1; i <= 5; i++) {
            fc1.send(null, i);
            fc2.send(null, i+5);
        }

        List<Integer> l1=r1.list(), l2=r2.list();
        for(int i=0; i < 20; i++) {
            if(l1.size() == 5 && l2.size() == 5)
                break;
            Util.sleep(500);
        }

        System.out.println("r1: " + r1.list() + ", r2: " + r2.list());
        assert r1.size() == 5 && r2.size() == 5;
        for(int i=1; i <= 5; i++)
            assert r1.list().contains(i) && r2.list().contains(i+5);
    }


    /**
     * Tests CounterService on 2 different fork-channels, using the *same* fork-stack. This means the 2 counter
     * services will 'see' each other and the counters must have the same value
     * @throws Exception
     */
    public void testCounterService() throws Exception {
        fc1=new ForkChannel(ch, "stack", "fc1", false,ProtocolStack.ABOVE, FORK.class, new COUNTER());
        fc2=new ForkChannel(ch, "stack", "fc2", false,ProtocolStack.ABOVE, FORK.class, new COUNTER());
        ch.connect(CLUSTER);
        fc1.connect("foo");
        fc2.connect("bar");

        CounterService cs1=new CounterService(fc1), cs2=new CounterService(fc2);

        Counter c1=cs1.getOrCreateCounter("counter", 1), c2=cs2.getOrCreateCounter("counter", 1);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 1 && c2.get() == 1;

        c1.addAndGet(5);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 6 && c2.get() == 6;

        c2.compareAndSet(6, 10);
        System.out.println("counter1=" + c1 + ", counter2=" + c2);
        assert c1.get() == 10 && c2.get() == 10;
    }

}

package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Average;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Measure the latency between messages with message bundling enabled at the transport level.
 * Update April 2016: this test is more or less useless, as System.nanoTime() can yield different values, depending
 * on which core it is run. E.g. the sender (main) thread might run on core-0, but the receiver thread on core-1,
 * and since the cores can have different counters for nanoTime(), subtracting the values is meaningless.
 * System.nanoTime() only really works when invoked by the same thread, e.g. as in {@link #testSimple()}.</p>
 * This was changed by making the sender block on a promise which is signalled by the receiver thread when a message
 * has been received. After that, System.nanoTime() is called by the sender, so start and stop times are called by the
 * same thread. Of course, (uncontended) lock acquisition, thread context switching etc amount to some overhead...
 *
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class MessageBundlingTest extends ChannelTestBase {
    private JChannel               a, b;
    private final Promise<Boolean> promise=new Promise<>();
    private SimpleReceiver         r2;
    private final static long      LATENCY=1500L;
    private final static long      LATENCY_NS=LATENCY * 1_000_000_000L;
    private final static long      SLEEP=5000L; // ms
    private final static long      SLEEP_NS=SLEEP * 1_000_000_000;
    private static final int       MAX_BYTES=62000;


    @BeforeMethod
    protected void createChannels() throws Exception {
        a=createChannel(true, 2, "A");
        setBundling(a,MAX_BYTES);
        a.connect("MessageBundlingTest");
        b=createChannel(a, "B");
        r2=new SimpleReceiver(promise);
        b.setReceiver(r2);
        b.connect("MessageBundlingTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }

    protected JChannel create(String name) throws Exception {
        return new JChannel(Util.getTestStack()).name(name);
    }

    @AfterMethod void tearDown() throws Exception {promise.reset(false); Util.close(b,a);}

    protected boolean useBlocking() {return false;}
    

    public void testSimple() throws Exception {
        long start=System.nanoTime();
        a.send(new Message());
        promise.getResult(5000);
        long diff=System.nanoTime() - start;
        System.out.printf("took %s to send and receive a multicast message\n", print(diff));
        assert diff < SLEEP_NS /2;
    }

    public void testLatencyWithoutMessageBundling() throws Exception {
        _testLatencyWithoutMessageBundling(true);
    }


    public void testLatencyWithMessageBundling() throws Exception {
       _testLatencyWithoutMessageBundling(false);
    }


    public void testLatencyWithMessageBundlingAndMaxBytes() throws Exception {
        final int num=500;
        final Average avg=new Average();
        long min=Long.MAX_VALUE, max=0;

        System.out.printf(">>> sending %s messages\n", num);
        long[] times=new long[num];
        for(int i=0; i < num; i++) {
            long start=System.nanoTime();
            a.send(new Message(null, new byte[4000]));
            promise.getResult(SLEEP);
            long time=System.nanoTime()-start;
            times[i]=time;
            avg.add(time);
            min=Math.min(min, time);
            max=Math.max(max, time);
            promise.reset(false);
        }
        for(int i=0; i < times.length; i++)
            System.out.printf("latency for %d: %s\n", i, print(times[i]));
        System.out.printf("\nmin/max/avg (us): %.2f  / %.2f / %.2f\n", min / 1000.0, max / 1000.0, avg.getAverage() / 1000.0);
        assert avg.getAverage() < LATENCY_NS;
    }


    protected void _testLatencyWithoutMessageBundling(boolean use_bundling) throws Exception {
        Message tmp=new Message();
        if(use_bundling) {
            tmp.setFlag(Message.Flag.DONT_BUNDLE);
            setBundling(a, MAX_BYTES);
        }
        long time=System.nanoTime();
        a.send(tmp);
        System.out.println(">>> sent message");
        promise.getResult(SLEEP);
        long diff=System.nanoTime() - time;
        System.out.printf("latency: %s\n", print(diff));
        assertTrue(String.format("latency (%s) should be less than %d ms", print(diff), LATENCY), diff <= LATENCY_NS);
    }


    private static String print(long time_ns) {
        double us=time_ns / 1_000.0;
        return String.format("%d ns (%.2f us)", time_ns, us);
    }

    private static void setBundling(JChannel ch, int max_bytes) {
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setMaxBundleSize(max_bytes);
        GMS gms=stack.findProtocol(GMS.class);
        gms.setViewAckCollectionTimeout(LATENCY * 2);
        gms.setJoinTimeout(LATENCY * 2);
    }



    protected static class SimpleReceiver extends ReceiverAdapter {
        protected final Promise<Boolean> promise;

        public SimpleReceiver(Promise<Boolean> promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(true);
        }
    }

}

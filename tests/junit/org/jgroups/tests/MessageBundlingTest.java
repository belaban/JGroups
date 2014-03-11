package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Measure the latency between messages with message bundling enabled at the transport level
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class MessageBundlingTest extends ChannelTestBase {
    private JChannel           a, b;
    private MyReceiver         r2;
    private final static long  LATENCY=1500L;
    private final static long  SLEEP=5000L;
    private static final int   MAX_BYTES=64000;


    @BeforeMethod
    protected void createChannels() throws Exception {
        a=createChannel(true, 2, "A");
        setBundling(a,MAX_BYTES,LATENCY);
        a.connect("MessageBundlingTest");
        b=createChannel(a, "B");
        r2=new MyReceiver();
        b.setReceiver(r2);
        b.connect("MessageBundlingTest");
        Util.waitUntilAllChannelsHaveSameSize(10000,1000,a,b);
    }

    @AfterMethod void tearDown() throws Exception {Util.close(b,a);}

    protected boolean useBlocking() {return false;}
    

    public void testSimple() throws Exception {
        final Promise<Boolean> promise=new Promise<Boolean>();
        SimpleReceiver receiver=new SimpleReceiver(promise);
        b.setReceiver(receiver);
        long start=System.currentTimeMillis();
        a.send(new Message());
        promise.getResult(5000);
        long diff=System.currentTimeMillis() - start;
        System.out.println("toook " + diff + " ms to send and receive a multicast message");
        assert diff < SLEEP /2;
    }

    public void testLatencyWithoutMessageBundling() throws Exception {
        Message tmp=new Message().setFlag(Message.Flag.DONT_BUNDLE);
        setBundling(a, MAX_BYTES, 30);
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        a.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        Assert.assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be less than " + LATENCY + " ms", diff <= LATENCY);
    }


    public void testLatencyWithMessageBundling() throws Exception {
        Message tmp=new Message();
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        a.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        Assert.assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be less than 2 times the LATENCY (" + LATENCY *2 + ")",
                   diff <= LATENCY * 2);
    }



    public void testLatencyWithMessageBundlingAndLoopback() throws Exception {
        Message tmp=new Message();
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        System.out.println(">>> sending message at " + new Date());
        a.send(tmp);
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        Assert.assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be less than 2 times the LATENCY (" + LATENCY *2 + ")",
                   diff <= LATENCY * 2);
    }


    public void testLatencyWithMessageBundlingAndMaxBytes() throws Exception {
        r2.setNumExpectedMesssages(10);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        Util.sleep(LATENCY * 2);
        System.out.println(">>> sending 10 messages at " + new Date());
        for(int i=0; i < 10; i++)
            a.send(new Message(null, null, new byte[2000]));

        promise.getResult(SLEEP); // we should get the messages immediately because max_bundle_size has been exceeded by the 20 messages
        List<Long> list=r2.getTimes();
        Assert.assertEquals(10, list.size());

        for(Iterator<Long> it=list.iterator(); it.hasNext();) {
            Long val=it.next();
            System.out.println(val);
        }
    }



    private static void setBundling(JChannel ch, int max_bytes, long timeout) {
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setMaxBundleSize(max_bytes);
        transport.setMaxBundleTimeout(timeout);
        GMS gms=(GMS)stack.findProtocol(GMS.class);
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

    private static class MyReceiver extends ReceiverAdapter {
        private final List<Long> times=new LinkedList<Long>();
        private int num_expected_msgs;
        private Promise<Integer> promise;

        public List<Long> getTimes() {
            return times;
        }

        public void setNumExpectedMesssages(int num_expected_msgs) {
            this.num_expected_msgs=num_expected_msgs;
        }

        public void setPromise(Promise<Integer> promise) {
            this.promise=promise;
        }

        public int size() {
            return times.size();
        }

        public void receive(Message msg) {
            times.add(System.currentTimeMillis());
            System.out.println("<<< received message from " + msg.getSrc() + " at " + new Date());
            if(times.size() >= num_expected_msgs && promise != null) {
                promise.setResult(times.size());
            }
        }
    }


}

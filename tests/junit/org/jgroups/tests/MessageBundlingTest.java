package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

import java.util.*;

/**
 * Measure the latency between messages with message bundling enabled at the transport level
 * @author Bela Ban
 * @version $Id: MessageBundlingTest.java,v 1.9 2007/11/09 12:25:17 belaban Exp $
 */
public class MessageBundlingTest extends ChannelTestBase {
    private JChannel ch1, ch2;
    private MyReceiver r2;
    private final static long LATENCY=1500L;
    private final static long SLEEP=5000L;
    private static final boolean BUNDLING=true;
    private static final int MAX_BYTES=20000;

    public void setUp() throws Exception {
        super.setUp();
        ch1=createChannel();
        setBundling(ch1, BUNDLING, MAX_BYTES, LATENCY);
        setLoopback(ch1, false);
        ch1.setReceiver(new NullReceiver());
        ch1.connect("x");
        ch2=createChannel();
        setBundling(ch2, BUNDLING, MAX_BYTES, LATENCY);
        setLoopback(ch1, false);
        r2=new MyReceiver();
        ch2.setReceiver(r2);
        ch2.connect("x");

        View view=ch2.getView();
        assertEquals(2, view.size());
    }


    public void tearDown() throws Exception {
        closeChannel(ch2);
        closeChannel(ch1);
        super.tearDown();
    }


    protected boolean useBlocking() {
       return false;
    }


    public void testLatencyWithoutMessageBundling() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();
        setBundling(ch1, false, 20000, 30);
        r2.setNumExpectedMesssages(1);
        Promise promise=new Promise();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be less than " + LATENCY + " ms", diff <= LATENCY);
    }


    public void testLatencyWithMessageBundling() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();
        r2.setNumExpectedMesssages(1);
        Promise promise=new Promise();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff >= LATENCY && diff <= LATENCY * 2);
    }



    public void testLatencyWithMessageBundlingAndLoopback() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();
        setLoopback(ch1, true);
        setLoopback(ch2, true);
        r2.setNumExpectedMesssages(1);
        Promise promise=new Promise();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        System.out.println(">>> sending message at " + new Date());
        ch1.send(tmp);
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff >= LATENCY && diff <= LATENCY * 2);
    }


    public void testLatencyWithMessageBundlingAndMaxBytes() throws ChannelClosedException, ChannelNotConnectedException {
        setLoopback(ch1, true);
        setLoopback(ch2, true);
        r2.setNumExpectedMesssages(10);
        Promise promise=new Promise();
        r2.setPromise(promise);
        Util.sleep(LATENCY *2);
        System.out.println(">>> sending 10 messages at " + new Date());
        for(int i=0; i < 10; i++)
            ch1.send(new Message(null, null, new byte[2000]));

        promise.getResult(SLEEP); // we should get the messages immediately because max_bundle_size has been exceeded by the 20 messages
        List<Long> list=r2.getTimes();
        assertEquals(10, list.size());

        for(Iterator<Long> it=list.iterator(); it.hasNext();) {
            Long val=it.next();
            System.out.println(val);
        }
    }


    public void testSimple() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();
        ch2.setReceiver(new SimpleReceiver());
        ch1.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        Util.sleep(5000);
    }

    private void setLoopback(JChannel ch, boolean b) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector<Protocol> prots=stack.getProtocols();
        TP transport=(TP)prots.lastElement();
        transport.setLoopback(b);
    }


    private void setBundling(JChannel ch, boolean enabled, int max_bytes, long timeout) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector<Protocol> prots=stack.getProtocols();
        TP transport=(TP)prots.lastElement();
        transport.setEnableBundling(enabled);
        transport.setMaxBundleSize(max_bytes);
        transport.setMaxBundleTimeout(timeout);
        transport.setEnable_unicast_bundling(false);
        if(enabled) {
            GMS gms=(GMS)stack.findProtocol("GMS");
            gms.setViewAckCollectionTimeout(LATENCY * 2);
            gms.setJoinTimeout(LATENCY * 2);
        }
    }

    private void closeChannel(Channel c) {
        if(c != null && (c.isOpen() || c.isConnected())) {
            c.close();
        }
    }

    private static class NullReceiver extends ReceiverAdapter {

        public void receive(Message msg) {
            ;
        }
    }


    private static class SimpleReceiver extends ReceiverAdapter {
        long start=System.currentTimeMillis();

        public void receive(Message msg) {
            System.out.println("<<< received message from " + msg.getSrc() + " at " + new Date() +
                    ", latency=" + (System.currentTimeMillis() - start) + " ms");
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final List<Long> times=new LinkedList<Long>();
        private int num_expected_msgs;
        private Promise promise;

        public List<Long> getTimes() {
            return times;
        }


        public void setNumExpectedMesssages(int num_expected_msgs) {
            this.num_expected_msgs=num_expected_msgs;
        }


        public void setPromise(Promise promise) {
            this.promise=promise;
        }

        public int size() {
            return times.size();
        }

        public void receive(Message msg) {
            times.add(new Long(System.currentTimeMillis()));
            System.out.println("<<< received message from " + msg.getSrc() + " at " + new Date());
            if(times.size() >= num_expected_msgs && promise != null) {
                promise.setResult(times.size());
            }
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={MessageBundlingTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}

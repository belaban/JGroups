package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Measure the latency between messages with message bundling enabled at the transport level
 * @author Bela Ban
 * @version $Id: MessageBundlingTest.java,v 1.18 2009/06/04 09:01:46 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class MessageBundlingTest extends ChannelTestBase {
    private JChannel ch1, ch2;
    private MyReceiver r2;
    private final static long LATENCY=1500L;
    private final static long SLEEP=5000L;
    private static final boolean BUNDLING=true;
    private static final int MAX_BYTES=64000;


    
    @AfterMethod
    void tearDown() throws Exception {
        closeChannel(ch2);
        closeChannel(ch1);
    }


    protected boolean useBlocking() {
       return false;
    }
    



    public void testLatencyWithoutMessageBundling() throws Exception {
        createChannels("testLatencyWithoutMessageBundling");
        Message tmp=new Message();
        setBundling(ch1, false, MAX_BYTES, 30);
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        ch1.send(tmp);
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
        createChannels("testLatencyWithMessageBundling");
        Message tmp=new Message();
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        Assert.assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff >= LATENCY && diff <= LATENCY * 2);
    }



    public void testLatencyWithMessageBundlingAndLoopback() throws Exception {
        createChannels("testLatencyWithMessageBundlingAndLoopback");
        Message tmp=new Message();
        setLoopback(ch1, true);
        setLoopback(ch2, true);
        r2.setNumExpectedMesssages(1);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        long time=System.currentTimeMillis();
        System.out.println(">>> sending message at " + new Date());
        ch1.send(tmp);
        promise.getResult(SLEEP);
        List<Long> list=r2.getTimes();
        Assert.assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        System.out.println("latency: " + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff >= LATENCY && diff <= LATENCY * 2);
    }


    public void testLatencyWithMessageBundlingAndMaxBytes() throws Exception {
        createChannels("testLatencyWithMessageBundlingAndMaxBytes");
        setLoopback(ch1, true);
        setLoopback(ch2, true);
        r2.setNumExpectedMesssages(10);
        Promise<Integer> promise=new Promise<Integer>();
        r2.setPromise(promise);
        Util.sleep(LATENCY *2);
        System.out.println(">>> sending 10 messages at " + new Date());
        for(int i=0; i < 10; i++)
            ch1.send(new Message(null, null, new byte[2000]));

        promise.getResult(SLEEP); // we should get the messages immediately because max_bundle_size has been exceeded by the 20 messages
        List<Long> list=r2.getTimes();
        Assert.assertEquals(10, list.size());

        for(Iterator<Long> it=list.iterator(); it.hasNext();) {
            Long val=it.next();
            System.out.println(val);
        }
    }


    public void testSimple() throws Exception {
        createChannels("testSimple");
        Message tmp=new Message();
        ch2.setReceiver(new SimpleReceiver());
        ch1.send(tmp);
        System.out.println(">>> sent message at " + new Date());
        Util.sleep(5000);
    }


    private void createChannels(String cluster) throws Exception {
        ch1=createChannel(true, 2);
        setBundling(ch1, BUNDLING, MAX_BYTES, LATENCY);
        setLoopback(ch1, false);
        ch1.setReceiver(new NullReceiver());
        ch1.connect("MessageBundlingTest-" + cluster);
        ch2=createChannel(ch1);
        // setBundling(ch2, BUNDLING, MAX_BYTES, LATENCY);
        // setLoopback(ch2, false);
        r2=new MyReceiver();
        ch2.setReceiver(r2);
        ch2.connect("MessageBundlingTest-" + cluster);

        View view=ch2.getView();
        assert view.size() == 2 : " view=" + view;
    }

    private static void setLoopback(JChannel ch, boolean b) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector<Protocol> prots=stack.getProtocols();
        TP transport=(TP)prots.lastElement();
        transport.setLoopback(b);
    }


    private static void setBundling(JChannel ch, boolean enabled, int max_bytes, long timeout) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector<Protocol> prots=stack.getProtocols();
        TP transport=(TP)prots.lastElement();
        transport.setEnableBundling(enabled);
        if(enabled) {
            transport.setMaxBundleSize(max_bytes);
            transport.setMaxBundleTimeout(timeout);
        }
        transport.setEnableUnicastBundling(false);
        if(enabled) {
            GMS gms=(GMS)stack.findProtocol("GMS");
            gms.setViewAckCollectionTimeout(LATENCY * 2);
            gms.setJoinTimeout(LATENCY * 2);
        }
    }

    private static void closeChannel(Channel c) {
        if(c != null && (c.isOpen() || c.isConnected())) {
            c.close();
        }
    }

    private static class NullReceiver extends ReceiverAdapter {
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
            times.add(new Long(System.currentTimeMillis()));
            System.out.println("<<< received message from " + msg.getSrc() + " at " + new Date());
            if(times.size() >= num_expected_msgs && promise != null) {
                promise.setResult(times.size());
            }
        }
    }


}

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * Measure the latency between messages with message bundling enabled at the transport level
 * @author Bela Ban
 * @version $Id: MessageBundlingTest.java,v 1.2.2.2 2007/05/03 20:02:41 belaban Exp $
 */
public class MessageBundlingTest extends TestCase {
    private JChannel ch1, ch2;
    private MyReceiver r2;
    private final static long LATENCY=30L;
    private static final boolean BUNDLING=true;

    public void setUp() throws Exception {
        super.setUp();
        ch1=new JChannel("udp.xml");
        setBundling(ch1, BUNDLING, 64000, LATENCY);
        ch1.setReceiver(new NullReceiver());
        ch1.connect("x");
        ch2=new JChannel("udp.xml");
        setBundling(ch2, BUNDLING, 64000, LATENCY);
        r2=new MyReceiver();
        ch2.setReceiver(r2);
        ch2.connect("x");
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
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println("sent message at " + new Date());
        Util.sleep(LATENCY + 10L);
        List list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=(Long)list.get(0);
        long diff=time2.longValue() - time;
        System.out.println("latency=" + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be less than the bundling timeout", diff < LATENCY);
    }


    public void testLatencyWithMessageBundling() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();

        // setBundling(ch1, false, 20000, 30);
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println("sent message at " + new Date());
        Util.sleep(LATENCY + 10L);
        List list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=(Long)list.get(0);
        long diff=time2.longValue() - time;
        System.out.println("latency=" + diff + " ms");
        assertTrue("latency (" + diff + "ms) should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff > LATENCY && diff < LATENCY * 2);
    }



    private void setBundling(JChannel ch, boolean enabled, int max_bytes, long timeout) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector prots=stack.getProtocols();
        TP transport=(TP)prots.lastElement();
        transport.setEnableBundling(enabled);
        transport.setMaxBundleSize(max_bytes);
        transport.setMaxBundleTimeout(timeout);
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

    private static class MyReceiver extends ReceiverAdapter {
        private final List times=new LinkedList();

        public List getTimes() {
            return times;
        }

        public void receive(Message msg) {
            times.add(new Long(System.currentTimeMillis()));
            System.out.println("received message from " + msg.getSrc() + " at " + new Date());
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={MessageBundlingTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}

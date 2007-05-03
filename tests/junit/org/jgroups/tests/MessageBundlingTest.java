package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Vector;
import java.util.List;
import java.util.LinkedList;
import java.util.Date;

/**
 * Measure the latency between messages with message bundling enabled at the transport level
 * @author Bela Ban
 * @version $Id: MessageBundlingTest.java,v 1.1 2007/05/03 19:51:15 belaban Exp $
 */
public class MessageBundlingTest extends ChannelTestBase {
    private JChannel ch1, ch2;
    private MyReceiver r2;
    private final static long LATENCY=30L;
    private static final boolean BUNDLING=true;

    public void setUp() throws Exception {
        super.setUp();
        ch1=createChannel();
        setBundling(ch1, BUNDLING, 64000, LATENCY);
        ch1.setReceiver(new NullReceiver());
        ch1.connect("x");
        ch2=createChannel();
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


    public void measureLatency() throws ChannelClosedException, ChannelNotConnectedException {
        Message tmp=new Message();
        long time=System.currentTimeMillis();
        ch1.send(tmp);
        System.out.println("sent message at " + new Date());
        Util.sleep(LATENCY + 10L);
        List<Long> list=r2.getTimes();
        assertEquals(1, list.size());
        Long time2=list.get(0);
        long diff=time2 - time;
        assertTrue("latency should be more than the bundling timeout (" + LATENCY +
                "ms), but less than 2 times the LATENCY (" + LATENCY *2 + ")", diff > LATENCY && diff < LATENCY * 2);
    }



    private void setBundling(JChannel ch, boolean enabled, int max_bytes, long timeout) {
        ProtocolStack stack=ch.getProtocolStack();
        Vector<Protocol> prots=stack.getProtocols();
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
        private final List<Long> times=new LinkedList<Long>();

        public List<Long> getTimes() {
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

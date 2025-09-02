package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests large retransmissions (https://issues.redhat.com/browse/JGRP-1868). Multicast equivalent to
 * {@link org.jgroups.protocols.UNICAST_RetransmitTest}
 * @author Bela Ban
 * @since  3.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class NAKACK_RetransmitTest {
    protected JChannel a, b, c;
    protected static final int MAX_BUNDLE_SIZE=10000;
    protected static final int NUM_MSGS=50000;

    protected static final Method START_RETRANSMISSION, STOP_RETRANSMISSION;

    static {
        try {
            START_RETRANSMISSION=NAKACK2.class.getDeclaredMethod("startRetransmitTask");
            START_RETRANSMISSION.setAccessible(true);
            STOP_RETRANSMISSION=NAKACK2.class.getDeclaredMethod("stopRetransmitTask");
            STOP_RETRANSMISSION.setAccessible(true);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @BeforeMethod
    protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A");
        b=new JChannel(Util.getTestStack()).name("B");
        c=new JChannel(Util.getTestStack()).name("C");
        change(a, b, c);
        a.connect("NAKACK_RetransmitTest");
        b.connect("NAKACK_RetransmitTest");
        c.connect("NAKACK_RetransmitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);
    }

    @AfterMethod
    protected void destroy() {Util.close(c,b,a);}


    /**
     * Sends a number of messages, but discards every other message. The retransmission task in NAKACK2 is initially
     * disabled. Then starts the retransmission task, which should generate an XMIT-REQ which is larger than
     * TP.max_bundle_size, leading to endless retransmissions. With JGRP-1868 resolved, the receiver should get
     * all messages.
     * <p>
     * https://issues.redhat.com/browse/JGRP-1868
     */
    public void testLargeRetransmission() throws Exception {
        a.setReceiver(new MyReceiver());
        b.setReceiver(new MyReceiver());
        c.setReceiver(new MyReceiver());
        Queue<Integer> la=((NAKACK_RetransmitTest.MyReceiver)a.getReceiver()).getList(),
          lb=((NAKACK_RetransmitTest.MyReceiver)b.getReceiver()).getList(),
          lc=((NAKACK_RetransmitTest.MyReceiver)c.getReceiver()).getList();

        stopRetransmission(a);

        insertDiscardProtocol(a);

        for(int i=1; i <= NUM_MSGS; i++)
            a.send(null, i);

        removeDiscardProtocol(a);
        startRetransmission(a);

        for(int i=0; i < 10; i++) {
            if(la.size() == NUM_MSGS && lb.size() == NUM_MSGS && lc.size() == NUM_MSGS)
                break;
            STABLE stable=a.getProtocolStack().findProtocol(STABLE.class);
            stable.gc();
            Util.sleep(1000);
        }

        System.out.println("A.size(): " + la.size() +
                             "\nB.size(): " + lb.size() +
                             "\nC.size(): " + lc.size());

        for(Queue<Integer> list: Arrays.asList(la,lb,lc)) {
            int expected=1;
            for(int num : list) {
                assert expected == num;
                assert num <= NUM_MSGS;
                expected++;
            }
        }
    }



    protected static void change(JChannel ... channels) {
        for(JChannel ch: channels) {
            TP transport=ch.getProtocolStack().getTransport();
            transport.getBundler().setMaxSize(MAX_BUNDLE_SIZE);
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak == null)
                throw new IllegalStateException("NAKACK2 not present in the stack");
            nak.setMaxXmitReqSize(5000);
        }
    }


    protected static class MyReceiver implements Receiver {
        protected final Queue<Integer> list=new ConcurrentLinkedQueue<>();

        public void receive(Message msg) {
            Integer num=msg.getObject();
            synchronized(list) {
                list.add(num);
            }
        }

        public Queue<Integer> getList() {return list;}
    }


    protected static void stopRetransmission(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            STOP_RETRANSMISSION.invoke(nak);
        }
    }

    protected static void startRetransmission(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            START_RETRANSMISSION.invoke(nak);
        }
    }

    protected static void insertDiscardProtocol(JChannel ... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            stack.insertProtocolInStack(new DiscardEveryOtherMulticastMessage(), stack.getTransport(), ProtocolStack.Position.ABOVE);
        }
    }

    protected static void removeDiscardProtocol(JChannel ... channels) {
        for(JChannel ch: channels)
            ch.getProtocolStack().removeProtocol(DiscardEveryOtherMulticastMessage.class);
    }

    protected static void setLevel(String level, JChannel ... channels) {
        for(JChannel ch: channels) {
            Protocol prot=ch.getProtocolStack().findProtocol(NAKACK2.class);
            prot.level(level);
        }
    }


    protected static class DiscardEveryOtherMulticastMessage extends Protocol {
        protected boolean discard=false;

        public Object down(Message msg) {
            if(msg.getDest() == null) {
                discard=!discard;
                if(discard)
                    return null;
            }
            return down_prot.down(msg);
        }
    }


}

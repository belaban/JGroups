package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests large retransmissions (https://issues.jboss.org/browse/JGRP-1868). Multicast equivalent to
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
     * <p/>
     * https://issues.jboss.org/browse/JGRP-1868
     */
    public void testLargeRetransmission() throws Exception {
        a.setReceiver(new MyReceiver());
        b.setReceiver(new MyReceiver());
        c.setReceiver(new MyReceiver());
        List<Integer> la=((NAKACK_RetransmitTest.MyReceiver)a.getReceiver()).getList();
        List<Integer> lb=((NAKACK_RetransmitTest.MyReceiver)b.getReceiver()).getList();
        List<Integer> lc=((NAKACK_RetransmitTest.MyReceiver)c.getReceiver()).getList();

        stopRetransmission(a);

        insertDiscardProtocol(a);

        for(int i=1; i <= NUM_MSGS; i++)
            a.send(null, i);

        removeDiscardProtocol(a);
        startRetransmission(a);

        for(int i=0; i < 10; i++) {
            if(la.size() == NUM_MSGS && lb.size() == NUM_MSGS && lc.size() == NUM_MSGS)
                break;
            STABLE stable=(STABLE)a.getProtocolStack().findProtocol(STABLE.class);
            stable.gc();
            Util.sleep(1000);
        }

        System.out.println("A.size(): " + la.size() +
                             "\nB.size(): " + lb.size() +
                             "\nC.size(): " + lc.size());

        for(List<Integer> list: Arrays.asList(la,lb,lc)) {
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
            transport.setMaxBundleSize(MAX_BUNDLE_SIZE);
            NAKACK2 nak=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak == null)
                throw new IllegalStateException("NAKACK2 not present in the stack");
            nak.setValue("max_xmit_req_size", 5000);
            nak.setValue("max_msg_batch_size", 10000);
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new ArrayList<>();

        public void receive(Message msg) {
            Integer num=(Integer)msg.getObject();
            list.add(num);
        }

        public List<Integer> getList() {return list;}
    }


    protected void stopRetransmission(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            NAKACK2 nak=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            STOP_RETRANSMISSION.invoke(nak);
        }
    }

    protected void startRetransmission(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            NAKACK2 nak=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            START_RETRANSMISSION.invoke(nak);
        }
    }

    protected static void insertDiscardProtocol(JChannel ... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            stack.insertProtocolInStack(new DiscardEveryOtherMulticastMessage(), stack.getTransport(), ProtocolStack.ABOVE);
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

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                if(msg.dest() == null) {
                    discard=!discard;
                    if(discard)
                        return null;
                }
            }
            return down_prot.down(evt);
        }
    }


}

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests large retransmissions (https://issues.jboss.org/browse/JGRP-1868)
 * @author Bela Ban
 * @since  3.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_RetransmitTest {
    protected JChannel a, b;
    protected static final int MAX_BUNDLE_SIZE=10000;
    protected static final int NUM_MSGS=50000;

    @BeforeMethod
    protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A");
        b=new JChannel(Util.getTestStack()).name("B");
        change(a, b);
        a.connect("UNICAST_RetransmitTest");
        b.connect("UNICAST_RetransmitTest");
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a, b);
    }

    @AfterMethod
    protected void destroy() {Util.close(b,a);}


    /**
     * Sends a number of messages, but discards every other message. The retransmission task in UNICAST3 is initially
     * disabled. Then starts the retransmission task, which should generate an XMIT-REQ which is larger than
     * TP.max_bundle_size, leading to endless retransmissions. With JGRP-1868 resolved, the receiver should get
     * all messages.
     * <p/>
     * https://issues.jboss.org/browse/JGRP-1868
     */
    public void testLargeRetransmission() throws Exception {
        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        List<Integer> list=receiver.getList();

        stopRetransmission(a, b);

        insertDiscardProtocol(a);

        Address dest=b.getAddress();
        for(int i=1; i <= NUM_MSGS; i++)
            a.send(dest, i);
        Util.sleep(500);

        removeDiscardProtocol(a);
        startRetransmission(a, b);
        setLevel("trace", a,b);

        Util.waitUntilListHasSize(list, NUM_MSGS, 10000, 1000);
        int expected=1;
        for(int num: list) {
            assert expected == num;
            assert num <= NUM_MSGS;
            expected++;
        }
        setLevel("warn", a,b);
    }



    protected static void change(JChannel ... channels) {
        for(JChannel ch: channels) {
            TP transport=ch.getProtocolStack().getTransport();
            transport.setMaxBundleSize(MAX_BUNDLE_SIZE);
            UNICAST3 ucast=(UNICAST3)ch.getProtocolStack().findProtocol(UNICAST3.class);
            if(ucast == null)
                throw new IllegalStateException("UNICAST3 not present in the stack");
            ucast.setValue("max_xmit_req_size", 5000);
            ucast.setValue("max_msg_batch_size", 2000);
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


    protected void stopRetransmission(JChannel ... channels) {
        for(JChannel ch: channels) {
            UNICAST3 ucast=(UNICAST3)ch.getProtocolStack().findProtocol(UNICAST3.class);
            ucast.stopRetransmitTask();
        }
    }

    protected void startRetransmission(JChannel ... channels) {
        for(JChannel ch: channels) {
            UNICAST3 ucast=(UNICAST3)ch.getProtocolStack().findProtocol(UNICAST3.class);
            ucast.startRetransmitTask();
        }
    }

    protected static void insertDiscardProtocol(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        stack.insertProtocolInStack(new DiscardEveryOtherUnicastMessage(), stack.getTransport(), ProtocolStack.ABOVE);
    }

    protected static void removeDiscardProtocol(JChannel ch) {
        ch.getProtocolStack().removeProtocol(DiscardEveryOtherUnicastMessage.class);
    }

    protected static void setLevel(String level, JChannel ... channels) {
        for(JChannel ch: channels) {
            Protocol prot=ch.getProtocolStack().findProtocol(UNICAST3.class);
            prot.level(level);
        }
    }


    protected static class DiscardEveryOtherUnicastMessage extends Protocol {
        protected boolean discard=false;

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                if(msg.dest() != null) {
                    discard=!discard;
                    if(discard)
                        return null;
                }
            }
            return down_prot.down(evt);
        }
    }


}

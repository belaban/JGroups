package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests large retransmissions (https://issues.redhat.com/browse/JGRP-1868)
 * @author Bela Ban
 * @since  3.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createUnicast")
public class UNICAST_RetransmitTest {
    protected JChannel a, b;
    protected static final int MAX_BUNDLE_SIZE=10000;
    protected static final int NUM_MSGS=50000;

    @DataProvider
    static Object[][] createUnicast() {
        return new Object[][]{
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    protected void setup(Class<? extends Protocol> unicast_class) throws Exception {
        a=new JChannel(Util.getTestStack()).name("A");
        b=new JChannel(Util.getTestStack()).name("B");
        change(a, b);
        a.connect("UNICAST_RetransmitTest");
        b.connect("UNICAST_RetransmitTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }

    @AfterMethod
    protected void destroy() {Util.close(b,a);}


    /**
     * Sends a number of messages, but discards every other message. The retransmission task in UNICAST3/4 is initially
     * disabled. Then starts the retransmission task, which should generate an XMIT-REQ which is larger than
     * TP.max_bundle_size, leading to endless retransmissions. With JGRP-1868 resolved, the receiver should get
     * all messages.
     * <p>
     * https://issues.redhat.com/browse/JGRP-1868
     */
    public void testLargeRetransmission(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        MyReceiver<Integer> receiver=new MyReceiver<>();
        b.setReceiver(receiver);
        List<Integer> list=receiver.list();

        stopRetransmission(a, b);

        insertDiscardProtocol(a);

        Address dest=b.getAddress();
        for(int i=1; i <= NUM_MSGS; i++)
            a.send(dest, i);
        Util.sleep(500);

        removeDiscardProtocol(a);
        startRetransmission(a, b);
        setLevel("trace", a,b);

        Util.waitUntilListHasSize(list, NUM_MSGS, 15000, 1000);
        int expected=1;
        for(int num: list) {
            assert expected == num;
            assert num <= NUM_MSGS;
            expected++;
        }
        setLevel("warn", a,b);
    }

    protected static void change(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            TP transport=ch.getProtocolStack().getTransport();
            transport.getBundler().setMaxSize(MAX_BUNDLE_SIZE);
            Protocol ucast=ch.getProtocolStack().findProtocol(UNICAST3.class,UNICAST4.class);
            if(ucast == null)
                throw new IllegalStateException("UNICAST prototocol not found in the stack");
            Util.invoke(ucast, "setMaxXmitReqSize", 5000);
        }
    }

    protected static void stopRetransmission(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            UNICAST3 ucast=ch.getProtocolStack().findProtocol(UNICAST3.class, UNICAST4.class);
            Util.invoke(ucast, "stopRetransmitTask");
        }
    }

    protected static void startRetransmission(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            Protocol ucast=ch.getProtocolStack().findProtocol(UNICAST3.class, UNICAST4.class);
            Util.invoke(ucast, "startRetransmitTask");
        }
    }

    protected static void insertDiscardProtocol(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        stack.insertProtocolInStack(new DiscardEveryOtherUnicastMessage(), stack.getTransport(), ProtocolStack.Position.ABOVE);
    }

    protected static void removeDiscardProtocol(JChannel ch) {
        ch.getProtocolStack().removeProtocol(DiscardEveryOtherUnicastMessage.class);
    }

    protected static void setLevel(String level, JChannel ... channels) {
        for(JChannel ch: channels) {
            Protocol prot=ch.getProtocolStack().findProtocol(UNICAST3.class,UNICAST4.class);
            prot.level(level);
        }
    }


    protected static class DiscardEveryOtherUnicastMessage extends Protocol {
        protected boolean discard=false;

        public Object down(Message msg) {
            if(msg.getDest() != null) {
                discard=!discard;
                if(discard)
                    return null;
            }
            return down_prot.down(msg);
        }
    }


}

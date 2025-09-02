package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

/**
 * Tests large retransmissions (https://issues.redhat.com/browse/JGRP-1868). Multicast equivalent to
 * {@link org.jgroups.protocols.UNICAST_RetransmitTest}
 * @author Bela Ban
 * @since  3.6
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class NAKACK_RetransmitTest {
    protected JChannel         a, b, c;
    protected static final int MAX_BUNDLE_SIZE=10000;
    protected static final int NUM_MSGS=50000, PRINT=NUM_MSGS / 10;

    protected static final Method START_RETRANSMISSION2, STOP_RETRANSMISSION2, START_RETRANSMISSION4, STOP_RETRANSMISSION4;
    protected static final Supplier<Protocol[]> NAK2=Util::getTestStack;
    protected static final Supplier<Protocol[]> NAK4=() -> new Protocol[] {
      new SHARED_LOOPBACK(),
      new SHARED_LOOPBACK_PING(),
      new NAKACK4().capacity(50000),
      new UNICAST3(),
      new GMS().setJoinTimeout(1000),
      new FRAG2().setFragSize(8000)
    };

    static {
        START_RETRANSMISSION2=Util.findMethod(NAKACK2.class, "startRetransmitTask");
        START_RETRANSMISSION2.setAccessible(true);
        STOP_RETRANSMISSION2=Util.findMethod(NAKACK2.class, "stopRetransmitTask");
        STOP_RETRANSMISSION2.setAccessible(true);

        START_RETRANSMISSION4=Util.findMethod(NAKACK4.class, "startRetransmitTask");
        START_RETRANSMISSION4.setAccessible(true);
        STOP_RETRANSMISSION4=Util.findMethod(NAKACK4.class, "stopRetransmitTask");
        STOP_RETRANSMISSION4.setAccessible(true);
    }



    @DataProvider
    protected static Object[][] create() {
        return new Object[][] {
          {NAK2},
          {NAK4}
        };
    }

    protected void setup(Supplier<Protocol[]> s) throws Exception {
        a=new JChannel(s.get()).name("A");
        b=new JChannel(s.get()).name("B");
        c=new JChannel(s.get()).name("C");
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
    public void testLargeRetransmission(Supplier<Protocol[]> s) throws Exception {
        setup(s);
        a.setReceiver(new MyReceiver());
        b.setReceiver(new MyReceiver());
        c.setReceiver(new MyReceiver());
        Queue<Integer> la=((NAKACK_RetransmitTest.MyReceiver)a.getReceiver()).getList(),
          lb=((NAKACK_RetransmitTest.MyReceiver)b.getReceiver()).getList(),
          lc=((NAKACK_RetransmitTest.MyReceiver)c.getReceiver()).getList();

        stopRetransmission(a);
        insertDiscardProtocol(a);

        for(int i=1; i <= NUM_MSGS; i++) {
            a.send(null, i);
            if(i > 0 && i % PRINT == 0)
                System.out.printf("-- sent %d msgs\n", i);
        }
        System.out.println("-- done sending. Removing DISCARD protocol and starting retransmission");

        removeDiscardProtocol(a);
        startRetransmission(a);

        for(int i=0; i < 20; i++) {
            if(la.size() == NUM_MSGS && lb.size() == NUM_MSGS && lc.size() == NUM_MSGS) {
                System.out.printf("-- A: %d, B: %d, C: %d\n", la.size(), lb.size(), lc.size());
                break;
            }
            System.out.printf("#%d -- A: %d, B: %d, C: %d\n", i+1, la.size(), lb.size(), lc.size());
            STABLE stable=a.getProtocolStack().findProtocol(STABLE.class);
            if(stable != null)
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
            NAKACK2 nak2=ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak2 != null) {
                setXmitMaxReqSize(nak2, 5000);
                nak2.setXmitInterval(500);
                continue;
            }
            NAKACK4 nak4=ch.stack().findProtocol(NAKACK4.class);
            if(nak4 != null) {
                setXmitMaxReqSize(nak4, 5000);
                nak4.setXmitInterval(500);
            }
        }
    }

    protected static final void setXmitMaxReqSize(NAKACK2 nak, int max_size) {
        nak.setMaxXmitReqSize(max_size);
    }

    protected static final void setXmitMaxReqSize(NAKACK4 nak, int max_size) {
        nak.setMaxXmitReqSize(max_size);
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
            Protocol nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak != null) {
                STOP_RETRANSMISSION2.invoke(nak);
                continue;
            }
            nak=ch.getProtocolStack().findProtocol(NAKACK4.class);
            if(nak != null)
                STOP_RETRANSMISSION4.invoke(nak);
        }
    }

    protected static void startRetransmission(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            Protocol nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak != null) {
                START_RETRANSMISSION2.invoke(nak);
                continue;
            }
            nak=ch.getProtocolStack().findProtocol(NAKACK4.class);
            if(nak != null)
                START_RETRANSMISSION4.invoke(nak);
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

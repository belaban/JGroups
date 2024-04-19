package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.NAKACK4;
import org.jgroups.protocols.NakAckHeader;
import org.jgroups.protocols.ReliableMulticast;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests the last message dropped problem in NAKACK (see doc/design/varia2.txt)
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class LastMessageDroppedTest extends ChannelTestBase {
    protected JChannel a, b;
    protected static final short NAKACK2_ID, NAKACK4_ID;

    static {
        NAKACK2_ID=ClassConfigurator.getProtocolId(NAKACK2.class);
        NAKACK4_ID=ClassConfigurator.getProtocolId(NAKACK4.class);
    }

    @BeforeMethod void init() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a,b);
        changeNAKACK(a, b);
        // it should take between 0 and 6s to retransmit the last missing msg. if dropped, may have to run multiple times
        changeDesiredGossipTime(2000, a,b);
        a.connect("LastMessageDroppedTest");
        b.connect("LastMessageDroppedTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }


    @AfterMethod void cleanup() {
        Util.close(b,a);
    }

    public void testLastMessageDropped() throws Exception {
        if(!retransmissionAvailable(a, b))
            return;
        DISCARD discard=new DISCARD();
        a.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.BELOW, NAKACK2.class, NAKACK4.class);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        a.send(null, 1);
        a.send(null, 2);
        discard.dropDownMulticasts(1); // drop the next multicast
        a.send(null, 3);

        Collection<Integer> list=receiver.getMsgs();
        Util.waitUntil(20000, 500, () -> list.size() == 3, () -> String.format("list: %s", list));
        System.out.println("list=" + list);
    }

    /**
     * Tests the case where the last message is dropped, and the ensuing LAST_SEQNO message is also dropped. STABLE with
     * a timeout of 5s should make sure that B eventually does get message 3.
     */
    public void testLastMessageAndLastSeqnoDropped() throws Exception {
        if(!retransmissionAvailable(a,b))
            return;
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.Position.BELOW, NAKACK2.class, NAKACK4.class);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        a.send(null, 1);
        a.send(null, 2);
        discard.dropDownMulticasts(1); // drop the next multicast
        stack.insertProtocol(new LastSeqnoDropper(1), ProtocolStack.Position.BELOW, NAKACK2.class, NAKACK4.class);
        a.send(null, 3);

        Collection<Integer> list=receiver.getMsgs();
        Util.waitUntil(20000, 500, () -> list.size() == 3, () -> String.format("list: %s", list));
        System.out.println("list=" + list);
        assert list.size() == 3 : "list=" + list;
    }

    protected static void changeNAKACK(JChannel ... channels) {
        for(JChannel ch: channels) {
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            if(nak != null) {
                nak.setResendLastSeqno(true);
                nak.setResendLastSeqnoMaxTimes(1);
            }
        }
    }

    protected static void changeDesiredGossipTime(long avg_desired_gossip, JChannel ... channels) {
        for(JChannel ch: channels) {
            STABLE stable=ch.getProtocolStack().findProtocol(STABLE.class);
            if(stable != null)
                stable.setDesiredAverageGossip(avg_desired_gossip);
        }
    }

    protected static boolean retransmissionAvailable(JChannel... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            Protocol nak=stack.findProtocol(NAKACK2.class);
            if(nak != null && ((NAKACK2)nak).getXmitInterval() > 0)
                continue;
            nak=stack.findProtocol(ReliableMulticast.class);
            if(nak != null && ((ReliableMulticast)nak).getXmitInterval() > 0)
                continue;
            return false;
        }
        return true;
    }

    /** Drop {@link org.jgroups.protocols.pbcast.NakAckHeader2#HIGHEST_SEQNO} headers, needs to be inserted below NAKACK2 */
    protected static class LastSeqnoDropper extends Protocol {
        protected final int num_times; // how many times should we drop HIGHEST_SEQNO messages
        protected int       count;

        public LastSeqnoDropper(int num_times) {
            this.num_times=num_times;
        }

        public Object down(Message msg) {
            NakAckHeader2 hdr=msg.getHeader(NAKACK2_ID);
            if(hdr != null && count < num_times) {
                count++;
                return null;
            }
            NakAckHeader hdr4=msg.getHeader(NAKACK4_ID);
            if(hdr4 != null && count < num_times) {
                count++;
                return null;
            }

            return down_prot.down(msg);
        }
    }

    protected static class MyReceiver implements Receiver {
        private final Collection<Integer> msgs=new ConcurrentLinkedQueue<>();

        public Collection<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            msgs.add(msg.getObject());
        }
    }
}

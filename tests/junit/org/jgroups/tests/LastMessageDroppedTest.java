package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.DISCARD;
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
    protected static final short NAKACK2_ID;

    static {
        NAKACK2_ID=ClassConfigurator.getProtocolId(NAKACK2.class);
    }

    @BeforeMethod void init() throws Exception {
        a=createChannel(true, 2).name("A");
        b=createChannel(a).name("B");
        changeNAKACK2(a,b);
        // it should take between 0 and 6s to retransmit the last missing msg. if dropped, may have to run multiple times
        changeDesiredGossipTime(3000, a,b);
        a.connect("LastMessageDroppedTest");
        b.connect("LastMessageDroppedTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }


    @AfterMethod void cleanup() {
        Util.close(b,a);
    }

    public void testLastMessageDropped() throws Exception {
        DISCARD discard=new DISCARD();
        a.getProtocolStack().insertProtocol(discard,ProtocolStack.Position.BELOW,NAKACK2.class);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        a.send(null, 1);
        a.send(null, 2);
        discard.dropDownMulticasts(1); // drop the next multicast
        a.send(null, 3);

        Collection<Integer> list=receiver.getMsgs();
        for(int i=0; i < 20 && list.size() < 3; i++)  {
            System.out.println("list=" + list);
            Util.sleep(1000);
        }
        System.out.println("list=" + list);
        assert list.size() == 3 : "list=" + list;
    }

    /**
     * Tests the case where the last message is dropped, and the ensuing LAST_SEQNO message is also dropped. STABLE with
     * a timeout of 5s should make sure that B eventually does get message 3.
     */
    public void testLastMessageAndLastSeqnoDropped() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard,ProtocolStack.Position.BELOW,NAKACK2.class);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        a.send(null, 1);
        a.send(null, 2);
        discard.dropDownMulticasts(1); // drop the next multicast
        stack.insertProtocol(new LastSeqnoDropper(1), ProtocolStack.Position.BELOW, NAKACK2.class);
        a.send(null, 3);

        Collection<Integer> list=receiver.getMsgs();
        for(int i=0; i < 20 && list.size() < 3; i++)  {
            System.out.println("list=" + list);
            Util.sleep(1000);
        }
        System.out.println("list=" + list);
        assert list.size() == 3 : "list=" + list;
    }

    protected static void changeNAKACK2(JChannel ... channels) {
        for(JChannel ch: channels) {
            NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
            nak.setResendLastSeqno(true);
            nak.setResendLastSeqnoMaxTimes(1);
        }
    }

    protected static void changeDesiredGossipTime(long avg_desired_gossip, JChannel ... channels) {
        for(JChannel ch: channels) {
            STABLE stable=ch.getProtocolStack().findProtocol(STABLE.class);
            stable.setDesiredAverageGossip(avg_desired_gossip);
        }
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

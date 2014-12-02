package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the last message dropped problem in NAKACK (see doc/design/varia2.txt)
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class LastMessageDroppedTest extends ChannelTestBase {
    JChannel a, b;

    @BeforeMethod void init() throws Exception {
        a=createChannel(true, 2).name("A");
        b=createChannel(a).name("B");
        changeNAKACK2(a,b);
        a.connect("LastMessageDroppedTest");
        b.connect("LastMessageDroppedTest");
        Util.waitUntilAllChannelsHaveSameSize(10000,500,a,b);
    }


    @AfterMethod void cleanup() {
        Util.close(b,a);
    }

    public void testLastMessageDropped() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard,ProtocolStack.BELOW,NAKACK2.class);
        a.setDiscardOwnMessages(true);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);
        a.send(null, 1);
        a.send(null, 2);
        discard.setDropDownMulticasts(1); // drop the next multicast
        a.send(null, 3);
        Util.sleep(100);

        List<Integer> list=receiver.getMsgs();
        for(int i=0; i < 20; i++)  {
            System.out.println("list=" + list);
            if(list.size() == 3)
                break;
            Util.sleep(1000);
        }
        System.out.println("list=" + list);

        assert list.size() == 3 : "list=" + list;
    }


    protected static void changeNAKACK2(JChannel ... channels) {
        for(JChannel ch: channels) {
            NAKACK2 nak=(NAKACK2)ch.getProtocolStack().findProtocol(NAKACK2.class);
            nak.setResendLastSeqno(true);
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        private final List<Integer> msgs=new ArrayList<Integer>(3);

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            msgs.add((Integer)msg.getObject());
        }
    }
}

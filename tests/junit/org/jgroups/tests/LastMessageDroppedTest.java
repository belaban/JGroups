package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
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
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class LastMessageDroppedTest extends ChannelTestBase {
    JChannel c1, c2;

    @BeforeMethod
    void init() throws Exception {
        c1=createChannel(true, 2);
        c2=createChannel(c1);
        // c1.setOpt(Channel.LOCAL, false);
        c1.connect("LastMessageDroppedTest");
        c2.connect("LastMessageDroppedTest");
        View view=c2.getView();
        System.out.println("view = " + view);
        assert view.size() == 2 : "view is " + view;
    }


    @AfterMethod
    void cleanup() {
        Util.close(c2, c1);
    }

    public void testLastMessageDropped() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK2.class);
        c1.setDiscardOwnMessages(true);

        Message m1=new Message(null, null, 1);
        Message m2=new Message(null, null, 2);
        Message m3=new Message(null, null, 3);

        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        c1.send(m1);
        c1.send(m2);
        discard.setDropDownMulticasts(1); // drop the next multicast
        c1.send(m3);
        Util.sleep(100);

        List<Integer> list=receiver.getMsgs();
        for(int i=0; i < 20; i++)  {
            System.out.println("list=" + list);
            if(list.size() == 3)
                break;
            stable(c1, c2);
            Util.sleep(1000);
        }
        System.out.println("list=" + list);

        assert list.size() == 3 : "list=" + list;
    }


    private static void stable(JChannel ... channels) {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            STABLE stable=(STABLE)stack.findProtocol(STABLE.class);
            if(stable == null)
                throw new IllegalStateException("STABLE protocol was not found");
            stable.gc();
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        private final List<Integer> msgs=new ArrayList<Integer>(3);

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            // System.out.println("<< " + msg.getObject());
            msgs.add((Integer)msg.getObject());
        }
    }
}

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PRIO;
import org.jgroups.protocols.PrioHeader;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=false)
public class PrioTest extends ChannelTestBase {
    protected JChannel c1, c2;
    protected PrioReceiver r1, r2;
    protected static final short PRIO_ID=ClassConfigurator.getProtocolId(PRIO.class);

    @BeforeTest void init() throws Exception {
        c1=createChannel(true, 2, "A");
        c1.getProtocolStack().insertProtocol(new PRIO(), ProtocolStack.ABOVE, NAKACK.class);
        c2=createChannel(c1, "B");
        c1.connect("PrioTest");
        r1=new PrioReceiver();
        c1.setReceiver(r1);
        c2.connect("PrioTest");
        r2=new PrioReceiver();
        c2.setReceiver(r2);
        assert c2.getView().size() == 2;
    }

    @AfterTest void destroy() {
        Util.close(c2, c1);
    } 


    public void testPrioritizedMessages() throws ChannelNotConnectedException, ChannelClosedException {
        byte[] prios={120,110,100,90,80,70,60,50,40,30,20,10,9,8,7,6,5,4,3,2,1};
        for(byte prio: prios) {
            Message msg=new Message(null, null, new Integer(prio));
            PrioHeader hdr=new PrioHeader(prio);
            msg.putHeader(PRIO_ID, hdr);
            c1.send(msg);
        }

        // Mike: how can we test this ? It seems this is not deterministic... which is fine, I guess, but hard to test !
    }

    
    protected static class PrioReceiver extends ReceiverAdapter {
        protected final List<Integer> msgs=new LinkedList<Integer>();

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            msgs.add((Integer)msg.getObject());
            PrioHeader hdr=(PrioHeader)msg.getHeader(PRIO_ID);
            System.out.println("<< " + msg.getSrc() + ": " + msg.getObject() + " (prio=" + hdr.getPriority() + ")");
        }
    }
}

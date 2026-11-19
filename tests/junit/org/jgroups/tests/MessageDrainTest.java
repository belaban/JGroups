package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.ObjectMessage;
import org.jgroups.protocols.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests message draining (see docs/design/MessageDraining.txt)
 * @author Bela Ban
 * @since  5.6.0
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class MessageDrainTest extends ChannelTestBase {
    protected JChannel            a,b,c;
    protected MyReceiver<Integer> ra, rb, rc;
    protected static final int    NUM_MSGS=5000;

    @BeforeMethod
    protected void setup() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        c=createChannel().name("C");
        makeUnique(a,b,c);
        a.connect("MessageDrainTest");
        b.connect("MessageDrainTest");
        c.connect("MessageDrainTest");
        Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b,c);
        a.receiver(ra=new MyReceiver<Integer>().dropMsgs(true));
        b.receiver(rb=new MyReceiver<Integer>().dropMsgs(true));
        c.receiver(rc=new MyReceiver<Integer>().dropMsgs(true));
    }

    @AfterMethod
    protected void destroy() {
        Util.close(c,b,a);
    }

    public void testUnicastDraining() throws Exception {
        _test(true);
        Drainable pa=findUnicastProtocol(a), pb=findUnicastProtocol(b), pc=findUnicastProtocol(c);
        long timeout=5000, start=System.nanoTime();
        pa.drain(timeout);
        pb.drain(timeout);
        pc.drain(timeout);
        long time=System.nanoTime()-start;
        System.out.printf("draining of {A,B,C} took %s\n", Util.printTime(time));
        assert rb.size() == NUM_MSGS;
        assert rc.size() == NUM_MSGS;
        System.out.printf("B: %,d messages\nC: %,d messages\n", rb.size(), rc.size());
    }

    protected void _test(boolean unicast) throws Exception {
        injectDISCARD(a, .2);
        int print=NUM_MSGS/10;
        for(int i=1; i <= NUM_MSGS; i++) {
            if(!unicast)
                a.send(new ObjectMessage(null, i));
            else {
                a.send(new ObjectMessage(b.address(), i));
                a.send(new ObjectMessage(c.address(), i));
            }
            if(i > 0 && i % print == 0)
                System.out.printf("-- sent %,d msgs\n", i);
        }
        removeDISCARD(a,b,c);
    }

    protected static <T extends Protocol> T findUnicastProtocol(JChannel ch) {
        return ch.stack().findProtocol(UNICAST3.class, ReliableUnicast.class);
    }

    protected static void injectDISCARD(JChannel ch, double drop_rate) throws Exception {
        DISCARD discard=new DISCARD().setDownDiscardRate(drop_rate);
        ch.stack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
    }

    protected static void removeDISCARD(JChannel ... channels) {
        for(JChannel ch: channels)
            ch.stack().removeProtocol(DISCARD.class);
    }

}

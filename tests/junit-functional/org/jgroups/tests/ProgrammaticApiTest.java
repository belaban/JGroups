package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ProgrammaticApiTest {
    protected JChannel ch;


    @AfterMethod void destroy() {
        Util.close(ch);
    }

    public void testChannelCreation() throws Exception {
        ch=new JChannel(new SHARED_LOOPBACK(), new MockProtocol1(), new MockProtocol2()).name("A");
        MyReceiver receiver=new MyReceiver();
        ch.setReceiver(receiver);
        ch.connect("demo");

        Protocol transport=ch.getProtocolStack().getTransport();
        transport.up((Message)new BytesMessage(null, "hello world").setSrc(Util.createRandomAddress()));
        assert receiver.num_msgs_received == 1;
    }



    protected static class MockProtocol1 extends Protocol {

    }

    protected static class MockProtocol2 extends Protocol {
        
    }

    static Protocol[] createProtocols() {
        return new Protocol[] {
                new PING(),
                new MERGE3(),
                new FD_SOCK(),
                new FD_ALL3().setTimeout(12000).setInterval(3000),
                new VERIFY_SUSPECT(),
                new BARRIER(),
                new NAKACK2(),
                new UNICAST3(),
                new STABLE(),
                new GMS(),
                new UFC(),
                new MFC(),
                new FRAG2()
        };
    }



    static class MyReceiver implements Receiver {
        int num_msgs_received;

        public void receive(Message msg) {
            System.out.println("<< " + msg.getObject());
            num_msgs_received++;
        }
    }
}

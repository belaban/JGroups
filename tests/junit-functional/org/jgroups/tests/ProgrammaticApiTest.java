package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @version $Id: ProgrammaticApiTest.java,v 1.1 2010/10/06 09:46:32 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class ProgrammaticApiTest {

    public void testChannelCreation() throws Exception {
        JChannel ch=new JChannel(false);
        ch.setReceiver(new ReceiverAdapter() {
            public void receive(Message msg) {
                System.out.println("<< " + msg);
            }
        });
        ProtocolStack stack=ch.createProtocolStack();
        stack.addProtocol(new SHARED_LOOPBACK()).addProtocol(new MockProtocol1()).addProtocol(new MockProtocol2());
        stack.init();
        ch.connect("demo");

        Protocol transport=stack.getTransport();
        transport.up(new Event(Event.MSG, new Message(null, Util.createRandomAddress(), "hello world")));

    }



    protected static class MockProtocol1 extends Protocol {

    }

    protected static class MockProtocol2 extends Protocol {
        
    }
}

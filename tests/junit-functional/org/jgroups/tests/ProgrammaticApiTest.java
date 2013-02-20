package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ProgrammaticApiTest {
    JChannel c1, c2;

    @BeforeMethod
    void init() {
        c1=new JChannel(false); c1.setName("A");
        c2=new JChannel(false); c2.setName("B");
    }

    @AfterMethod
    void destroy() {
        Util.close(c2, c1);
    }

    public void testChannelCreation() throws Exception {
        MyReceiver receiver=new MyReceiver(null);
        c1.setReceiver(receiver);
        ProtocolStack stack=new ProtocolStack();
        c1.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK()).addProtocol(new MockProtocol1()).addProtocol(new MockProtocol2());
        stack.init();
        c1.connect("demo");

        Protocol transport=stack.getTransport();
        transport.up(new Event(Event.MSG, new Message(null, Util.createRandomAddress(), "hello world")));
        assert receiver.getNumMsgsReceived() == 1;
    }


    public void testSharedTransport() throws Exception {
        ProtocolStack stack1=new ProtocolStack(), stack2=new ProtocolStack();
        c1.setProtocolStack(stack1);
        c2.setProtocolStack(stack2);

        MyReceiver receiver1=new MyReceiver("A"), receiver2=new MyReceiver("B");

        UDP shared_transport=(UDP)new UDP().setValue("singleton_name", "shared");

        stack1.addProtocol(shared_transport).addProtocols(createProtocols());
        stack2.addProtocol(shared_transport).addProtocols(createProtocols());

        stack1.init();
        stack2.init();

        c1.setReceiver(receiver1);
        c2.setReceiver(receiver2);

        c1.connect("cluster-one");
        c2.connect("cluster-two");

        for(int i=0; i < 10; i++)
            c1.send(new Message(null, null, "hello-" + i));

        for(int i=0; i < 5; i++)
            c2.send(new Message(null, null, "hello-" + i));

        for(int i =0; i < 20; i++) {
            if(receiver1.getNumMsgsReceived() == 10 && receiver2.getNumMsgsReceived() == 5)
                break;
            Util.sleep(500);
        }
        assert receiver1.getNumMsgsReceived() == 10 : "num msgs for A: " + receiver1.getNumMsgsReceived() + " (expected=10)";
        assert receiver2.getNumMsgsReceived() == 5 : "num msgs for B: " + receiver1.getNumMsgsReceived() + " (expected=5)";
    }



    protected static class MockProtocol1 extends Protocol {

    }

    protected static class MockProtocol2 extends Protocol {
        
    }

    static Protocol[] createProtocols() {
        return new Protocol[] {
                new PING(),
                new MERGE2(),
                new FD_SOCK(),
                new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000),
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



    static class MyReceiver extends ReceiverAdapter {
        int num_msgs_received=0;
        final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public int getNumMsgsReceived() {
            return num_msgs_received;
        }

        public void receive(Message msg) {
            System.out.println((name != null? "[" + name + "]" : "") + "<< " + msg.getObject());
            num_msgs_received++;
        }
    }
}

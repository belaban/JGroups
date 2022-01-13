package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class TransportThreadPoolTest extends ChannelTestBase {
    JChannel c1, c2;

    @BeforeMethod
    protected void setUp() throws Exception {
        c1=createChannel().name("A");
        c2=createChannel().name("B");
        makeUnique(c1,c2);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(c2, c1);
    }


    @Test
    public void testThreadPoolReplacement() throws Exception {
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        
        c1.connect("TransportThreadPoolTest");
        c2.connect("TransportThreadPoolTest");
        
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, c1, c2);

        TP transport=c1.getProtocolStack().getTransport();
        ExecutorService thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);

        transport=c2.getProtocolStack().getTransport();
        thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);

        Collection<String> l1=r1.getMsgs(), l2=r2.getMsgs();
        c1.send(null, "hello world");
        c2.send(null, "bela");
        c1.send(null, "message 3");
        c2.send(null, "message 4");

        Util.waitUntil(10000, 100, () -> l1.size() == 4 && l2.size() == 4,
                       () -> String.format("r1: %s, r2: %s", print(l1), print(l2)));

        System.out.println("messages c1: " + print(r1.getMsgs()) + "\nmessages c2: " + print(r2.getMsgs()));
        assert r1.getMsgs().size() == 4;
        assert r2.getMsgs().size() == 4;
    }


    private static String print(Collection<String> msgs) {
        return String.join(", ", msgs);
    }


    private static class MyReceiver implements Receiver {
        Collection<String> msgs=new ConcurrentLinkedQueue<>();
        
        public Collection<String> getMsgs() {
            return msgs;
        }
        
        public void receive(Message msg) {
            msgs.add(msg.getObject());
        }
    }
}

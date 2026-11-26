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
import java.util.stream.Stream;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class TransportThreadPoolTest extends ChannelTestBase {
    JChannel a, b;

    @BeforeMethod
    protected void setUp() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a, b);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(b, a);
    }


    @Test
    public void testThreadPoolReplacement() throws Exception {
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();
        a.setReceiver(r1);
        b.setReceiver(r2);
        
        a.connect("TransportThreadPoolTest");
        b.connect("TransportThreadPoolTest");
        
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        // wait until the existing thread pools have no active tasks:
        Util.waitUntilTrue(2000, 100,
                           () -> Stream.of(a, b).map(c -> c.getProtocolStack().getTransport().getThreadPool())
                             .allMatch(p -> p.getThreadPoolSizeActive() == 0));
        TP transport=a.getProtocolStack().getTransport();
        ExecutorService thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);

        transport=b.getProtocolStack().getTransport();
        thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);

        Collection<String> l1=r1.getMsgs(), l2=r2.getMsgs();
        a.send(null, "A.m1");
        b.send(null, "B.m1");
        a.send(null, "A.m2");
        b.send(null, "B.m2");

        Util.waitUntil(10000, 100, () -> l1.size() == 4 && l2.size() == 4,
                       () -> String.format("A: %s, B: %s", print(l1), print(l2)));

        System.out.println("messages A: " + print(r1.getMsgs()) + "\nmessages B: " + print(r2.getMsgs()));
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

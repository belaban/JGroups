package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class TransportThreadPoolTest extends ChannelTestBase {
    JChannel c1, c2;

    @BeforeMethod
    protected void setUp() throws Exception {
        c1=createChannel(true, 2, "A");
        c2=createChannel(c1, "B");
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(c2, c1);
    }


    @Test
    public void testThreadPoolReplacement() throws Exception {
        Receiver r1=new Receiver(), r2=new Receiver();
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        
        c1.connect("TransportThreadPoolTest");
        c2.connect("TransportThreadPoolTest");
        
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, c1, c2);
        assert c2.getView().size() == 2 : "view is " + c2.getView() + ", but should have had a size of 2";
        
        TP transport=c1.getProtocolStack().getTransport();
        ExecutorService thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);

        transport=c2.getProtocolStack().getTransport();
        thread_pool=Executors.newFixedThreadPool(2);
        transport.setThreadPool(thread_pool);
        
        c1.send(null, "hello world");
        c2.send(null, "bela");
        c1.send(null, "message 3");
        c2.send(null, "message 4");

        long start=System.currentTimeMillis();
        
        r1.getLatch().await(3000, TimeUnit.MILLISECONDS);
        r2.getLatch().await(3000, TimeUnit.MILLISECONDS);

        long diff=System.currentTimeMillis() - start;
        System.out.println("messages c1: " + print(r1.getMsgs()) + "\nmessages c2: " + print(r2.getMsgs())
                + "\ntook " + diff + " ms");
        assert r1.getMsgs().size() == 4;
        assert r2.getMsgs().size() == 4;
    }


    private static String print(Collection<Message> msgs) {
        StringBuilder sb=new StringBuilder();
        for(Message msg: msgs) {
            sb.append("\"" + msg.getObject() + "\"").append(" ");
        }
        return sb.toString();
    }


    private static class Receiver extends ReceiverAdapter {
        Collection<Message> msgs=new ConcurrentLinkedQueue<>();
        
        final CountDownLatch latch = new CountDownLatch(4);

        public Collection<Message> getMsgs() {
            return msgs;
        }
        
        public CountDownLatch getLatch(){
           return latch;
        }

        public void receive(Message msg) {
            msgs.add(msg);
            latch.countDown();
        }
    }
}

package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @version $Id: TransportThreadPoolTest.java,v 1.10 2009/06/04 09:18:00 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class TransportThreadPoolTest extends ChannelTestBase {
    JChannel c1, c2;

    @BeforeMethod
    protected void setUp() throws Exception {
        c1=createChannel(true, 2);
        c2=createChannel(c1);
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
        
        blockUntilViewsReceived(2,5000,c1,c2);

        c1.send(null, null, "hello world");
        c2.send(null, null, "bela");

        TP transport=c1.getProtocolStack().getTransport();
        ExecutorService thread_pool=Executors.newCachedThreadPool();
        transport.setDefaultThreadPool(thread_pool);

        transport=c2.getProtocolStack().getTransport();
        thread_pool=Executors.newCachedThreadPool();
        transport.setDefaultThreadPool(thread_pool);

        c1.send(null, null, "message 3");
        c2.send(null, null, "message 4");
        
        r1.getLatch().await(2000, TimeUnit.MILLISECONDS);
        r1.getLatch().await(2000, TimeUnit.MILLISECONDS);

        System.out.println("messages c1: " + print(r1.getMsgs()) + "\nmessages c2: " + print(r2.getMsgs()));
        assert r1.getMsgs().size() == 4;
        assert r2.getMsgs().size() == 4;
    }


    private static String print(List<Message> msgs) {
        StringBuilder sb=new StringBuilder();
        for(Message msg: msgs) {
            sb.append("\"" + msg.getObject() + "\"").append(" ");
        }
        return sb.toString();
    }


    private static class Receiver extends ReceiverAdapter {
        List<Message> msgs=new LinkedList<Message>();
        
        final CountDownLatch latch = new CountDownLatch(4);

        public List<Message> getMsgs() {
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

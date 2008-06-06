package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.LinkedList;

/**
 * Tests whether OOB mcast messages are blocked by regular messages (which block) - should NOT be the case
 * @author Bela Ban
 * @version $Id: OOBMcastTest.java,v 1.1 2008/06/06 14:29:52 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class OOBMcastTest extends ChannelTestBase {
    private JChannel c1, c2;
    private final ReentrantLock lock=new ReentrantLock();

    @BeforeMethod
    void init() throws Exception {
        c1=createChannel(true, 2);
        c1.setOpt(Channel.LOCAL, false);
        c2=createChannel(c1);
        setOOBPoolSize(c2);
        c1.connect("OOBMcastTest");
        c2.connect("OOBMcastTest");
        View view=c2.getView();
        assert view.size() == 2 : "view is " + view;
        lock.lock();
    }

    private static void setOOBPoolSize(JChannel channel) {
        TP transport=channel.getProtocolStack().getTransport();
        transport.setOOBMinPoolSize(1);
        transport.setOOBMaxPoolSize(2);
    }

    @AfterMethod
    private void cleanup() {
        Util.close(c2, c1);
        if(lock.isHeldByCurrentThread())
            lock.unlock();
    }

    /**
     * A and B. A multicasts a regular message, which blocks in B. Then A multicasts an OOB message, which must be
     * received by B.
     */
    public void testNonBlockingOOBMessage() throws ChannelNotConnectedException, ChannelClosedException {
        final BlockingReceiver receiver=new BlockingReceiver(lock);
        final int NUM=10;
        c2.setReceiver(receiver);

        c1.send(new Message(null, null, 1L));
        for(int i=2; i <= NUM; i++) {
            Message msg=new Message(null, null, (long)i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }
        Util.sleep(1000); // give the asynchronous msgs some time to be received
        List<Long> list=receiver.getMsgs();
        System.out.println("list = " + list);
        assert list.size() == NUM : "list is " + list;
        assert list.contains(2L);
        lock.unlock();
        Util.sleep(10);

        list=receiver.getMsgs();
        assert list.size() == NUM : "list is " + list;
        for(long i=1; i <= NUM; i++)
            assert list.contains(i);
    }


    private static class BlockingReceiver extends ReceiverAdapter {
        final Lock lock;
        final List<Long> msgs=new LinkedList<Long>();

        public BlockingReceiver(Lock lock) {
            this.lock=lock;
        }

        public List<Long> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            System.out.println("got " + (msg.isFlagSet(Message.OOB)? "OOB" : "regular") + " message "
                    + "from " + msg.getSrc() + ": " + msg.getObject());
            if(!msg.isFlagSet(Message.OOB)) {
                System.out.println("acquiring lock");
                lock.lock();
                System.out.println("acquired lock successfully");
            }

            msgs.add((Long)msg.getObject());
        }
        
    }
}

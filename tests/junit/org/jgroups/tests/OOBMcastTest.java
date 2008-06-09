package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 * @version $Id: OOBMcastTest.java,v 1.5.2.2 2008/06/09 09:26:54 belaban Exp $
 */
public class OOBMcastTest extends ChannelTestBase {
    private JChannel c1, c2;
    private ReentrantLock lock;


    protected void setUp() throws Exception {
        super.setUp();
        c1=createChannel();
        c1.setOpt(Channel.LOCAL, false);
        c2=createChannel();
        setOOBPoolSize(c2);
        c1.connect("OOBMcastTest");
        c2.connect("OOBMcastTest");
        View view=c2.getView();
        System.out.println("view = " + view);
        assert view.size() == 2 : "view is " + view;
        lock=new ReentrantLock();
        lock.lock();
    }

    protected void tearDown() throws Exception {
        if(lock.isHeldByCurrentThread())
            lock.unlock();
        Util.sleep(1000);
        Util.close(c2, c1);
        super.tearDown();
    }



    /**
     * A and B. A multicasts a regular message, which blocks in B. Then A multicasts an OOB message, which must be
     * received by B.
     */
    public void testNonBlockingUnicastOOBMessage() throws ChannelNotConnectedException, ChannelClosedException {
        Address dest=c2.getLocalAddress();
        send(dest);
    }

    public void testNonBlockingMulticastOOBMessage() throws ChannelNotConnectedException, ChannelClosedException {
        send(null);
    }

    private void send(Address dest) throws ChannelNotConnectedException, ChannelClosedException {
        final BlockingReceiver receiver=new BlockingReceiver(lock);
        final int NUM=10;
        c2.setReceiver(receiver);

        c1.send(new Message(dest, null, 1L));
        Util.sleep(1000); // this (regular) message needs to be received first

        for(int i=2; i <= NUM; i++) {
            Message msg=new Message(dest, null, (long)i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }
        Util.sleep(1000); // give the asynchronous msgs some time to be received
        List<Long> list=receiver.getMsgs();
        System.out.println("list = " + list);
        assertEquals("list is " + list, list.size(), NUM-1);
        assertTrue(list.contains(2L));

        Util.sleep(2000);
        System.out.println("[" + Thread.currentThread().getName() + "]: unlocking lock");
        lock.unlock();
        Util.sleep(10);

        list=receiver.getMsgs();
        assertEquals("list is " + list, list.size(), NUM);
        for(long i=1; i <= NUM; i++)
            assertTrue(list.contains(i));
    }


     private static void setOOBPoolSize(JChannel channel) {
        TP transport=channel.getProtocolStack().getTransport();
        transport.setOOBMinPoolSize(1);
        transport.setOOBMaxPoolSize(2);
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
            System.out.println("[" + Thread.currentThread().getName() + "]: got " + (msg.isFlagSet(Message.OOB)? "OOB" : "regular") + " message "
                    + "from " + msg.getSrc() + ": " + msg.getObject());
            if(!msg.isFlagSet(Message.OOB)) {
                System.out.println("[" + Thread.currentThread().getName() + "]: acquiring lock");
                lock.lock();
                System.out.println("[" + Thread.currentThread().getName() + "]: acquired lock successfully");
                lock.unlock();
            }

            msgs.add((Long)msg.getObject());
        }
        
    }
}

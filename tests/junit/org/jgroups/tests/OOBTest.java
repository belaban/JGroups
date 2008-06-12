package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 * @version $Id: OOBTest.java,v 1.4 2008/06/12 09:39:26 belaban Exp $
 */
@Test(groups="temp",sequential=true)
public class OOBTest extends ChannelTestBase {
    private JChannel c1, c2;
    private ReentrantLock lock;

    @BeforeMethod
    void init() throws Exception {
        c1=createChannel(true, 2);
        c1.setOpt(Channel.LOCAL, false);
        c2=createChannel(c1);
        setOOBPoolSize(c2);
        c1.connect("OOBMcastTest");
        c2.connect("OOBMcastTest");
        View view=c2.getView();
        System.out.println("view = " + view);
        assert view.size() == 2 : "view is " + view;
        lock=new ReentrantLock();
        lock.lock();
    }


    @AfterMethod
    private void cleanup() {
        if(lock.isHeldByCurrentThread())
            lock.unlock();
        Util.sleep(1000);
        Util.close(c2, c1);
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


    /**
     * Tests sending 1, 2 (OOB) and 3, where they are received in the order 1, 3, 2. Message 3 should not get delivered
     * until message 4 is received (http://jira.jboss.com/jira/browse/JGRP-780)
     */
    public void testRegularAndOOBUnicasts() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, UNICAST.class);

        Address dest=c2.getLocalAddress();
        Message m1=new Message(dest, null, 1);
        Message m2=new Message(dest, null, 2);
        m2.setFlag(Message.OOB);
        Message m3=new Message(dest, null, 3);

        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        c1.send(m1);
        discard.setDropDownUnicasts(1);
        c1.send(m2);
        c1.send(m3);

        Util.sleep(1000); // time for potential retransmission
        List<Integer> list=receiver.getMsgs();
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }

    public void testRegularAndOOBUnicasts2() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, UNICAST.class);

        Address dest=c2.getLocalAddress();
        Message m1=new Message(dest, null, 1);
        Message m2=new Message(dest, null, 2);
        m2.setFlag(Message.OOB);
        Message m3=new Message(dest, null, 3);
        m3.setFlag(Message.OOB);
        Message m4=new Message(dest, null, 4);

        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        c1.send(m1);

        discard.setDropDownUnicasts(1);
        c1.send(m3);

        discard.setDropDownUnicasts(1);
        c1.send(m2);
        
        c1.send(m4);

        Util.sleep(1000); // time for potential retransmission
        List<Integer> list=receiver.getMsgs();
        System.out.println("list = " + list);
        assert list.size() == 4 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3) && list.contains(4);
    }

    public void testRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK.class);
        c1.setOpt(Channel.LOCAL, false);

        Address dest=null; // send to all
        Message m1=new Message(dest, null, 1);
        Message m2=new Message(dest, null, 2);
        m2.setFlag(Message.OOB);
        Message m3=new Message(dest, null, 3);

        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        c1.send(m1);
        discard.setDropDownMulticasts(1);
        c1.send(m2);
        c1.send(m3);

        Util.sleep(500);
        List<Integer> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            System.out.println("list = " + list);
            if(list.size() == 3)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
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

        Util.sleep(500);
        List<Long> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            System.out.println("list = " + list);
            if(list.size() == NUM -1)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }

        assert list.size() == NUM-1 : "list is " + list;
        assert list.contains(2L);

        Util.sleep(2000);
        System.out.println("[" + Thread.currentThread().getName() + "]: unlocking lock");
        lock.unlock();
        Util.sleep(10);

        list=receiver.getMsgs();
        assert list.size() == NUM : "list is " + list;
        for(long i=1; i <= NUM; i++)
            assert list.contains(i);
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
            // System.out.println("[" + Thread.currentThread().getName() + "]: got " + (msg.isFlagSet(Message.OOB)? "OOB" : "regular") + " message "
               //     + "from " + msg.getSrc() + ": " + msg.getObject());
            if(!msg.isFlagSet(Message.OOB)) {
                //System.out.println("[" + Thread.currentThread().getName() + "]: acquiring lock");
                lock.lock();
                //System.out.println("[" + Thread.currentThread().getName() + "]: acquired lock successfully");
                lock.unlock();
            }

            msgs.add((Long)msg.getObject());
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final List<Integer> msgs=new ArrayList<Integer>(3);

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            Integer val=(Integer)msg.getObject();
            msgs.add(val);
            // System.out.println("received " + msg + ": " + val + ", headers:\n" + msg.getHeaders());
        }
    }
}

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 * @version $Id: OOBTest.java,v 1.2.2.5 2008/06/16 06:07:38 belaban Exp $
 */
public class OOBTest extends ChannelTestBase {
    private JChannel c1, c2;
    private ReentrantLock lock;


    protected void setUp() throws Exception {
        super.setUp();
        c1=createChannel();
        c1.setOpt(Channel.LOCAL, false);
        c2=createChannel();
        c2.setOpt(Channel.LOCAL, false);
        setOOBPoolSize(c2);
        c1.connect("OOBMcastTest");
        c2.connect("OOBMcastTest");
        View view=c2.getView();
        System.out.println("view = " + view);
        assertEquals("view is " + view, 2, view.size());
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

        Util.sleep(500); // time for potential retransmission
        List<Integer> list=receiver.getMsgs();
        assertEquals("list is " + list, 3, list.size());
        assertTrue(list.contains(1) && list.contains(2) && list.contains(3));
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
        assertSame("list is " + list, list.size(), 4);
        assertTrue(list.contains(1) && list.contains(2) && list.contains(3) && list.contains(4));
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

        Util.sleep(1000); // time for potential retransmission
        List<Integer> list=receiver.getMsgs();
        System.out.println("list = " + list);
        assertEquals("list is " + list, 3, list.size());
        assertTrue(list.contains(1) && list.contains(2) && list.contains(3));
    }
    
    public void testRandomRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK.class);
        Address dest=null; // send to all
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        final int NUM_MSGS=100;
        final int NUM_THREADS=10;
        send(dest, NUM_MSGS, NUM_THREADS, 0.5, 0.5, discard);
        List<Integer> one=r1.getMsgs(), two=r2.getMsgs();
        for(int i=0; i < 20; i++) {
            if(one.size() == NUM_MSGS && two.size() == NUM_MSGS)
                break;
            System.out.print(".");
            Util.sleep(1000);
        }
        System.out.println("");
        check(NUM_MSGS, one, two);
    }

    private void send(final Address dest, final int num_msgs, final int num_threads,
                      final double oob_prob, final double drop_prob, final DISCARD discard) throws Exception {
        Channel sender;
        boolean oob, drop;
        final boolean multicast=dest == null || dest.isMulticastAddress();
        Message msg;

        if(num_threads <= 0)
            throw new IllegalArgumentException("number of threads <= 0");

        if(num_msgs % num_threads != 0)
            throw new IllegalArgumentException("number of messages ( " + num_msgs + ") needs to be divisible by " +
                    "the number o threads (" + num_threads + ")");

        if(num_threads > 1) {
            final int msgs_per_thread=num_msgs / num_threads;
            Thread[] threads=new Thread[num_threads];
            final AtomicInteger counter=new AtomicInteger(0);
            for(int i=0; i < threads.length; i++) {
                threads[i]=new Thread() {
                    public void run() {
                        for(int i=0; i < msgs_per_thread; i++) {
                            Channel sender=Util.tossWeightedCoin(0.5) ? c1 : c2;
                            boolean oob=Util.tossWeightedCoin(oob_prob);
                            boolean drop=Util.tossWeightedCoin(drop_prob);
                            Message msg=new Message(dest, null, counter.getAndIncrement());
                            if(oob)
                                msg.setFlag(Message.OOB);
                            if(drop) {
                                if(multicast)
                                    discard.setDropDownMulticasts(1);
                                else
                                    discard.setDropDownUnicasts(1);
                            }
                            try {
                                sender.send(msg);
                            }
                            catch(Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                };
            }
            for(int i=0; i < threads.length; i++) {
                threads[i].start();
            }
            for(int i=0; i < threads.length; i++) {
                threads[i].join(20000);
            }
            return;
        }


        for(int i=0; i < num_msgs; i++) {
            sender=Util.tossWeightedCoin(0.5) ? c1 : c2;
            oob=Util.tossWeightedCoin(oob_prob);
            drop=Util.tossWeightedCoin(drop_prob);
            msg=new Message(dest, null, i);
            if(oob)
                msg.setFlag(Message.OOB);
            if(drop) {
                if(multicast)
                    discard.setDropDownMulticasts(1);
                else
                    discard.setDropDownUnicasts(1);
            }
            sender.send(msg);
        }
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
        assertEquals("list is " + list, NUM -1, list.size());
        assertTrue(list.contains(2L));

        Util.sleep(2000);
        System.out.println("[" + Thread.currentThread().getName() + "]: unlocking lock");
        lock.unlock();
        Util.sleep(10);

        list=receiver.getMsgs();
        assertEquals("list is " + list, NUM, list.size());
        for(long i=1; i <= NUM; i++)
            assertTrue(list.contains(i));
    }
    
    private static void check(final int num_expected_msgs, List<Integer>... lists) {
        for(List<Integer> list: lists) {
            System.out.println("list: " + list);
        }

        for(List<Integer> list: lists) {
            assert list.size() == num_expected_msgs : "expected " + num_expected_msgs + " elements, but got " +
                    list.size() + " (list=" + list + ")";
            for(int i=0; i < num_expected_msgs; i++)
                assert list.contains(i);
        }
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

package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 * @version $Id: OOBTest.java,v 1.16 2009/04/09 09:11:16 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class OOBTest extends ChannelTestBase {
    private JChannel c1, c2;

    @BeforeMethod
    public void init() throws Exception {
        c1=createChannel(true, 2);
        // c1.setOpt(Channel.LOCAL, false);
        c2=createChannel(c1);
        setOOBPoolSize(c1, c2);
        setStableGossip(c1, c2);
        c1.connect("OOBMcastTest");
        c2.connect("OOBMcastTest");
        View view=c2.getView();
        log.info("view = " + view);
        assert view.size() == 2 : "view is " + view;
    }


    @AfterMethod
    public void cleanup() {
        Util.sleep(1000);
        Util.close(c2, c1);
    }


    /**
     * A and B. A multicasts a regular message, which blocks in B. Then A multicasts an OOB message, which must be
     * received by B.
     */
    public void testNonBlockingUnicastOOBMessage() throws ChannelNotConnectedException, ChannelClosedException {
        Address dest=c2.getAddress();
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

        Address dest=c2.getAddress();
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

        Address dest=c2.getAddress();
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
        Util.sleep(1000); // sleep some time to receive all messages

        List<Integer> list=receiver.getMsgs();
        int count=10;
        while(list.size() < 4 && --count > 0) {
            Util.sleep(500); // time for potential retransmission
        }
        log.info("list = " + list);
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
            log.info("list = " + list);
            if(list.size() == 3)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }


    // @Test(invocationCount=5)
    public void testRandomRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(c1.getAddress());
        discard.setDownDiscardRate(0.5);   
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK.class);
        Address dest=null; // send to all
        MyReceiver r1=new MyReceiver(), r2=new MyReceiver();
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        final int NUM_MSGS=100;
        final int NUM_THREADS=10;       
        send(dest, NUM_MSGS, NUM_THREADS, 0.5);
        List<Integer> one=r1.getMsgs(), two=r2.getMsgs();
        for(int i=0; i < 20; i++) {
            if(one.size() == NUM_MSGS && two.size() == NUM_MSGS)
                break;
            log.info("one size " + one.size() + ", two size " + two.size());
            Util.sleep(1000);
        }
        log.info("one size " + one.size() + ", two size " + two.size());        
        check(NUM_MSGS, one, two);
    }


    private void send(final Address dest, final int num_msgs, final int num_threads,
                      final double oob_prob) throws Exception {
        Channel sender;
        boolean oob;       
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
                        for(int j=0; j < msgs_per_thread; j++) {
                            Channel sender=Util.tossWeightedCoin(0.5) ? c1 : c2;
                            boolean oob=Util.tossWeightedCoin(oob_prob);                            
                            Message msg=new Message(dest, null, counter.getAndIncrement());                            
                            if(oob)
                               msg.setFlag(Message.OOB);                            
                            try {
                                sender.send(msg);                               
                            }
                            catch(Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                };
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
            msg=new Message(dest, null, i);
            if(oob)
               msg.setFlag(Message.OOB);          
            sender.send(msg);            
        }
    }


    private void send(Address dest) throws ChannelNotConnectedException, ChannelClosedException {
        final ReentrantLock lock=new ReentrantLock();
        final BlockingReceiver receiver=new BlockingReceiver(lock);
        final int NUM=10;
        c2.setReceiver(receiver);


        lock.lock();
        c1.send(new Message(dest, null, 1L));
        Util.sleep(1000); // this (regular) message needs to be received first

        for(int i=2; i <= NUM; i++) {
            Message msg=new Message(dest, null, (long)i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }

        Util.sleep(500); // sleep some time to receive all messages
        List<Long> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            log.info("list = " + list);
            if(list.size() == NUM-1)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }

        System.out.println("list = " + list);

        assert list.size() == NUM-1 : "list is " + list;
        assert list.contains(2L);

        log.info("[" + Thread.currentThread().getName() + "]: unlocking lock");
        if(lock.isHeldByCurrentThread())
            lock.unlock();
        Util.sleep(10);

        list=receiver.getMsgs();
        System.out.println("list = " + list);
        assert list.size() == NUM : "list is " + list;
        for(long i=1; i <= NUM; i++)
            assert list.contains(i);
    }

    private  void check(final int num_expected_msgs, List<Integer>... lists) {
        for(List<Integer> list: lists) {
            log.info("list: " + list);
        }

        for(List<Integer> list: lists) {
            assert list.size() == num_expected_msgs : "expected " + num_expected_msgs + " elements, but got " +
                    list.size() + " (list=" + list + ")";
            for(int i=0; i < num_expected_msgs; i++)
                assert list.contains(i);
        }
    }


    private static void setOOBPoolSize(JChannel... channels) {
        for(Channel channel: channels) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setOOBMinPoolSize(1);
            transport.setOOBMaxPoolSize(2);
        }
    }

    private static void setStableGossip(JChannel... channels) {
        for(Channel channel: channels) {
            ProtocolStack stack=channel.getProtocolStack();
            STABLE stable=(STABLE)stack.findProtocol(STABLE.class);
            stable.setDesiredAverageGossip(2000);
        }
    }

    private static class BlockingReceiver extends ReceiverAdapter {
        final Lock lock;
        final List<Long> msgs=Collections.synchronizedList(new LinkedList<Long>());

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
                // System.out.println("[" + Thread.currentThread().getName() + "]: acquiring lock");
                lock.lock();
                // System.out.println("[" + Thread.currentThread().getName() + "]: acquired lock successfully");
                lock.unlock();
            }

            msgs.add((Long)msg.getObject());
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final List<Integer> msgs=Collections.synchronizedList(new ArrayList<Integer>());

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            Integer val=(Integer)msg.getObject();
            msgs.add(val);            
        }
    }
}

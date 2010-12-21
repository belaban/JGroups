package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class OOBTest extends ChannelTestBase {
    private JChannel c1, c2;

    @BeforeMethod
    public void init() throws Exception {
        c1=createChannel(true, 2);
        c1.setName("C1");
        c2=createChannel(c1);
        c2.setName("C2");
        setOOBPoolSize(c1, c2);
        setStableGossip(c1, c2);
        c1.connect("OOBTest");
        c2.connect("OOBTest");
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
        stack.insertProtocol(discard, ProtocolStack.BELOW, UNICAST.class, UNICAST2.class);

        Address dest=c2.getAddress();
        Message m1=new Message(dest, null, 1);
        Message m2=new Message(dest, null, 2);
        m2.setFlag(Message.OOB);
        Message m3=new Message(dest, null, 3);

        MyReceiver receiver=new MyReceiver("C2");
        c2.setReceiver(receiver);
        c1.send(m1);
        discard.setDropDownUnicasts(1);
        c1.send(m2);
        c1.send(m3);

        sendStableMessages(c1,c2);
        Util.sleep(1000); // time for potential retransmission
        Collection<Integer> list=receiver.getMsgs();
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }

    public void testRegularAndOOBUnicasts2() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, UNICAST.class, UNICAST2.class);

        Address dest=c2.getAddress();
        Message m1=new Message(dest, null, 1);
        Message m2=new Message(dest, null, 2);
        m2.setFlag(Message.OOB);
        Message m3=new Message(dest, null, 3);
        m3.setFlag(Message.OOB);
        Message m4=new Message(dest, null, 4);

        MyReceiver receiver=new MyReceiver("C2");
        c2.setReceiver(receiver);
        c1.send(m1);

        discard.setDropDownUnicasts(1);
        c1.send(m3);

        discard.setDropDownUnicasts(1);
        c1.send(m2);
        
        c1.send(m4);
        Util.sleep(1000); // sleep some time to receive all messages

        Collection<Integer> list=receiver.getMsgs();
        int count=10;
        while(list.size() < 4 && --count > 0) {
            Util.sleep(500); // time for potential retransmission
            sendStableMessages(c1,c2);
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

        MyReceiver receiver=new MyReceiver("C2");
        c2.setReceiver(receiver);
        c1.send(m1);
        discard.setDropDownMulticasts(1);
        c1.send(m2);
        c1.send(m3);

        Util.sleep(500);
        Collection<Integer> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            log.info("list = " + list);
            if(list.size() == 3)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
            sendStableMessages(c1,c2);
        }
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }


    @Test(invocationCount=5)
    @SuppressWarnings("unchecked")
    public void testRandomRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(c1.getAddress());
        discard.setDownDiscardRate(0.5);
        ProtocolStack stack=c1.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK.class);
        MyReceiver r1=new MyReceiver("C1"), r2=new MyReceiver("C2");
        c1.setReceiver(r1);
        c2.setReceiver(r2);
        final int NUM_MSGS=20;
        final int NUM_THREADS=10;

        send(null, NUM_MSGS, NUM_THREADS, 0.5); // send on random channel (c1 or c2)
        
        Collection<Integer> one=r1.getMsgs(), two=r2.getMsgs();
        for(int i=0; i < 10; i++) {
            if(one.size() == NUM_MSGS && two.size() == NUM_MSGS)
                break;
            log.info("one size " + one.size() + ", two size " + two.size());
            Util.sleep(1000);
            sendStableMessages(c1,c2);
        }
        log.info("one size " + one.size() + ", two size " + two.size());

        stack.removeProtocol("DISCARD");

        for(int i=0; i < 5; i++) {
            if(one.size() == NUM_MSGS && two.size() == NUM_MSGS)
                break;
            sendStableMessages(c1,c2);
            Util.sleep(500);
        }
        System.out.println("C1 received " + one.size() + " messages ("+ NUM_MSGS + " expected)" +
                "\nC2 received " + two.size() + " messages ("+ NUM_MSGS + " expected)");

        check(NUM_MSGS, one, two);
    }

    /**
     * Tests https://jira.jboss.org/jira/browse/JGRP-1079
     * @throws ChannelNotConnectedException
     * @throws ChannelClosedException
     */
    public void testOOBMessageLoss() throws ChannelNotConnectedException, ChannelClosedException {
        Util.close(c2); // we only need 1 channel
        MyReceiver receiver=new MySleepingReceiver("C1", 1000);
        c1.setReceiver(receiver);

        TP transport=c1.getProtocolStack().getTransport();
        transport.setOOBRejectionPolicy("discard");

        final int NUM=10;

        for(int i=1; i <= NUM; i++) {
            Message msg=new Message(null, null, i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }
        STABLE stable=(STABLE)c1.getProtocolStack().findProtocol(STABLE.class);
        if(stable != null)
            stable.runMessageGarbageCollection();
        Collection<Integer> msgs=receiver.getMsgs();

        for(int i=0; i < 20; i++) {
            if(msgs.size() == NUM)
                break;
            Util.sleep(1000);
            sendStableMessages(c1,c2);
        }

        System.out.println("msgs = " + Util.print(msgs));

        assert msgs.size() == NUM : "expected " + NUM + " messages but got " + msgs.size() + ", msgs=" + Util.print(msgs);
        for(int i=1; i <= NUM; i++) {
            assert msgs.contains(i);
        }
    }

    /**
     * Tests https://jira.jboss.org/jira/browse/JGRP-1079 for unicast messages
     * @throws ChannelNotConnectedException
     * @throws ChannelClosedException
     */
    public void testOOBUnicastMessageLoss() throws ChannelNotConnectedException, ChannelClosedException {
        MyReceiver receiver=new MySleepingReceiver("C2", 1000);
        c2.setReceiver(receiver);

        c1.getProtocolStack().getTransport().setOOBRejectionPolicy("discard");

        final int NUM=10;
        final Address dest=c2.getAddress();
        for(int i=1; i <= NUM; i++) {
            Message msg=new Message(dest, null, i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }

        Collection<Integer> msgs=receiver.getMsgs();

        for(int i=0; i < 20; i++) {
            if(msgs.size() == NUM)
                break;
            Util.sleep(1000);
            // sendStableMessages(c1,c2); // not needed for unicasts !
        }

        assert msgs.size() == NUM : "expected " + NUM + " messages but got " + msgs.size() + ", msgs=" + Util.print(msgs);
        for(int i=1; i <= NUM; i++) {
            assert msgs.contains(i);
        }
    }



    private void send(final Address dest, final int num_msgs, final int num_threads, final double oob_prob) throws Exception {
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
                            int num=counter.incrementAndGet();
                            Message msg=new Message(dest, null, num);
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
            Channel sender=Util.tossWeightedCoin(0.5) ? c1 : c2;
            boolean oob=Util.tossWeightedCoin(oob_prob);
            Message msg=new Message(dest, null, i);
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

        System.out.println("[" + Thread.currentThread().getName() + "]: locking lock");
        lock.lock();
        c1.send(new Message(dest, null, 1));
        for(int i=2; i <= NUM; i++) {
            Message msg=new Message(dest, null, i);
            msg.setFlag(Message.OOB);
            c1.send(msg);
        }
        sendStableMessages(c1, c2);
        Util.sleep(500);

        List<Integer> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            if(list.size() == NUM-1)
                break;
            System.out.println("list = " + list);
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }

        System.out.println("list = " + list);

        assert list.size() == NUM-1 : "list is " + list;
        assert list.contains(2) && list.contains(10);

        System.out.println("[" + Thread.currentThread().getName() + "]: unlocking lock");
        lock.unlock();

        for(int i=0; i < 10; i++) {
            if(list.size() == NUM)
                break;
            System.out.println("list = " + list);
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }

        System.out.println("list = " + list);
        assert list.size() == NUM : "list is " + list;
        for(int i=1; i <= NUM; i++)
            assert list.contains(i);
    }

    @SuppressWarnings("unchecked")
    private  void check(final int num_expected_msgs, Collection<Integer>... lists) {
        for(Collection<Integer> list: lists) {
            log.info("list: " + list);
        }

        for(Collection<Integer> list: lists) {
            Collection<Integer> missing=new TreeSet<Integer>();
            if(list.size() != num_expected_msgs) {
                for(int i=0; i < num_expected_msgs; i++)
                    missing.add(i);

                missing.removeAll(list);
                assert list.size() == num_expected_msgs : "expected " + num_expected_msgs + " elements, but got " +
                        list.size() + " (list=" + list + "), missing=" + missing;

            }
        }
    }


    private static void setOOBPoolSize(JChannel... channels) {
        for(Channel channel: channels) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setOOBThreadPoolMinThreads(1);
            transport.setOOBThreadPoolMaxThreads(2);
        }
    }

    private static void setStableGossip(JChannel... channels) {
        for(Channel channel: channels) {
            ProtocolStack stack=channel.getProtocolStack();
            STABLE stable=(STABLE)stack.findProtocol(STABLE.class);
            stable.setDesiredAverageGossip(2000);
        }
    }

    private static void sendStableMessages(JChannel ... channels) {
        for(JChannel ch: channels) {
            STABLE stable=(STABLE)ch.getProtocolStack().findProtocol(STABLE.class);
            if(stable != null)
                stable.runMessageGarbageCollection();
        }
    }

    private static class BlockingReceiver extends ReceiverAdapter {
        final Lock lock;
        final List<Integer> msgs=Collections.synchronizedList(new LinkedList<Integer>());

        public BlockingReceiver(Lock lock) {
            this.lock=lock;
        }

        public List<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            if(!msg.isFlagSet(Message.OOB)) {
                lock.lock();
                lock.unlock();
            }

            msgs.add((Integer)msg.getObject());
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final Collection<Integer> msgs=new ConcurrentLinkedQueue<Integer>();
        final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public Collection<Integer> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            Integer val=(Integer)msg.getObject();
            msgs.add(val);
        }
    }

    private static class MySleepingReceiver extends MyReceiver {
        final long sleep_time;

        public MySleepingReceiver(String name, long sleep_time) {
            super(name);
            this.sleep_time=sleep_time;
        }

        public void receive(Message msg) {
            super.receive(msg);
            System.out.println("-- received " + msg.getObject());
            Util.sleep(sleep_time);
        }
    }
}

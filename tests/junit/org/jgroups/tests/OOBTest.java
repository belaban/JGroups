package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests whether OOB multicast/unicast messages are blocked by regular messages (which block) - should NOT be the case.
 * The class name is a misnomer, both multicast *and* unicast messages are tested
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class OOBTest extends ChannelTestBase {
    private JChannel a, b;

    @BeforeMethod
    void init() throws Exception {
        a=createChannel(true, 2, "A");
        b=createChannel(a, "B");
        setOOBPoolSize(a,b);
        setStableGossip(a,b);
        a.connect("OOBTest");
        b.connect("OOBTest");
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, a, b);
    }


    @AfterMethod void cleanup() {Util.close(b, a);}


    /**
     * A and B. A multicasts a regular message, which blocks in B. Then A multicasts an OOB message, which must be
     * received by B.
     */
    public void testNonBlockingUnicastOOBMessage() throws Exception {
        send(b.getAddress());
    }

    public void testNonBlockingMulticastOOBMessage() throws Exception {
        send(null);
    }


    /**
     * Tests sending 1, 2 (OOB) and 3, where they are received in the order 1, 3, 2. Message 3 should not get delivered
     * until message 4 is received (http://jira.jboss.com/jira/browse/JGRP-780)
     */
    public void testRegularAndOOBUnicasts() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW,(Class<? extends Protocol>[])Util.getUnicastProtocols());

        Address dest=b.getAddress();
        Message m1=new Message(dest, 1);
        Message m2=new Message(dest, 2).setFlag(Message.Flag.OOB);
        Message m3=new Message(dest, 3);

        MyReceiver receiver=new MyReceiver("B");
        b.setReceiver(receiver);
        a.send(m1);
        discard.setDropDownUnicasts(1);
        a.send(m2);
        a.send(m3);

        Collection<Integer> list=receiver.getMsgs();
        int count=10;
        while(list.size() < 3 && --count > 0) {
            Util.sleep(500); // time for potential retransmission
            sendStableMessages(a,b);
        }

        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }

    public void testRegularAndOOBUnicasts2() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW,(Class<? extends Protocol>[])Util.getUnicastProtocols());

        Address dest=b.getAddress();
        Message m1=new Message(dest, 1);
        Message m2=new Message(dest, 2).setFlag(Message.Flag.OOB);
        Message m3=new Message(dest, 3).setFlag(Message.Flag.OOB);
        Message m4=new Message(dest, 4);

        MyReceiver receiver=new MyReceiver("B");
        b.setReceiver(receiver);
        a.send(m1);

        discard.setDropDownUnicasts(2);
        a.send(m2); // dropped
        a.send(m3); // dropped
        a.send(m4);

        Collection<Integer> list=receiver.getMsgs();
        int count=10;
        while(list.size() < 4 && --count > 0) {
            Util.sleep(500); // time for potential retransmission
            sendStableMessages(a,b);
        }
        System.out.println("list = " + list);
        assert list.size() == 4 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3) && list.contains(4);
    }

    public void testRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.BELOW, NAKACK2.class);
        a.setDiscardOwnMessages(true);

        Address dest=null; // send to all
        Message m1=new Message(dest, 1);
        Message m2=new Message(dest, 2).setFlag(Message.Flag.OOB);
        Message m3=new Message(dest, 3);

        MyReceiver receiver=new MyReceiver("B");
        b.setReceiver(receiver);
        a.send(m1);
        discard.setDropDownMulticasts(1);
        a.send(m2);
        a.send(m3);

        Util.sleep(500);
        Collection<Integer> list=receiver.getMsgs();
        for(int i=0; i < 10; i++) {
            System.out.println("list = " + list);
            if(list.size() == 3)
                break;
            Util.sleep(1000); // give the asynchronous msgs some time to be received
            sendStableMessages(a,b);
        }
        assert list.size() == 3 : "list is " + list;
        assert list.contains(1) && list.contains(2) && list.contains(3);
    }


    @Test(invocationCount=5)
    @SuppressWarnings("unchecked")
    public void testRandomRegularAndOOBMulticasts() throws Exception {
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(a.getAddress());
        discard.setUpDiscardRate(0.5);
        ProtocolStack stack=a.getProtocolStack();
        stack.insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        MyReceiver r1=new MyReceiver("A"), r2=new MyReceiver("B");
        a.setReceiver(r1);
        b.setReceiver(r2);
        final int NUM_MSGS=20;
        final int NUM_THREADS=10;

        send(null, NUM_MSGS, NUM_THREADS, 0.5); // send on random channel (a or b)
        
        Collection<Integer> one=r1.getMsgs(), two=r2.getMsgs();
        for(int i=0; i < 20; i++) {
            if(one.size() == NUM_MSGS && two.size() == NUM_MSGS)
                break;
            System.out.println("A size " + one.size() + ", B size " + two.size());
            sendStableMessages(a,b);
            Util.sleep(1000);
        }
        System.out.println("A size " + one.size() + ", B size " + two.size());

        stack.removeProtocol(DISCARD.class);

        System.out.println("A received " + one.size() + " messages (" + NUM_MSGS + " expected)" +
                             "\nB received " + two.size() + " messages (" + NUM_MSGS + " expected)");

        check(NUM_MSGS, one, two);
    }

    /**
     * Tests https://jira.jboss.org/jira/browse/JGRP-1079
     */
    public void testOOBMessageLoss() throws Exception {
        Util.close(b); // we only need 1 channel
        MyReceiver receiver=new MySleepingReceiver("A", 1000);
        a.setReceiver(receiver);

        TP transport=a.getProtocolStack().getTransport();
        transport.setOOBRejectionPolicy("discard");

        final int NUM=10;
        for(int i=1; i <= NUM; i++)
            a.send(new Message(null, i).setFlag(Message.Flag.OOB));

        STABLE stable=(STABLE)a.getProtocolStack().findProtocol(STABLE.class);
        if(stable != null)
            stable.gc();
        Collection<Integer> msgs=receiver.getMsgs();

        for(int i=0; i < 20; i++) {
            if(msgs.size() == NUM)
                break;
            Util.sleep(1000);
            sendStableMessages(a,b);
        }

        System.out.println("msgs = " + Util.print(msgs));

        assert msgs.size() == NUM : "expected " + NUM + " messages but got " + msgs.size() + ", msgs=" + Util.print(msgs);
        for(int i=1; i <= NUM; i++)
            assert msgs.contains(i);
    }

    /**
     * Tests https://jira.jboss.org/jira/browse/JGRP-1079 for unicast messages
     */
    public void testOOBUnicastMessageLoss() throws Exception {
        MyReceiver receiver=new MySleepingReceiver("B", 1000);
        b.setReceiver(receiver);
        a.getProtocolStack().getTransport().setOOBRejectionPolicy("discard");

        final int NUM=10;
        final Address dest=b.getAddress();
        for(int i=1; i <= NUM; i++)
            a.send(new Message(dest, i).setFlag(Message.Flag.OOB));

        Collection<Integer> msgs=receiver.getMsgs();
        for(int i=0; i < 20; i++) {
            if(msgs.size() == NUM)
                break;
            Util.sleep(1000);
        }

        assert msgs.size() == NUM : "expected " + NUM + " messages but got " + msgs.size() + ", msgs=" + Util.print(msgs);
        for(int i=1; i <= NUM; i++)
            assert msgs.contains(i);
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
                            Channel sender=Util.tossWeightedCoin(0.5) ? a : b;
                            boolean oob=Util.tossWeightedCoin(oob_prob);
                            int num=counter.incrementAndGet();
                            Message msg=new Message(dest, num);
                            if(oob)
                               msg.setFlag(Message.Flag.OOB);
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
            Channel sender=Util.tossWeightedCoin(0.5) ? a : b;
            boolean oob=Util.tossWeightedCoin(oob_prob);
            Message msg=new Message(dest, null, i);
            if(oob)
               msg.setFlag(Message.Flag.OOB);
            sender.send(msg);            
        }
    }


    private void send(Address dest) throws Exception {
        final CountDownLatch latch=new CountDownLatch(1);
        final BlockingReceiver receiver=new BlockingReceiver(latch);
        final int NUM=10;
        b.setReceiver(receiver);

        a.send(dest, 1); // the only regular message
        for(int i=2; i <= NUM; i++)
            a.send(new Message(dest, i).setFlag(Message.Flag.OOB));

        sendStableMessages(a,b);
        List<Integer> list=receiver.getMsgs();
        for(int i=0; i < 20; i++) {
            if(list.size() == NUM-1)
                break;
            sendStableMessages(a,b);
            Util.sleep(500); // give the asynchronous msgs some time to be received
        }

        System.out.println("list = " + list);
        assert list.size() == NUM-1 : "list is " + list;
        assert list.contains(2) && list.contains(10);

        System.out.println("[" + Thread.currentThread().getName() + "]: releasing latch");
        latch.countDown();

        for(int i=0; i < 20; i++) {
            if(list.size() == NUM)
                break;
            sendStableMessages(a,b);
            Util.sleep(1000); // give the asynchronous msgs some time to be received
        }

        System.out.println("list = " + list);
        assert list.size() == NUM : "list is " + list;
        for(int i=1; i <= NUM; i++)
            assert list.contains(i);
    }

    @SuppressWarnings("unchecked")
    private static void check(final int num_expected_msgs, Collection<Integer>... lists) {
        for(Collection<Integer> list: lists) {
            System.out.println("list: " + list);
        }

        for(Collection<Integer> list: lists) {
            Collection<Integer> missing=new TreeSet<Integer>();
            if(list.size() != num_expected_msgs) {
                for(int i=1; i <= num_expected_msgs; i++)
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
            transport.setOOBThreadPoolMinThreads(4);
            transport.setOOBThreadPoolMaxThreads(8);
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
                stable.gc();
            UNICAST2 uni=(UNICAST2)ch.getProtocolStack().findProtocol(UNICAST2.class);
            if(uni != null)
                uni.sendStableMessages();
        }
    }

    private static class BlockingReceiver extends ReceiverAdapter {
        final CountDownLatch latch;
        final List<Integer>  msgs=Collections.synchronizedList(new LinkedList<Integer>());

        public BlockingReceiver(CountDownLatch latch) {this.latch=latch;}

        public List<Integer> getMsgs() {return msgs;}

        public void receive(Message msg) {
            if(!msg.isFlagSet(Message.Flag.OOB)) {
                try {
                    System.out.println(Thread.currentThread() + ": waiting on latch");
                    latch.await(25000,TimeUnit.MILLISECONDS);
                    System.out.println(Thread.currentThread() + ": DONE waiting on latch");
                }
                catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }

            msgs.add((Integer)msg.getObject());
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        private final Collection<Integer> msgs=new ConcurrentLinkedQueue<Integer>();
        final String name;

        public MyReceiver(String name) {this.name=name;}

        public Collection<Integer> getMsgs() {return msgs;}

        public void receive(Message msg) {
            Integer val=(Integer)msg.getObject();
            System.out.println(name + ": <-- " + val);
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

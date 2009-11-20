

package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.SHUFFLE;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests a SEQUENCER based stack: demonstrates race condition where thread#1
 * gets seqno, thread#2 gets seqno, thread#2 sends, thread#1 tries to send but
 * is out of order.
 * 
 * In order to test total ordering, make sure that messages are sent from 
 * concurrent senders; using one sender will cause NAKACK to FIFO order 
 * the messages and the assertions in this test will still hold true, whether
 * SEQUENCER is present or not. 
 * @version $Id: SequencerOrderTest.java,v 1.13 2009/11/20 10:49:13 belaban Exp $
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class SequencerOrderTest {
    private JChannel    c1, c2, c3;
    private MyReceiver  r1, r2, r3;
    static final String GROUP="SequencerOrderTest";
    static final int    NUM_MSGS=50; // messages per thread
    static final int    NUM_THREADS=10;
    static final int    EXPECTED_MSGS=NUM_MSGS * NUM_THREADS;
    static final String props="sequencer.xml";
    private Sender[]    senders=new Sender[NUM_THREADS];


    @BeforeMethod
    void setUp() throws Exception {
        c1=new JChannel(props);
        c1.setName("A");
        c1.connect(GROUP);
        r1=new MyReceiver("A");
        c1.setReceiver(r1);

        c2=new JChannel(props);
        c2.setName("B");
        c2.connect(GROUP);
        r2=new MyReceiver("B");
        c2.setReceiver(r2);

        c3=new JChannel(props);
        c3.setName("C");
        c3.connect(GROUP);
        r3=new MyReceiver("C");
        c3.setReceiver(r3);

        AtomicInteger num=new AtomicInteger(1);

        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(NUM_MSGS, num, c1, c2, c3);
        }
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }

    @Test @SuppressWarnings("unchecked")
    public void testBroadcastSequence() throws Exception {
        insertShuffle(c1, c2, c3);
        
        // use concurrent senders to send messages to the group
        System.out.println("Starting " + senders.length + " sender threads (each sends " + NUM_MSGS + " messages)");
        for(Sender sender: senders)
            sender.start();

        for(Sender sender: senders)
            sender.join(20000);
        System.out.println("Ok, senders have completed");

        final List<String> l1=r1.getMsgs();
        final List<String> l2=r2.getMsgs();
        final List<String> l3=r3.getMsgs();
        
        System.out.println("-- verifying messages on A and B");
        verifyNumberOfMessages(EXPECTED_MSGS, l1, l2, l3);
        verifySameOrder(EXPECTED_MSGS, l1, l2, l3);
    }

    private static void insertShuffle(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=new SHUFFLE();
            shuffle.setDown(false);
            shuffle.setUp(true);
            shuffle.setMaxSize(10);
            shuffle.setMaxTime(1000);
            ch.getProtocolStack().insertProtocol(shuffle, ProtocolStack.BELOW, NAKACK.class);
            shuffle.init(); // gets the timer
        }
    }

    private static void verifyNumberOfMessages(int num_msgs, List<String> ... lists) throws Exception {
        long end_time=System.currentTimeMillis() + 10000;
        while(System.currentTimeMillis() < end_time) {
            boolean all_correct=true;
            for(List<String> list: lists) {
                if(list.size() != num_msgs) {
                    all_correct=false;
                    break;
                }
            }
            if(all_correct)
                break;
            Util.sleep(1000);
        }

        for(int i=0; i < lists.length; i++)
            System.out.println("list #" + (i+1) + ": " + lists[i]);

        for(int i=0; i < lists.length; i++)
            assert lists[i].size() == num_msgs : "list #" + (i+1) + " should have " + num_msgs + " elements";
        System.out.println("OK, all lists have the same size (" + num_msgs + ")\n");
    }



    private static void verifySameOrder(int expected_msgs, List<String> ... lists) throws Exception {
        for(int index=0; index < expected_msgs; index++) {
            String val=null;
            for(List<String> list: lists) {
                if(val == null)
                    val=list.get(index);
                else {
                    String val2=list.get(index);
                    assert val.equals(val2) : "found different values at index " + index + ": " + val + " != " + val2;
                }
            }
        }
        System.out.println("OK, all lists have the same order");
    }

    private static class Sender extends Thread {
        final int        num_msgs;
        final JChannel[] channels;
        final AtomicInteger num;

        public Sender(int num_msgs, AtomicInteger num, JChannel ... channels) {
            this.num_msgs=num_msgs;
            this.num=num;
            this.channels=channels;
        }

        public void run() {
            for(int i=1; i <= num_msgs; i++) {
                try {
                    JChannel ch=(JChannel)Util.pickRandomElement(channels);
                    String channel_name=ch.getName();
                    ch.send(null, null, channel_name + ":" + num.getAndIncrement());
                }
                catch(Exception e) {
                }
            }
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final List<String> msgs=new LinkedList<String>();

        private MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getMsgs() {
            return msgs;
        }

        public void receive(Message msg) {
            String val=(String)msg.getObject();
            if(val != null) {
                synchronized(msgs) {
                    msgs.add(val);
                }
            }
        }
    }



}

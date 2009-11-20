

package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.SEQUENCER;
import org.jgroups.protocols.SHUFFLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests a SEQUENCER based stack: demonstrates race condition where thread#1
 * gets seqno, thread#2 gets seqno, thread#2 sends, thread#1 tries to send but
 * is out of order.
 * 
 * In order to test total ordering, make sure that messages are sent from 
 * concurrent senders; using one sender will cause NAKACK to FIFO order 
 * the messages and the assertions in this test will still hold true, whether
 * SEQUENCER is present or not. 
 * @version $Id: SequencerOrderTest.java,v 1.11 2009/11/20 08:57:52 belaban Exp $
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class SequencerOrderTest {
    private JChannel    c1, c2;
    private MyReceiver  r1, r2;
    static final String GROUP="SequencerOrderTest";
    static final int    NUM_MSGS=10; // messages per thread
    static final int    NUM_THREADS=1;
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

        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(NUM_MSGS, c1, c2);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c2, c1);
    }

    @Test @SuppressWarnings("unchecked")
    public void testBroadcastSequence() throws Exception {
        insertShuffle(c1, c2);
        
        // use concurrent senders to send messages to the group
        for(Sender sender: senders)
            sender.start();

        for(Sender sender: senders)
            sender.join(20000);

        final List<String> l1=r1.getMsgs();
        final List<String> l2=r2.getMsgs();
        
        System.out.println("-- verifying messages on A and B");
        verifyNumberOfMessages(EXPECTED_MSGS, l1, l2);
        verifySameOrder(EXPECTED_MSGS, l1, l2);
    }

    private static void insertShuffle(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=new SHUFFLE();
            shuffle.setDown(false);
            shuffle.setUp(true);
            shuffle.setMaxSize(10);
            shuffle.setMaxTime(1000);
            ch.getProtocolStack().insertProtocol(shuffle, ProtocolStack.BELOW, SEQUENCER.class);
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
    }

    private static class Sender extends Thread {
        final int num_msgs;
        final JChannel[] channels;

        public Sender(int num_msgs, JChannel ... channels) {
            this.num_msgs=num_msgs;
            this.channels=channels;
        }

        public void run() {
            for(int i=1; i <= num_msgs; i++) {
                try {
                    JChannel ch=(JChannel)Util.pickRandomElement(channels);
                    String channel_name=ch.getName();
                    ch.send(null, null, channel_name + ":" + i);
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

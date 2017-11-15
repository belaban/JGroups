

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.SHUFFLE;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests ordering with SEQUENCER: NUM_THREADS send messages concurrently over 3 channels, each thread sending NUM_MSGS
 * messages. At the end of the test, we assert that NUM_THREADS * NUM_MSGS messages have been received and that every
 * receiver delivered all messages in the same order.
 * @author Bela Ban
 */
@Test(groups=Global.STACK_INDEPENDENT,singleThreaded=true)
public class SequencerOrderTest {
    private JChannel              a, b, c;
    private MyReceiver            r1, r2, r3;
    static final String           GROUP="SequencerOrderTest";
    static final int              NUM_MSGS=50; // messages per thread
    static final int              NUM_THREADS=10;
    static final int              EXPECTED_MSGS=NUM_MSGS * NUM_THREADS;
    static final String           props="sequencer.xml";
    private final Sender[]        senders=new Sender[NUM_THREADS];
    protected final AtomicInteger num=new AtomicInteger(0);


    @BeforeMethod
    void setUp() throws Exception {
        a=new JChannel(props).name("A");
        a.connect(GROUP);
        r1=new MyReceiver("A");
        a.setReceiver(r1);

        b=new JChannel(props).name("B");
        b.connect(GROUP);
        r2=new MyReceiver("B");
        b.setReceiver(r2);

        c=new JChannel(props).name("C");
        c.connect(GROUP);
        r3=new MyReceiver("C");
        c.setReceiver(r3);

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);

        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(NUM_MSGS, num,a,b,c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        removeSHUFFLE(c,b,a);
        Util.close(c,b,a);
    }

    @Test @SuppressWarnings("unchecked")
    public void testBroadcastSequence() throws Exception {
        insertShuffle(a,b,c);
        
        // use concurrent senders to send messages to the group
        System.out.println("Starting " + senders.length + " sender threads (each sends " + NUM_MSGS + " messages)");
        for(Sender sender: senders)
            sender.start();

        for(Sender sender: senders)
            sender.join(60000);
        System.out.println("Ok, senders have completed");

        for(int i=0; i < 10; i++) {
            if(r1.size() == EXPECTED_MSGS && r2.size() == EXPECTED_MSGS && r3.size() == EXPECTED_MSGS)
                break;
            Util.sleep(1000);
        }

        final List<String> l1=r1.getMsgs();
        final List<String> l2=r2.getMsgs();
        final List<String> l3=r3.getMsgs();
        
        System.out.println("-- verifying messages on A, B and C");
        verifyNumberOfMessages(EXPECTED_MSGS, l1, l2, l3);
        verifySameOrder(EXPECTED_MSGS, l1, l2, l3);
    }

    protected static void insertShuffle(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=new SHUFFLE();
            shuffle.setDown(false);
            shuffle.setUp(true);
            shuffle.setMaxSize(10);
            shuffle.setMaxTime(1000);
            ch.getProtocolStack().insertProtocol(shuffle, ProtocolStack.Position.BELOW, NAKACK2.class);
            shuffle.init(); // starts the timer
        }
    }

    protected static void removeSHUFFLE(JChannel ... channels) {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=ch.getProtocolStack().removeProtocol(SHUFFLE.class);
            if(shuffle != null)
                shuffle.destroy();
        }
    }

    @SafeVarargs
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
            assert lists[i].size() == num_msgs : "list #" + (i+1) + " should have " + num_msgs +
              " elements, but has " + lists[i].size() + " elements";
        System.out.println("OK, all lists have the same size (" + num_msgs + ")\n");
    }



    @SafeVarargs
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
        final int           num_msgs;
        final JChannel[]    channels;
        final AtomicInteger num;

        public Sender(int num_msgs, AtomicInteger num, JChannel ... channels) {
            this.num_msgs=num_msgs;
            this.num=num;
            this.channels=channels;
        }

        public void run() {
            for(int i=1; i <= num_msgs; i++) {
                try {
                    JChannel ch=Util.pickRandomElement(channels);
                    String channel_name=ch.getName();
                    int number=num.incrementAndGet();
                    ch.send(null, channel_name + number);
                }
                catch(Exception e) {
                }
            }
        }
    }

    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final List<String> msgs=new LinkedList<>();

        private MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getMsgs() {
            return msgs;
        }

        public int size() {return msgs.size();}

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

package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.protocols.MPING;
import org.jgroups.protocols.SHUFFLE;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;


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
        String mcast_addr=ResourceManager.getNextMulticastAddress();
        a=create(props, "A", mcast_addr).connect(GROUP);
        a.setReceiver(r1=new MyReceiver("A"));

        b=create(props, "B", mcast_addr).connect(GROUP);
        b.setReceiver(r2=new MyReceiver("B"));

        c=create(props, "C", mcast_addr).connect(GROUP);
        c.setReceiver(r3=new MyReceiver("C"));

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);

        for(int i=0; i < senders.length; i++)
            senders[i]=new Sender(NUM_MSGS, num,a,b,c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        removeSHUFFLE(c,b,a);
        Util.close(c,b,a);
    }

    @Test
    public void testBroadcastSequence() throws Exception {
        insertShuffle(a,b,c);
        
        // use concurrent senders to send messages to the group
        System.out.println("Starting " + senders.length + " sender threads (each sends " + NUM_MSGS + " messages)");
        for(Sender sender: senders)
            sender.start();

        stopShuffling(2000);
        Util.waitUntilTrue(10_000, 100,
                           () -> Stream.of(r1, r2, r3).allMatch(r -> r.size() == EXPECTED_MSGS));

        final List<String> l1=r1.getMsgs();
        final List<String> l2=r2.getMsgs();
        final List<String> l3=r3.getMsgs();
        
        System.out.println("-- verifying messages on A, B and C");
        verifyNumberOfMessages(EXPECTED_MSGS, l1, l2, l3);
        verifySameOrder(EXPECTED_MSGS, l1, l2, l3);
    }

    protected void stopShuffling(long time_to_wait_before_stopping) {
        for(JChannel ch: List.of(a,b,c)) {
            final SHUFFLE shuffle=ch.getProtocolStack().findProtocol(SHUFFLE.class);
            CompletableFuture.runAsync(() -> {
                Util.sleep(time_to_wait_before_stopping);
                System.out.printf("-- stopping shuffling in %s\n", ch.getAddress());
                shuffle.flush(true); // stops and disables shuffling
            });
        }
    }

    protected static JChannel create(String props, String name, String mcast_addr) throws Exception {
        JChannel ch=new JChannel(props).name(name);
        TP tp=ch.getProtocolStack().getTransport();
        if(tp instanceof UDP)
            ((UDP)tp).setMulticastAddress(InetAddress.getByName(mcast_addr));
        MPING mping=ch.getProtocolStack().findProtocol(MPING.class);
        if(mping != null)
            mping.setMulticastAddress(mcast_addr);
        return ch;
    }

    protected static void insertShuffle(JChannel... channels) throws Exception {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=new SHUFFLE().setDown(false).setUp(true).setMaxSize(10).setMaxTime(1000);
            ch.getProtocolStack().insertProtocol(shuffle, ProtocolStack.Position.BELOW, NAKACK2.class);
            shuffle.init();
            shuffle.start();
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

    private static class MyReceiver implements Receiver {
        @SuppressWarnings("unused")
        final String name;
        final List<String> msgs=new ArrayList<>();

        private MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getMsgs() {
            return msgs;
        }

        public int size() {return msgs.size();}

        public void receive(Message msg) {
            String val=msg.getObject();
            if(val != null) {
                synchronized(msgs) {
                    msgs.add(val);
                }
            }
        }
    }

   


}

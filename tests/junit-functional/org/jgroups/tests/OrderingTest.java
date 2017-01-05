package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Tests multicast and unicast ordering of <em>regular</em> messages.<br/>
 * Regular messages from different senders can be delivered in parallel; messages from the same sender must be
 * delivered in send order.<br/>
 * There is no relation between multicast and unicast messages from a sender P; they are unrelated and therefore
 * delivered in parallel as well.
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class OrderingTest {
    protected static final int NUM_MSGS=100000;
    protected static final int PRINT=NUM_MSGS / 5;
    protected static final int NUM_SENDERS=2;

    protected JChannel[] channels=new JChannel[NUM_SENDERS];


    @BeforeMethod void init() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=createChannel(i).connect("OrderingTest.testFIFOOrder");
            channels[i].setReceiver(new MyReceiver(channels[i].name()));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        for(JChannel ch: channels) {
            SHUFFLE shuffle=new SHUFFLE();
            ch.getProtocolStack().insertProtocol(shuffle, ProtocolStack.Position.ABOVE, Discovery.class);
            shuffle.init();
        }
    }

    @AfterMethod void destroy() {
        Stream.of(channels).forEach(ch -> ch.getProtocolStack().removeProtocol(SHUFFLE.class));
        Util.close(channels);
    }


    protected static JChannel createChannel(int index) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            // new SHUFFLE(), // reorders messages and message batches
                            new NAKACK2().setValue("use_mcast_xmit", false).setValue("discard_delivered_msgs", true),
                            new UNICAST3(),
                            new STABLE().setValue("max_bytes", 50000).setValue("desired_avg_gossip", 1000),
                            new GMS().setValue("print_local_addr", false),
                            new UFC().setValue("max_credits", 2000000),
                            new MFC().setValue("max_credits", 2000000),
                            new FRAG2())
          .name(String.valueOf((char)('A' +index)));
    }


    public void testMulticastFIFOOrdering() throws Exception {
        System.out.println("\n-- sending " + NUM_MSGS + " messages");
        final CountDownLatch latch=new CountDownLatch(1);
        MySender[] senders=new MySender[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new MySender(channels[i], null, latch);
            senders[i].start();
        }
        latch.countDown();
        for(MySender sender: senders)
            sender.join();

        System.out.println("-- senders done");

        checkOrder(NUM_MSGS * NUM_SENDERS);
    }

    public void testUnicastFIFOOrdering() throws Exception {
        System.out.printf("\n-- sending %d unicast messages\n", NUM_MSGS);
        final CountDownLatch latch=new CountDownLatch(1);
        MySender[] senders=new MySender[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            Address dest=channels[(i+1) % channels.length].getAddress();
            senders[i]=new MySender(channels[i], dest, latch);
            System.out.printf("-- %s sends to %s\n", channels[i].getAddress(), dest);
            senders[i].start();
        }
        latch.countDown();
        for(MySender sender: senders)
            sender.join();

        System.out.println("-- senders done");

        checkOrder(NUM_MSGS);
    }

    protected void checkOrder(int expected_msgs) {
        System.out.println("\n-- waiting for message reception by all receivers:");
        for(int i=0; i < 50; i++) {
            boolean done=true;
            for(JChannel ch: channels) {
                MyReceiver receiver=(MyReceiver)ch.getReceiver();
                int received=receiver.getReceived();
                if(received != expected_msgs) {
                    done=false;
                    break;
                }
            }
            if(done)
                break;
            Util.sleep(1000);
        }

        Stream.of(channels).forEach(ch -> System.out.printf("%s: %d\n", ch.getAddress(),
                                                            ((MyReceiver)ch.getReceiver()).getReceived()));
        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            assert receiver.getReceived() == expected_msgs :
              String.format("%s had %d messages (expected=%d)", receiver.name, receiver.getReceived(), expected_msgs);
        }

        System.out.println("\n-- checking message order");
        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            System.out.print(ch.getAddress() + ": ");
            boolean ok=receiver.getNumberOfErrors() == 0;
            System.out.println(ok? "OK" : "FAIL (" + receiver.getNumberOfErrors() + " errors)");
            assert ok : receiver.getNumberOfErrors() + " errors";
        }
    }



    protected static class MySender extends Thread {
        protected final JChannel       ch;
        protected final Address        dest;
        protected final CountDownLatch latch;

        public MySender(JChannel ch, Address dest, CountDownLatch latch) {
            this.ch=ch;
            this.dest=dest;
            this.latch=latch;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            for(int i=1; i <= NUM_MSGS; i++) {
                try {
                    Message msg=new Message(dest, i);
                    ch.send(msg);
                    if(i % PRINT == 0)
                        System.out.println(ch.getAddress() + ": " + i + " sent");
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected final ConcurrentMap<Address,Integer> map=new ConcurrentHashMap<>();
        final AtomicInteger    received=new AtomicInteger(0);
        protected int          num_errors;
        protected final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public int getNumberOfErrors() {
            return num_errors;
        }

        public int getReceived() {
            return received.intValue();
        }

        public void receive(Message msg) {
            Integer num=msg.getObject();
            Address sender=msg.getSrc();

            Integer current_seqno=map.get(sender);
            if(current_seqno == null) {
                current_seqno=1;
                Integer tmp=map.putIfAbsent(sender, current_seqno);
                if(tmp != null)
                    current_seqno=tmp;
            }

            if(current_seqno.intValue() == num)
                map.put(sender, current_seqno + 1);
            else
                num_errors++;

            if(received.incrementAndGet() % PRINT == 0)
                System.out.printf("%s: received %d\n", name, received.get());
        }
    }


}

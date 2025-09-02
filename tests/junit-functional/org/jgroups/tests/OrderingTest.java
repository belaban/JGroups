package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Tests multicast and unicast ordering of <em>regular</em> messages.<p>
 * Regular messages from different senders can be delivered in parallel; messages from the same sender must be
 * delivered in send order.<p>
 * There is no relation between multicast and unicast messages from a sender P; they are unrelated and therefore
 * delivered in parallel as well.
 * @author Bela Ban
 */
@Test(groups=Global.TIME_SENSITIVE,singleThreaded=true)
public class OrderingTest {
    protected static final int NUM_MSGS=50_000;
    protected static final int PRINT=NUM_MSGS / 5;
    protected static final int NUM_SENDERS=2;
    protected JChannel[]       channels=new JChannel[NUM_SENDERS];

    @BeforeMethod protected void init() throws Exception {
        for(int i=0; i < channels.length; i++) {
            channels[i]=createChannel(i).connect("OrderingTest.testFIFOOrder");
            channels[i].setReceiver(new MyReceiver(channels[i].name()));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        for(JChannel ch: channels) {
            SHUFFLE shuffle=ch.getProtocolStack().findProtocol(SHUFFLE.class);
            shuffle.setUp(true);
        }
    }

    @AfterMethod protected void destroy() {
        Stream.of(channels).forEach(ch -> {
            SHUFFLE shuffle=ch.getProtocolStack().findProtocol(SHUFFLE.class);
            shuffle.setDown(false).setUp(false);
        });
        Util.close(channels);
    }

    protected static JChannel createChannel(int index) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new SHARED_LOOPBACK_PING(),
                                 new SHUFFLE().setUp(false).setDown(false).setMaxSize(100), // reorders messages
                                 new NAKACK4().capacity(32000),
                                 new UNICAST4().capacity(32000).setConnExpiryTimeout(0),
                                 new GMS().setJoinTimeout(500).printLocalAddress(false),
                                 new FRAG2().setFragSize(40_000))
          .name(String.valueOf((char)('A' + index)));
        ch.stack().getTransport().enableDiagnostics();
        return ch;
    }


    // @Test(invocationCount=100)
    public void testMulticastFIFOOrdering() throws Exception {
        System.out.println("\n-- sending " + NUM_MSGS + " messages");
        final CountDownLatch latch=new CountDownLatch(1);
        Thread[] senders=new Thread[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Thread(new MySender(channels[i], null, latch), "sender-" + i);
            senders[i].start();
        }
        latch.countDown();
        long start=System.nanoTime();
        for(Thread sender: senders)
            sender.join();
        System.out.println("-- senders done");
        checkOrder(NUM_MSGS * NUM_SENDERS);
        long time=System.nanoTime()-start;
        System.out.printf("-- took %s to send and receive %,d msgs\n", Util.printTime(time, NANOSECONDS), NUM_MSGS*NUM_SENDERS);
    }

    // @Test(invocationCount=10)
    public void testUnicastFIFOOrdering() throws Exception {
        System.out.printf("\n-- sending %d unicast messages\n", NUM_MSGS);
        final CountDownLatch latch=new CountDownLatch(1);
        Thread[] senders=new Thread[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            Address dest=channels[(i+1) % channels.length].getAddress();
            senders[i]=new Thread(new MySender(channels[i], dest, latch), "sender=" + i);
            System.out.printf("-- %s sends to %s\n", channels[i].getAddress(), dest);
            senders[i].start();
        }
        latch.countDown();
        long start=System.nanoTime();
        for(Thread sender: senders)
            sender.join();

        System.out.println("-- senders done");
        checkOrder(NUM_MSGS);
        long time=System.nanoTime()-start;
        System.out.printf("-- took %s to send, reshuffle and receive %,d msgs\n", Util.printTime(time, NANOSECONDS), NUM_MSGS);
    }

    protected void checkOrder(int expected_msgs) {
        for(JChannel ch: channels) {
            SHUFFLE shuffle=ch.getProtocolStack().findProtocol(SHUFFLE.class);
            if(shuffle != null)
                shuffle.flush(true);  // disables shuffling
        }

        System.out.println("\n-- waiting for message reception by all receivers:");
        Util.waitUntilTrue(10000, 500,
                           () -> Stream.of(channels).map(JChannel::getReceiver)
                             .allMatch(r -> ((MyReceiver)r).getReceived() == expected_msgs));

        Stream.of(channels).forEach(ch -> System.out.printf("%s: %d\n", ch.getAddress(),
                                                            ((MyReceiver)ch.getReceiver()).getReceived()));
        for(JChannel ch: channels) {
            MyReceiver r=(MyReceiver)ch.getReceiver();
            assert r.getReceived() == expected_msgs :
              String.format("%s received %d messages (expected=%d)", r.name, r.getReceived(), expected_msgs);
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



    protected static class MySender implements Runnable {
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
                    Message msg=new ObjectMessage(dest, i);
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

    protected static class MyReceiver implements Receiver {
        protected final ConcurrentMap<Address,Integer> map=new ConcurrentHashMap<>();
        protected int   received, num_errors;
        protected final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public synchronized int getNumberOfErrors() {return num_errors;}
        public synchronized int getReceived()       {return received;}

        public synchronized void receive(Message msg) {
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

            if(++received % PRINT == 0)
                System.out.printf("%s: received %d\n", name, received);
        }

    }



}

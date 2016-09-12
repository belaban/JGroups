
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the reliable FIFO (NAKACK{2}) protocol
 * <p/>
 * Two sender peers send 1000 messages to the group, where each message contains
 * a long value, mirroring seqnos used. A receiver peer receives the messages
 * from each sender and checks that seqnos are received in the correct order.
 * <p/>
 * An object all_msgs_recd is used to allow the main test thread to discover when
 * all sent messages have been received.
 * <p/>
 * The test case passes if the expected number of messages is received, and messages
 * are received in order from each sender. This implies that:
 * (i) all messages from each peer were received (reliable) and
 * (ii) all messages from each peer are received in order (FIFO)
 * @author Richard Achmatowicz
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, singleThreaded=true)
public class NakackTest {
    final static int NUM_PEERS=3;
    final static int NUM_SENDERS=NUM_PEERS-1;
    final static int NUM_MSGS=1000;
    final static int MSGS_PER_STATUS_LINE=500;
    final static int TOT_MSGS_FOR_ALL_RECEIVERS=NUM_SENDERS * NUM_MSGS * NUM_PEERS;

    // convey assertion failure from thread to main framework
    static boolean notFIFO=false;

    JChannel[] channels=new JChannel[NUM_PEERS];
    Thread[]   threads=new Thread[NUM_PEERS];

    //define senders and receivers
    boolean[] isSender={false,true,true};

    protected final AtomicInteger received_msgs=new AtomicInteger(0);



    @BeforeMethod
    protected void setUp() throws Exception {
        for(int i=0; i < NUM_PEERS; i++) {
            channels[i]=createChannel().name(Character.toString((char)(i + 'A')));
            channels[i].connect("NakackTest");
        }

        org.jgroups.Receiver[] receivers=new org.jgroups.Receiver[NUM_PEERS];

        // set up the sender and the receiver callbacks, according to whether the peer is a sender or a receiver
        for(int i=0; i < NUM_PEERS; i++) {
            receivers[i]=new Receiver(channels[i]);
            channels[i].setReceiver(receivers[i]);
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channels);
    }

    /**
     * Test to see thyat NAKACK delivery is reliable and FIFO.
     */
    public void testReceptionOfAllMessages() throws TimeoutException {


        // start the NAKACK peers and let them exchange messages
        for(int i=0; i < NUM_PEERS; i++) {
            threads[i]=new Sender(channels[i], isSender[i]);
            threads[i].start();
        }

        // wait for the threads to terminate
        try {
            for(int i=0; i < NUM_PEERS; i++)
                threads[i].join();
        }
        catch(InterruptedException e) {
        }

        // wait for the receiver peer to signal that it has received messages, or timeout
        for(int i=0; i < 20; i++) {
            if(received_msgs.get() >= TOT_MSGS_FOR_ALL_RECEIVERS)
                break;
            Util.sleep(500);
        }


        // the test fails if:
        // - a seqno is received out of order (not FIFO), or
        // - not all messages are received in time allotted (allMsgsReceived)
        Assert.assertTrue(received_msgs.get() == TOT_MSGS_FOR_ALL_RECEIVERS, "Incorrect number of messages received by the receiver thread");
        Assert.assertFalse(notFIFO, "Sequenece numbers for a peer not in correct order");
    }

    protected static JChannel createChannel() throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new MERGE3().setValue("min_interval", 1000).setValue("max_interval", 3000),
          new NAKACK2().setValue("use_mcast_xmit", false),
          new UNICAST3(),
          new STABLE().setValue("max_bytes", 50000),
          new GMS().setValue("print_local_addr", false),
          new UFC(),
          new MFC(),
          new FRAG2()
        };
        return new JChannel(protocols);
    }

   

    /**
     * This method should do the following:
     * - receive messages from senders
     * - check that sequence numbers for each sender are in order (with no gaps)
     * - terminate when correct number of messages have been received
     */
    protected class Receiver extends ReceiverAdapter {
        final JChannel               channel;
        ConcurrentMap<Address, Long> senders=new ConcurrentHashMap<>();

        public Receiver(JChannel channel) {
            this.channel=channel;
        }

        /**
         * Receive() is concurrent for different senders, but sequential per sender
         * @param msg
         */
        public void receive(Message msg) {
            // keep track of seqno ordering of messages received
            Address sender=msg.getSrc();
            received_msgs.incrementAndGet();

            // get the expected next seqno for this sender
            Long num=senders.get(sender);
            if(num == null) {
                num=(long)1;
                senders.putIfAbsent(sender, num);
            }
            long last_seqno=num;

            try {
                num=(Long)msg.getObject();
                long received_seqno=num;

                // 1. check if sequence numbers are in sequence
                if(received_seqno == last_seqno) // correct - update with next expected seqno
                    senders.put(sender,last_seqno + 1);
                else {
                    // error, terminate test
                    notFIFO=true;
                    Assert.fail("FAIL: received msg #" + received_seqno + ", expected " + last_seqno);
                }

                Address address=channel.getAddress();
                if(received_seqno % MSGS_PER_STATUS_LINE == 0 && received_seqno > 0)
                    System.out.println("<" + address + ">:" + "PASS: received msg #" + received_seqno + " from " + sender);
            }
            catch(Exception ex) {
                System.err.println(ex.toString());
            }
        }
    }


    static class Sender extends Thread {
        JChannel ch=null;
        boolean  sender=false;

        public Sender(JChannel ch, boolean sender) {
            this.ch=ch;
            this.sender=sender;
        }

        public void run() {

            // senders send NUM_MSGS messages to all peers, beginning with seqno 1
            if(sender) {
                Address address=ch.getAddress();
                for(int i=1; i <= NUM_MSGS; i++) {
                    try {
                        Message msg=new Message(null, (long)i).src(address);
                        ch.send(msg);
                        if(i % MSGS_PER_STATUS_LINE == 0) // status indicator
                            System.out.println("<" + address + ">:" + " ==> " + i);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

}

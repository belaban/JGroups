
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tests the reliable FIFO (NAKACK) protocol
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
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class NakackTest {
    final static int NUM_PEERS=3;
    final static int NUM_MSGS=1000;
    final static int WAIT_TIMEOUT=10; // secs
    final static int MSGS_PER_STATUS_LINE=100;

    // convey assertion failure from thread to main framework
    static boolean notFIFO=false;
    static boolean allMsgsReceived=false;

    JChannel[] channels=new JChannel[NUM_PEERS];
    Thread[] threads=new Thread[NUM_PEERS];

    //define senders and receivers
    boolean[] isSender={false,true,true};

    // used to wait for signal that all messages received
    static final Object all_msgs_recd=new Object();





    /**
     * Set up a number of simulator instances wrapping NAKACK
     */
    @BeforeMethod
    public void setUp() throws Exception {

        // create new simulator instances
        for(int i=0; i < NUM_PEERS; i++) {
            channels[i]=createChannel();
            channels[i].setName(Character.toString((char)(i + 'A')));
            channels[i].connect("NakackTest");
        }

        // set up the receiver callbacks for each simulator
        Receiver[] receivers=new Receiver[NUM_PEERS];

        // set up the sender and the receiver callbacks, according to whether the peer is a sender or a receiver
        for(int i=0; i < NUM_PEERS; i++) {
            if(isSender[i])
                receivers[i]=new ReceiverPeer(channels[i]);
            else
                receivers[i]=new ReceiverPeer(channels[i]);
            channels[i].setReceiver(receivers[i]);
        }
    }

    @AfterMethod
    public void tearDown() throws Exception {
        for(int i=0; i < NUM_PEERS; i++)
            channels[i].close();
    }

    /**
     * Test to see thyat NAKACK delivery is reliable and FIFO.
     */
    public void testReceptionOfAllMessages() throws TimeoutException {
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, channels);

        // start the NAKACK peers and let them exchange messages
        for(int i=0; i < NUM_PEERS; i++) {
            threads[i]=new MyNAKACKPeer(channels[i], isSender[i]);
            threads[i].start();
        }

        // wait for the receiver peer to signal that it has received messages, or timeout
        synchronized(all_msgs_recd) {
            try {
                all_msgs_recd.wait(WAIT_TIMEOUT * 1000);
            }
            catch(InterruptedException e) {
                System.out.println("main thread interrupted");
            }
        }

        // wait for the threads to terminate
        try {
            for(int i=0; i < NUM_PEERS; i++)
                threads[i].join();
        }
        catch(InterruptedException e) {
        }

        // the test fails if:
        // - a seqno is received out of order (not FIFO), or
        // - not all messages are received in time allotted (allMsgsReceived)
        Assert.assertTrue(allMsgsReceived, "Incorrect number of messages received by the receiver thread");
        Assert.assertFalse(notFIFO, "Sequenece numbers for a peer not in correct order");
    }

    protected static JChannel createChannel() throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK())
          .addProtocol(new PING().setValue("timeout", 2000).setValue("num_initial_members", 3))
          .addProtocol(new MERGE2().setValue("min_interval", 1000).setValue("max_interval", 3000))
          .addProtocol(new NAKACK().setValue("use_mcast_xmit", false))
          .addProtocol(new UNICAST2())
          .addProtocol(new STABLE().setValue("max_bytes", 50000))
          .addProtocol(new GMS().setValue("print_local_addr", false))
          .addProtocol(new UFC())
          .addProtocol(new MFC())
          .addProtocol(new FRAG2());
        stack.init();
        return ch;
    }

   

    /**
     * This method should do the following:
     * - receive messages from senders
     * - check that sequence numbers for each sender are in order (with no gaps)
     * - terminate when correct number of messages have been received
     */
    static class ReceiverPeer extends ReceiverAdapter {
        final JChannel channel;
        int num_mgs_received=0;
        ConcurrentMap<Address, Long> senders=new ConcurrentHashMap<Address, Long>();

        public ReceiverPeer(JChannel channel) {
            this.channel=channel;
        }

        /**
         * Receive() is concurrent for different senders, but sequential per sender
         * @param msg
         */
        public void receive(Message msg) {
            // keep track of seqno ordering of messages received
            Address sender=msg.getSrc();

            // get the expected next seqno for this sender
            Long num=senders.get(sender);
            if(num == null) {
                num=new Long(1);
                senders.putIfAbsent(sender, num);
            }
            long last_seqno=num.longValue();

            try {
                num=(Long)msg.getObject();
                long received_seqno=num.longValue();
                num_mgs_received++;

                // 1. check if sequence numbers are in sequence
                if(received_seqno == last_seqno) { // correct - update with next expected seqno
                    senders.put(sender, new Long(last_seqno + 1));
                }
                else {
                    // error, terminate test
                    notFIFO=true;
                    Assert.fail("FAIL: received msg #" + received_seqno + ", expected " + last_seqno);
                }

                Address address=channel.getAddress();
                if(received_seqno % MSGS_PER_STATUS_LINE == 0 && received_seqno > 0)
                    System.out.println("<" + address + ">:" + "PASS: received msg #" + received_seqno + " from " + sender);


                // condition to terminate the test - all messages received (whether in correct order or not)
                if(num_mgs_received >= NakackTest.NUM_MSGS * (NUM_PEERS - 1)) {
                    // signal that all messages have been received - this will allow the receiver
                    // thread to terminate normally
                    synchronized(all_msgs_recd) {
                        // indicate that we have received the required number of messages
                        // to differentiate between timeout and notifyAll cases on monitor
                        allMsgsReceived=true;
                        all_msgs_recd.notifyAll();
                    }
                }
            }
            catch(Exception ex) {
                System.err.println(ex.toString());
            }
        }


        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }


    static class MyNAKACKPeer extends Thread {
        JChannel ch=null;
        boolean sender=false;

        public MyNAKACKPeer(JChannel ch, boolean sender) {
            this.ch=ch;
            this.sender=sender;
        }

        public void run() {

            // senders send NUM_MSGS messages to all peers, beginning with seqno 1
            if(sender) {
                Address address=ch.getAddress();
                for(int i=1; i <= NUM_MSGS; i++) {
                    try {
                        Message msg=new Message(null, address, new Long(i));
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

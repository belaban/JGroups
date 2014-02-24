
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
@Test(groups=Global.FUNCTIONAL, singleThreaded=true)
public class NakackTest {
    final static int NUM_PEERS=3;
    final static int NUM_MSGS=1000;
    final static int WAIT_TIMEOUT=10; // secs
    final static int MSGS_PER_STATUS_LINE=NUM_MSGS / 2;

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
    void setUp() throws Exception {
        for(int i=0; i < NUM_PEERS; i++) {
            channels[i]=createChannel(Character.toString((char)(i + 'A')));
            channels[i].connect("NakackTest");
        }

        // set up the receiver callbacks for each simulator
        Receiver[] receivers=new Receiver[NUM_PEERS];

        // set up the sender and the receiver callbacks, according to whether the peer is a sender or a receiver
        for(int i=0; i < NUM_PEERS; i++) {
            receivers[i]=new ReceiverPeer(channels[i].getAddress());
            channels[i].setReceiver(receivers[i]);
        }
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, channels);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channels);
    }

    /**
     * Test to see thyat NAKACK delivery is reliable and FIFO.
     */
    // @Test(invocationCount=50)
    public void testReceptionOfAllMessages() throws TimeoutException {
        // start the NAKACK peers and let them exchange messages
        for(int i=0; i < NUM_PEERS; i++) {
            if(isSender[i]) {
                threads[i]=new MyNAKACKPeer(channels[i]);
                threads[i].start();
            }
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

        // the test fails if:
        // - a seqno is received out of order (not FIFO), or
        // - not all messages are received in time allotted (allMsgsReceived)
        Assert.assertTrue(allMsgsReceived, "Incorrect number of messages received by the receiver thread");
        Assert.assertFalse(notFIFO, "Sequenece numbers for a peer not in correct order");
    }

    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(new Protocol[] {
          new SHARED_LOOPBACK(),
          new PING().setValue("timeout", 2000).setValue("num_initial_members", 3),
          new MERGE2().setValue("min_interval", 1000).setValue("max_interval", 3000),
          new NAKACK2().setValue("use_mcast_xmit", false),
          new UNICAST2(),
          new STABLE().setValue("max_bytes", 50000),
          new GMS().setValue("print_local_addr", false),
          new UFC(),
          new MFC(),
          new FRAG2()
        }).name(name);
    }

   

    /**
     * This method should do the following:
     * - receive messages from senders
     * - check that sequence numbers for each sender are in order (with no gaps)
     * - terminate when correct number of messages have been received
     */
    static class ReceiverPeer extends ReceiverAdapter {
        protected int                                num_mgs_received;
        protected final Address                      address;
        protected final ConcurrentMap<Address, Long> senders=new ConcurrentHashMap<Address, Long>();

        public ReceiverPeer(Address address) {
            this.address=address;
        }

        /**
         * Receive() is concurrent for different senders, but sequential per sender
         */
        public void receive(Message msg) {
            // keep track of seqno ordering of messages received
            Address sender=msg.getSrc();

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
                num_mgs_received++;

                // 1. check if sequence numbers are in sequence
                if(received_seqno == last_seqno) // correct - update with next expected seqno
                    senders.put(sender,last_seqno + 1);
                else {
                    // error, terminate test
                    notFIFO=true;
                    Assert.fail("FAIL: received msg #" + received_seqno + ", expected " + last_seqno);
                }

                if(received_seqno % MSGS_PER_STATUS_LINE == 0 && received_seqno > 0)
                    System.out.println(address + ": received msg #" + received_seqno + " from " + sender);


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
                System.err.println(ex);
            }
        }


        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }


    protected static class MyNAKACKPeer extends Thread {
        protected JChannel ch;

        public MyNAKACKPeer(JChannel ch) {
            this.ch=ch;
        }

        public void run() {
            // senders send NUM_MSGS messages to all peers, beginning with seqno 1
            for(long i=1; i <= NUM_MSGS; i++) {
                try {
                    ch.send(new Message(null,i));
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}

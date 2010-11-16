
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Digest;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Arrays;

/**
 * Tests the reliable FIFO (NAKACK) protocol
 * <p/>
 * Two sender peers send 1000 messages to the group, where each message contains
 * a long value, mirroring seqnos used. A receiver peer receives the messages
 * from each sender and checks that seqnos are received in the correct order.
 * <p/>
 * This test makes use of Simulator to test the protocol in
 * isolation from a JChannel. Each peer is wrapped in a Simulator instance and
 * the instances are linked together to form the group.
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

    Address[] addresses=new Address[NUM_PEERS];
    Vector<Address> members=null;
    View view;
    Simulator[] simulators=new Simulator[NUM_PEERS];
    NAKACK[] layers=new NAKACK[NUM_PEERS];
    Protocol[][] stacks=new Protocol[NUM_PEERS][];
    Thread[] threads=new Thread[NUM_PEERS];

    //define senders and receivers
    boolean[] isSender=new boolean[NUM_PEERS];

    // used to wait for signal that all messages received
    static final Object all_msgs_recd=new Object();

    /**
     * Set up a number of simulator instances wrapping NAKACK
     */
    @BeforeMethod
    public void setUp() throws Exception {

        // define the senders and the receivers
        isSender[0]=false;
        isSender[1]=true;
        isSender[2]=true;

        // dummy IP addresses and ports
        for(int i=0; i < addresses.length; i++) {
            UUID uuid=UUID.randomUUID();
            UUID.add(uuid, "node-" + i);
            addresses[i]=uuid;
        }

        // dummy set of members which works for all three simulators
        members=new Vector<Address>();
        members.addAll(Arrays.asList(addresses).subList(0, NUM_PEERS));

        // create a dummy View(creator, timestamp, member set)
        view=new View(addresses[0], 1, members);

        // create new simulator instances
        for(int i=0; i < NUM_PEERS; i++) {

            // create the simulator instance

            // at this stage, the ProtocolAdapter should be created and the timer present
            simulators[i]=new Simulator();
            simulators[i].setLocalAddress(addresses[i]);
            simulators[i].setView(view);

            // set up the protocol under test
            layers[i]=new NAKACK();

            // set up its properties
            layers[i].setUseMcastXmit(true);

            // our protocol stack under test consists of one protocol
            stacks[i]=new Protocol[]{layers[i]};

            // initalise the protocol stack
            simulators[i].setProtocolStack(stacks[i]);
        }

        // describe the configuration of the three simulators
        for(int i=0; i < NUM_PEERS; i++) {
            for(int j=0; j < NUM_PEERS; j++) {
                if(i == j)
                    simulators[i].addMember(addresses[j]);
                else
                    simulators[i].addMember(addresses[j], simulators[j]);
            }
        }

        // set up the receiver callbacks for each simulator
        Simulator.Receiver[] receivers=new Simulator.Receiver[NUM_PEERS];

        // set up the sender and the receiver callbacks, according to whether
        // the peer is a sender or a receiver
        for(int i=0; i < NUM_PEERS; i++) {

            if(isSender[i])
                receivers[i]=new SenderPeer(simulators[i]);
            else
                receivers[i]=new ReceiverPeer(simulators[i]);

            simulators[i].setReceiver(receivers[i]);
        }

        // start the simulators
        for(int i=0; i < NUM_PEERS; i++)
            simulators[i].start();

        MutableDigest digest=new MutableDigest(NUM_PEERS);
        for(Address addr: addresses)
            digest.add(new Digest(addr, 0, 0));
        for(int i=0; i < NUM_PEERS; i++) {
            layers[i].down(new Event(Event.SET_DIGEST, digest));
        }

    }

    @AfterMethod
    public void tearDown() throws Exception {

        // stop the simulators
        for(int i=0; i < NUM_PEERS; i++)
            simulators[i].stop();
    }

    /**
     * Test to see thyat NAKACK delivery is reliable and FIFO.
     */
    public void testReceptionOfAllMessages() {


        // start the NAKACK peers and let them exchange messages
        for(int i=0; i < NUM_PEERS; i++) {

            threads[i]=new MyNAKACKPeer(simulators[i], isSender[i]);
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
            for(int i=0; i < NUM_PEERS; i++) {
                threads[i].join();
            }
        }
        catch(InterruptedException e) {
        }

        // the test fails if:
        // - a seqno is received out of order (not FIFO), or
        // - not all messages are received in time allotted (allMsgsReceived)
        Assert.assertTrue(allMsgsReceived, "Incorrect number of messages received by the receiver thread");
        Assert.assertFalse(notFIFO, "Sequenece numbers for a peer not in correct order");
    }

    /**
     * This is called by the Simulator when a message comes back up the stack.
     * Used by message senders to simply display messages received from other peers.
     */
    static class SenderPeer implements Simulator.Receiver {
        Simulator simulator=null;
        int num_mgs_received=0;

        SenderPeer(Simulator s) {
            this.simulator=s;
        }

        // keep track of how many messages were received
        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                num_mgs_received++;
                if(num_mgs_received % MSGS_PER_STATUS_LINE == 0)
                    System.out.println("<" + simulator.getLocalAddress() + ">:" + "<== " + num_mgs_received);
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }

    /**
     * This is called by the Simulator when a message comes back up the stack.
     * This method should do the following:
     * - receive messages from senders
     * - check that sequence numbers for each sender are in order (with no gaps)
     * - terminate when correct number of messages have been received
     */
    static class ReceiverPeer implements Simulator.Receiver {
        Simulator simulator=null;
        int num_mgs_received=0;
        long starting_seqno=1;
        long last_seqno=starting_seqno;

        Hashtable<Address, Long> senders=new Hashtable<Address, Long>();
        Message msg;
        Address sender;
        Long s;
        long received_seqno;

        ReceiverPeer(Simulator s) {
            this.simulator=s;
        }

        public synchronized void receive(Event evt) {

            if(evt.getType() == Event.MSG) {

                // keep track of seqno ordering of messages received
                msg=(Message)evt.getArg();
                sender=msg.getSrc();

                // get the expected next seqno for this sender
                s=senders.get(sender);
                if(s == null) {
                    s=new Long(starting_seqno);
                    senders.put(sender, s);
                }
                last_seqno=s.longValue();

                try {
                    s=(Long)msg.getObject();
                    received_seqno=s.longValue();

                    num_mgs_received++;

                    // 1. check if sequence numbers are in sequence
                    if(received_seqno == last_seqno) {
                        // correct - update with next expected seqno
                        senders.put(sender, new Long(last_seqno + 1));
                    }
                    else {
                        // error, terminate test
                        notFIFO=true;
                        Assert.fail("FAIL: received msg #" + received_seqno + ", expected " + last_seqno);
                    }

                    Address address=simulator.getLocalAddress();

                    if(received_seqno % MSGS_PER_STATUS_LINE == 0 && received_seqno > 0)
                        System.out.println("<" + address + ">:" + "PASS: received msg #" + received_seqno + " from " + sender);


                    // condition to terminate the test - all messages received (whether in
                    // correct order or not)
                    if(num_mgs_received >= NakackTest.NUM_MSGS * (NUM_PEERS - 1)) {

                        // indicate that we have received the required number of messages
                        // to differentiate between timeout and notifyAll cases on monitor
                        allMsgsReceived=true;

                        // signal that all messages have been received - this will allow the receiver
                        // thread to terminate normally
                        synchronized(all_msgs_recd) {
                            all_msgs_recd.notifyAll();
                        }
                    }
                }
                catch(Exception ex) {
                    System.out.println(ex.toString());
                    // log.error("NakackTest.CheckNoGaps.up()", ex);
                }
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }


    static class MyNAKACKPeer extends Thread {

        Simulator s=null;
        boolean sender=false;

        public MyNAKACKPeer(Simulator s, boolean sender) {
            this.s=s;
            this.sender=sender;
        }

        public void run() {

            // senders send NUM_MSGS messages to all peers, beginning with seqno 1
            if(sender) {

                Address address=s.getLocalAddress();

                // send a collection of dummy messages by mcast to the stack under test
                for(int i=1; i <= NUM_MSGS; i++) {

                    Message msg=new Message(null, address, new Long(i));
                    Event evt=new Event(Event.MSG, msg);

                    // call Simulator.send() to introduce the event into the stack under test
                    s.send(evt);

                    // status indicator
                    if(i % MSGS_PER_STATUS_LINE == 0)
                        System.out.println("<" + address + ">:" + " ==> " + i);
                }
            }

            if(!sender) {
                // wait for the receiver callback to signal that it has received messages, or timeout
                // this just causes this thread to block until its receiver has finished
                synchronized(all_msgs_recd) {
                    try {
                        all_msgs_recd.wait(WAIT_TIMEOUT * 1000);
                    }
                    catch(InterruptedException e) {
                        System.out.println("main thread interrupted");
                    }
                }
            }
        }
    }

}

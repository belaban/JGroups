package org.jgroups.tests.byteman;


import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;


/**
 * Tests SEQUENCER with message sending while the coordinator crashes
 * (see individual tests for more detailed descriptions).
 * @author Bela Ban
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class SequencerFailoverTest extends BMNGRunner {
    JChannel a, b, c; // A is the coordinator
    static final String GROUP="SequencerFailoverTest";
    static final int    NUM_MSGS=50;
    static final String props="sequencer.xml";


    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel(props, "A", GROUP);
        b=createChannel(props, "B", GROUP);
        c=createChannel(props, "C", GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b, c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c, b, a);
    }

    /**
     * Tests that the ordering of messages is correct after a coordinator fails half-way through the sending of N msgs.
     * We have {A,B,C} initially, and B sending messages (forwarding to A). Then A crashes, and B becomes the coordinator.
     * Now B needs to broadcast the messages in the forward-table and then broadcast messages after that.
     */
    public void testBroadcastSequenceSenderIsB() throws Exception {
        _testBroadcastSequence(b);
    }

    /**
     * Tests that the ordering of messages is correct after a coordinator fails half-way through the sending of N msgs.
     * We have {A,B,C} initially, and C sending messages (forwarding to A). Then A crashes, and C now needs to forward
     * the messages to B.
     */
    public void testBroadcastSequenceSenderIsC() throws Exception {
        _testBroadcastSequence(c);
    }


    /**
     * Tests that resending of messages in the forward-queue on a view change and sending of new messages at the
     * same time doesn't lead to incorrect ordering (forward-queue messages need to be delivered before new msgs).
     * https://issues.jboss.org/browse/JGRP-1449
     */
    @BMScript(dir="scripts/SequencerFailoverTest", value="testResendingVersusNewMessages")
    public void testResendingVersusNewMessages() throws Exception {
        MyReceiver rb=new MyReceiver("B"), rc=new MyReceiver("C");
        b.setReceiver(rb); c.setReceiver(rc);

        final int expected_msgs=5;

        Util.sleep(500);
        // Now kill A (the coordinator)
        System.out.print("-- killing A: ");
        Util.shutdown(a);
        System.out.println("done");
        injectSuspectEvent(a.getAddress(), b, c);
        a=null;

        // Now send message 1 (it'll end up in the forward-queue)
        System.out.println("-- sending message 1");
        Message msg=new Message(null, 1);
        c.send(msg);

        // Now wait for the view change, the sending of new messages 2-5 and the resending of 1, and make sure
        // 1 is delivered before 2-5
        List<Integer> list_b=rb.getList(), list_c=rc.getList();
        for(int i=0; i < 10; i++) {
            if(list_b.size() == expected_msgs && list_c.size() == expected_msgs)
                break;
            Util.sleep(1000);
        }
        System.out.println("\nB: " + list_b + "\nC: " + list_c);

        assert list_b.size() == expected_msgs : "expected " + expected_msgs + " msgs, but got " + list_b.size() + ": " +list_b;
        assert list_c.size() == expected_msgs : "expected " + expected_msgs + " msgs, but got " + list_c.size() + ": " +list_c;
        System.out.println("OK: both B and C have the expected number of messages (" + expected_msgs + ")");

        assert list_b.equals(list_c);
        System.out.println("OK: B and C's messages are in the same order");

        int seqno=1;
        for(int i=0; i < expected_msgs; i++) {
            Integer seqno_b=list_b.get(i);
            assert seqno_b == seqno : "expected " + seqno + " , but got " + seqno_b + " (B)";
            Integer seqno_c=list_c.get(i);
            assert seqno_c == seqno : "expected " + seqno + " , but got " + seqno_c + " (C)";
            seqno++;
        }
        System.out.println("OK: B and C's messages are in the correct order");
    }


    /**
     * Tests the following scenario:
     * - The cluster is {A,B,C}
     * - Failure detection and merging protocols have been removed
     * - A is disabled (DISCARD drops all messages) and shut down
     * - We set threshold in SEQUENCER to 0 (permanent ack-mode)
     * - On C, 5 threads are sending messages 1 and 2 each (prefixed with the thread name)
     * - The first thread is looping trying to send its message 1. The threads are blocked trying to acquire send-lock
     * - We inject view {B,C} into B and C
     * - The flush on C waits until threads 2-4 on C have acquired send-lock, added their message 1 to forward-table
     *   and dropped out of the for loop
     * - The threads are now all blocked trying to send message 2
     * - The flush phase resends messages 1 from all 5 threads in the forward-queue, then unblocks the threads
     * - The 5 threads now send message 2
     * - We need to assert that both B and C delivered the same messages in the same order, and that all 1 messages
     *   were delivered before the 2 messages
     */
    public void testFailoverWithMultipleThreadsSendingMessages() throws Exception {
        adjustConfiguration(a,b,c); // removes FD/FD_ALL and MERGE protocols

        final int num_senders=5;

        MyReceiver rb=new MyReceiver("B"), rc=new MyReceiver("C");
        b.setReceiver(rb); c.setReceiver(rc);

        // insert DISCARD into A
        DISCARD discard=new DISCARD();
        discard.setLocalAddress(a.getAddress());
        discard.setDiscardAll(true);
        ProtocolStack stack=a.getProtocolStack();
        TP transport=stack.getTransport();
        stack.insertProtocol(discard,  ProtocolStack.Position.ABOVE, transport.getClass());
        
        MySender[] senders=new MySender[num_senders];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new MySender((i+1) * 10, c);
            senders[i].start();
        }
        Util.sleep(1000);

        System.out.println("Injecting SUSPECT(A) into B and C");
        injectSuspectEvent(a.getAddress(), b,c);

        for(int i=0; i < 20; i++) {
            if(b.getView().size() == 2 && c.getView().size() == 2)
                break;
            Util.sleep(500);
        }
        System.out.println("B: " + b.getView() +"\nC: " + c.getView());
        assert b.getView().size() == 2 && c.getView().size() == 2;

        for(MySender sender: senders)
            sender.join(30000);
        System.out.println("senders are done");

        List<Integer> list_b=rb.getList(), list_c=rc.getList();
        System.out.println("\nB: " + list_b + "\nC: " + list_c);

        assert list_b.size() == num_senders * 2 && list_c.size() == num_senders * 2;
        System.out.println("OK: both B and C have the expected number of messages (" + num_senders *2 + ")");
        assert list_b.equals(list_c);
        System.out.println("OK: both list have the same ordering");

        // check that the first messages (11, 21, 31, 41, 51) are in the first half of both lists
        List<Integer> expected_list=Arrays.asList(11, 21, 31, 41, 51);
        List<Integer> list_bb=new ArrayList<>();
        for(int i=0; i < num_senders; i++)
            list_bb.add(list_b.get(i));

        Collections.sort(list_bb);

        System.out.println("Expected first half: " + expected_list + ", received: " + list_bb);
        assert expected_list.equals(list_bb);

        List<Integer> list_cc=new ArrayList<>();
        for(int i=0; i < num_senders; i++)
            list_cc.add(list_c.get(i));

        Collections.sort(list_cc);

        System.out.println("Expected first half: " + expected_list + ", received: " + list_cc);
        assert expected_list.equals(list_cc);

        System.out.println("OK: first set of messages of all threads were delivered before second set of messages");
    }



    /**
     * Tests that the ordering of messages is correct after a coordinator fails half-way through the sending of N msgs
     */
    protected void _testBroadcastSequence(JChannel channel) throws Exception {
        MyReceiver rb=new MyReceiver("B"), rc=new MyReceiver("C");
        b.setReceiver(rb); c.setReceiver(rc);

        new Thread() {
            public void run() {
                Util.sleep(3000);
                System.out.println("** killing A");
                try {
                    Util.shutdown(a);
                }
                catch(Exception e) {
                    System.err.println("failed shutting down channel " + a.getAddress() + ", exception=" + e);
                }
                System.out.println("** A killed");
                injectSuspectEvent(a.getAddress(), b, c);
                a=null;
            }
        }.start();

        final Address sender=channel.getAddress();
        for(int i=1; i <= NUM_MSGS; i++) {
            Util.sleep(300);
            channel.send(new Message(null, i));
            System.out.print("[" + sender + "] -- messages sent: " + i + "/" + NUM_MSGS + "\r");
        }
        System.out.println("");
        View v2=b.getView();
        View v3=c.getView();
        System.out.println("B's view: " + v2 + "\nC's view: " + v3);
        assert v2.equals(v3);
        assert v2.size() == 2;
        for(int i=20000; i > 0; i-=1000) {
            if(rb.size() >= NUM_MSGS && rc.size() >= NUM_MSGS)
                break;
            Util.sleep(500);
        }
        List<Integer> list_b=rb.getList(), list_c=rc.getList();
        System.out.println("\nB: " + list_b + "\nC: " + list_c);

        assert list_b.size() == NUM_MSGS && list_c.size() == NUM_MSGS;
        System.out.println("OK: both B and C have the expected number of messages (" + NUM_MSGS + ")");

        assert list_b.equals(list_c);
        System.out.println("OK: B's and C's message are in the same order");
    }



    /** Injects SUSPECT event(suspected_mbr) into channels */
    protected static void injectSuspectEvent(Address suspected_mbr, JChannel ... channels) {
        Event evt=new Event(Event.SUSPECT, suspected_mbr);
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            if(gms != null)
                gms.up(evt);
        }
    }

    /** Removes FD, FD_ALL, MERGEX protocols, sets SEQUENCER.threshold=0 */
    protected void adjustConfiguration(JChannel ... channels) {
        for(JChannel ch: channels) {
            ch.getProtocolStack().removeProtocol(FD_ALL.class,FD.class,MERGE3.class, VERIFY_SUSPECT.class);
            SEQUENCER seq=(SEQUENCER)ch.getProtocolStack().findProtocol(SEQUENCER.class);
            seq.setThreshold(0); // permanent ack-mode
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new LinkedList<>();
        protected final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<Integer> getList() {return list;}

        public int size() {return list.size();}

        public void receive(Message msg) {
            synchronized(list) {
                list.add((Integer)msg.getObject());
            }
        }
    }

    protected static class MySender extends Thread {
        protected final int      rank;
        protected final JChannel ch;

        public MySender(int rank, JChannel ch) {
            this.rank=rank;
            this.ch=ch;
            setName("sender-" + rank);
        }

        public void run() {
            for(int i=1; i <=2; i++) {
                Message msg=new Message(null, (rank + i));
                try {
                    System.out.println("[" + rank + "]: sending msg " + (rank + i));
                    ch.send(msg);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }


    protected JChannel createChannel(final String props, final String name, final String cluster_name) throws Exception {
        JChannel retval=new JChannel(props);
        retval.setName(name);
        retval.connect(cluster_name);
        return retval;
    }
}

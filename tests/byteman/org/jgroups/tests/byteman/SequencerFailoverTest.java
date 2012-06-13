package org.jgroups.tests.byteman;


import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;


/**
 * Tests a SEQUENCER based stack: A, B and C. B starts multicasting messages with a monotonically increasing
 * number. Then A is crashed. C and B should receive *all* numbers *without* a gap.
 * @author Bela Ban
 */
@Test(groups=Global.BYTEMAN,sequential=true)
public class SequencerFailoverTest extends BMNGRunner {
    JChannel a, b, c; // A is the coordinator
    static final String GROUP="SequencerFailoverTest";
    static final int    NUM_MSGS=50;
    static final String props="sequencer.xml";


    @BeforeMethod
    void setUp() throws Exception {
        a=new JChannel(props);
        a.setName("A");
        a.connect(GROUP);

        b=new JChannel(props);
        b.setName("B");
        b.connect(GROUP);

        c=new JChannel(props);
        c.setName("C");
        c.connect(GROUP);

        Util.sleep(500);
        assert a.getView().size() == 3 : "A's view: " + a.getView();
        assert b.getView().size() == 3 : "B's view: " + b.getView();
        assert c.getView().size() == 3 : "C's view: " + c.getView();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c, b, a);
    }

    /**
     * Tests that the ordering of messages is correct after a coordinator fails half-way through the sending of N msgs.
     * We have {A,B,C} initially, and B sending messages (forwarding to A). Then A crashes, and B becomes the coordinator.
     * Now B needs to broadcast the messgages in the forward-table and then broadcast messages after that.
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
            channel.send(new Message(null, null, new Integer(i)));
            System.out.print("[" + sender + "] -- messages sent: " + i + "/" + NUM_MSGS + "\r");
        }
        System.out.println("");
        View v2=b.getView();
        View v3=c.getView();
        System.out.println("B's view: " + v2 + "\nC's view: " + v3);
        assert v2.equals(v3);
        assert v2.size() == 2;
        int s2, s3;
        for(int i=15000; i > 0; i-=1000) {
            s2=rb.size(); s3=rc.size();
            if(s2 >= NUM_MSGS && s3 >= NUM_MSGS) {
                System.out.print("B: " + s2 + " msgs, C: " + s3 + " msgs\r");
                break;
            }
            Util.sleep(1000);
            System.out.print("sleeping for " + (i/1000) + " seconds (B: " + s2 + " msgs, C: " + s3 + " msgs)\r");
        }
        System.out.println("-- verifying messages on B and C");
        List<Integer> list_b=rb.getList(), list_c=rc.getList();
        System.out.println("\nB: " + list_b + "\nC: " + list_c);

        assert list_b.size() == list_c.size();
        System.out.println("OK: both B and C have the same number of messages (" + list_b.size() + ")");

        assert list_b.size() == NUM_MSGS && list_c.size() == NUM_MSGS;
        System.out.println("OK: both B and C have the expected number (" + NUM_MSGS + ") of messages");

        System.out.println("verifying B and C have the same order");
        for(int i=0; i < list_b.size(); i++) {
            Integer el_b=list_b.get(i), el_c=list_c.get(i);
            assert el_b.equals(el_c) : "element at index=" + i + " in B (" + el_b +
              ") is different from element " + i + " in C (" + el_c + ")";
        }
        System.out.println("OK: B and C's message are in the same order");
    }



    /**
     * Tests that resending of messages in the forward-queue on a view change and sending of new messages at the
     * same time doesn't lead to incorrect ordering (forward-queue messages need to be delivered before new msgs)
     * https://issues.jboss.org/browse/JGRP-1449
     */
    @BMScript(dir="scripts/SequencerFailoverTest", value="testResendingVersusNewMessages")
    public void testResendingVersusNewMessages() throws Exception {
        MyReceiver rb=new MyReceiver("B"), rc=new MyReceiver("C");
        b.setReceiver(rb); c.setReceiver(rc);

        /*for(JChannel ch: Arrays.asList(a,b,c)) {
            SEQUENCER seq=(SEQUENCER)ch.getProtocolStack().findProtocol(SEQUENCER.class);
            seq.setValue("ack_mode", false);
        }*/

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
        System.out.println("-- verifying messages on B and C");
        List<Integer> list_b=rb.getList(), list_c=rc.getList();
        for(int i=0; i < 10; i++) {
            if(list_b.size() == expected_msgs && list_c.size() == expected_msgs)
                break;
            Util.sleep(1000);
        }
        System.out.println("\nB: " + list_b + "\nC: " + list_c);

        assert list_b.size() == expected_msgs : "expected " + expected_msgs + " msgs, but got " + list_b.size() + ": " +list_b;
        assert list_c.size() == expected_msgs : "expected " + expected_msgs + " msgs, but got " + list_c.size() + ": " +list_c;
        System.out.println("OK: both B and C have the expected number (" + expected_msgs + ") of messages");

        System.out.println("Verifying that B and C have the same order");
        for(int i=0; i < list_b.size(); i++) {
            Integer el_b=list_b.get(i), el_c=list_c.get(i);
            assert el_b.equals(el_c) : "element at index=" + i + " in B (" + el_b +
                    ") is different from element " + i + " in C (" + el_c + ")";
        }
        System.out.println("OK: B and C's messages are in the same order");

        System.out.println("Verifying that B and C have the correct order");
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




    /** Injects SUSPECT event(suspected_mbr) into channels */
    private static void injectSuspectEvent(Address suspected_mbr, JChannel ... channels) {
        Event evt=new Event(Event.SUSPECT, suspected_mbr);
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            if(gms != null)
                gms.up(evt);
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new LinkedList<Integer>();
        protected final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<Integer> getList() {return list;}

        public int size() {return list.size();}

        public void receive(Message msg) {
            Integer val=(Integer)msg.getObject();
            // System.out.println("[" + name + "] received " + val);
            synchronized(list) {
                list.add(val);
            }
        }

        protected void clear() {list.clear();}
    }


}

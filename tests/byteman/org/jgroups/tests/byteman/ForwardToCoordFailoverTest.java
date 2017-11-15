package org.jgroups.tests.byteman;


import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


/**
 * Tests FIFO ordering of {@link FORWARD_TO_COORD} with message sending while the coordinator crashes
 * @author Bela Ban
 */
@Test(groups={Global.BYTEMAN,Global.EAP_EXCLUDED},singleThreaded=true)
public class ForwardToCoordFailoverTest extends BMNGRunner {
    JChannel a, b, c; // A is the coordinator
    static final String CLUSTER="ForwardToCoordFailoverTest";


    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel("A", CLUSTER);
        b=createChannel("B", CLUSTER);
        c=createChannel("C", CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c, b, a);
    }




    /**
     * Tests that resending of messages in the forward-queue on a view change and sending of new messages at the
     * same time doesn't lead to incorrect ordering (forward-queue messages need to be delivered before new msgs).
     * <p/>
     * C sends messages while A leaves, we need to make sure that the messages sent by C are received in FIFO order by B.
     * <p/>
     * https://issues.jboss.org/browse/JGRP-1517
     */
    @BMScript(dir="scripts/ForwardToCoordFailoverTest", value="testSendingDuringViewChange")
    public void testSendingDuringViewChange() throws Exception {
        MyReceiver rb=new MyReceiver("B");
        b.setReceiver(rb);

        final int EXPECTED_MSGS=10;

        // Now kill A (the coordinator)
        System.out.print("-- killing A: ");
        Util.shutdown(a);
        System.out.println("done");
        a=null;

        // Now send message 1-5 (they'll end up in the forward-queue of FORWARD_TO_COORD)
        System.out.println("-- sending message 1-5");
        for(int i=1; i <= 5; i++) {
            Event evt=new Event(Event.FORWARD_TO_COORD, new BytesMessage(null, i));
            c.down(evt);
        }

        // now inject view B={B,C} into B and C
        View view=View.create(b.getAddress(), 5, b.getAddress(), c.getAddress());
        System.out.println("Injecting view " + view + " into B and C");
        for(JChannel ch: Arrays.asList(c,b)) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }

        // Now wait for the view change, the sending of new messages 2-5 and the resending of 1, and make sure
        // 1 is delivered before 2-5
        List<Integer> list_b=rb.getList();
        for(int i=0; i < 10; i++) {
            if(list_b.size() == EXPECTED_MSGS)
                break;
            Util.sleep(1000);
        }
        System.out.println("\nB: " + list_b);

        assert list_b.size() == EXPECTED_MSGS : "expected " + EXPECTED_MSGS + " msgs, but got " + list_b.size() + ": " +list_b;
        System.out.println("OK: B has the expected number of messages (" + EXPECTED_MSGS + ")");

        int seqno=1;
        for(int i=0; i < EXPECTED_MSGS; i++) {
            Integer seqno_b=list_b.get(i);
            assert seqno_b == seqno : "expected " + seqno + " , but got " + seqno_b + " (B)";
            seqno++;
        }
        System.out.println("OK: B's messages are in the correct order");
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
                Message msg=new BytesMessage(null, (rank + i));
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


    protected JChannel createChannel(final String name, final String cluster_name) throws Exception {
        JChannel retval=new JChannel(new SHARED_LOOPBACK(),
                                     new SHARED_LOOPBACK_PING(),
                                     new NAKACK2(),
                                     new UNICAST3(),
                                     new GMS(),
                                     new FORWARD_TO_COORD());
        retval.setName(name);
        retval.connect(cluster_name);
        return retval;
    }
}

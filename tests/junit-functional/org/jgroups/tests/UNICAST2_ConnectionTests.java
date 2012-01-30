package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

/**
 * Tests unilateral closings of UNICAST2 connections. The test scenarios are described in doc/design.UNICAST.new.txt.
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class UNICAST2_ConnectionTests {
    private JChannel a, b;
    private Address a_addr, b_addr;
    private MyReceiver r1, r2;
    private UNICAST2 u1, u2;
    private static final String props="SHARED_LOOPBACK:UNICAST2(stable_interval=2000;conn_expiry_timeout=0)";
    private static final String CLUSTER="UNICAST2_ConnectionTests";


    @BeforeMethod
    void start() throws Exception {
        r1=new MyReceiver("A");
        r2=new MyReceiver("B");
        a=new JChannel(props);
        a.setName("A");
        a.connect(CLUSTER);
        a_addr=a.getAddress();
        a.setReceiver(r1);
        u1=(UNICAST2)a.getProtocolStack().findProtocol(UNICAST2.class);
        b=new JChannel(props);
        b.setName("B");
        b.connect(CLUSTER);
        b_addr=b.getAddress();
        b.setReceiver(r2);
        u2=(UNICAST2)b.getProtocolStack().findProtocol(UNICAST2.class);
    }


    @AfterMethod void stop() {Util.close(b, a);}


    /**
     * Tests cases #1 and #2 of UNICAST.new.txt
     * @throws Exception
     */
    public void testRegularMessageReception() throws Exception {
        sendAndCheck(a, b_addr, 100, r2);
        sendAndCheck(b, a_addr,  50, r1);
    }


    /**
     * Tests case #3 of UNICAST.new.txt
     */
    public void testBothChannelsClosing() throws Exception {
        sendToEachOtherAndCheck(10);

        // now close the connections to each other
        System.out.println("==== Closing the connections on both sides");
        u1.removeConnection(b_addr);
        u2.removeConnection(a_addr);
        r1.clear(); r2.clear();

        // causes new connection establishment
        sendToEachOtherAndCheck(10);
    }


    /**
     * Scenario #4 (A closes the connection unilaterally (B keeps it open), then reopens it and sends messages)
     */
    public void testAClosingUnilaterally() throws Exception {
        sendToEachOtherAndCheck(10);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        u1.removeConnection(b_addr);

        // then send messages from A to B
        sendAndCheck(a, b_addr, 10, r2);
    }

    /**
     * Scenario #5 (B closes the connection unilaterally (A keeps it open), then A sends messages to B)
     */
    public void testBClosingUnilaterally() throws Exception {
        sendToEachOtherAndCheck(10);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on B");
        u2.removeConnection(a_addr);

        // then send messages from A to B
        // u2.setLevel("trace");
        sendAndCheck(a, b_addr, 10, r2);
    }


    /**
     * Scenario #6 (A closes the connection unilaterally (B keeps it open), then reopens it and sends messages,
     * but loses the first message
     */
    public void testAClosingUnilaterallyButLosingFirstMessage() throws Exception {
        sendToEachOtherAndCheck(10);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        u1.removeConnection(b_addr);

        // add a Drop protocol to drop the first unicast message
        Drop drop=new Drop(true);
        a.getProtocolStack().insertProtocol(drop, ProtocolStack.BELOW, UNICAST2.class);


        u1.setLevel("trace");
        u2.setLevel("trace");

        // then send messages from A to B
        System.out.println("===================== sending 5 messages from A to B");
        sendAndCheck(a, b_addr, 5, r2);
        System.out.println("===================== sending 5 messages from A to B: done");


    }


    /** Tests concurrent reception of multiple messages with a different conn_id (https://issues.jboss.org/browse/JGRP-1347) */
    public void testMultipleConcurrentResets() throws Exception {
        sendAndCheck(a, b_addr, 1, r2);

        // now close connection on A unilaterally
        System.out.println("==== Closing the connection on A");
        u1.removeConnection(b_addr);

        r2.clear();

        final UNICAST2 unicast=(UNICAST2)b.getProtocolStack().findProtocol(UNICAST2.class);

        int NUM=10;

        final List<Message> msgs=new ArrayList<Message>(NUM);

        for(int i=1; i <= NUM; i++) {
            Message msg=new Message(b_addr, a_addr, "m" + i);
            UNICAST2.Unicast2Header hdr=UNICAST2.Unicast2Header.createDataHeader(1, (short)2, true);
            msg.putHeader(unicast.getId(), hdr);
            msgs.add(msg);
        }


        Thread[] threads=new Thread[NUM];
        final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
        for(int i=0; i < NUM; i++) {
            final int index=i;
            threads[i]=new Thread() {
                public void run() {
                    try {
                        barrier.await();
                        unicast.up(new Event(Event.MSG, msgs.get(index)));
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            threads[i].start();
        }

        barrier.await();
        for(Thread thread: threads)
            thread.join();

        List<Message> list=r2.getMessages();
        System.out.println("list = " + print(list));

        assert list.size() == 1 : "list must have 1 element but has " + list.size() + ": " + print(list);
    }


    /**
     * Send num unicasts on both channels and verify the other end received them
     * @param num
     * @throws Exception
     */
    private void sendToEachOtherAndCheck(int num) throws Exception {
        for(int i=1; i <= num; i++) {
            a.send(b_addr, "m" + i);
            b.send(a_addr, "m" + i);
        }
        List<Message> l1=r1.getMessages();
        List<Message> l2=r2.getMessages();
        for(int i=0; i < 10; i++) {
            if(l1.size()  == num && l2.size() == num)
                break;
            Util.sleep(500);
        }
        System.out.println("l1 = " + print(l1));
        System.out.println("l2 = " + print(l2));
        assert l1.size() == num;
        assert l2.size() == num;
    }

    private void sendAndCheck(JChannel channel, Address dest, int num, MyReceiver receiver) throws Exception {
        receiver.clear();
        for(int i=1; i <= num; i++) {
//            if(i == 2) {
//                u1.sendStableMessages();
//                u2.sendStableMessages();
//            }
            channel.send(dest, "m" + i);
        }
        List<Message> list=receiver.getMessages();
        for(int i=0; i < 1000; i++) {
            if(list.size() == num)
                break;
            Util.sleep(500);
        }
        u1.setLevel("warn");
        u2.setLevel("warn");
        System.out.println("list = " + print(list));
        int size=list.size();
        assert size == num : "list has " + size + " elements";
    }


    private static String print(List<Message> list) {
        List<String> tmp=new ArrayList<String>(list.size());
        for(Message msg: list)
            tmp.add((String)msg.getObject());
        return Util.printListWithDelimiter(tmp, " ");
    }


    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final List<Message> msgs=new ArrayList<Message>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            msgs.add(msg);
        }

        public List<Message> getMessages() { return msgs; }
        public void clear() {msgs.clear();}
        public int size() {return msgs.size();}

        public String toString() {
            return name;
        }
    }

    private static class Drop extends Protocol {
        private volatile boolean drop_next=false;

        private Drop(boolean drop_next) {
            this.drop_next=drop_next;
        }

        public String getName() {
            return "Drop";
        }

        public void dropNext() {
            drop_next=true;
        }

        public Object down(Event evt) {
            if(drop_next && evt.getType() == Event.MSG) {
                drop_next=false;
                return null;
            }
            return super.down(evt);
        }
    }
}
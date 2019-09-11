package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests the UNICAST3 protocol for OOB msgs, tests http://jira.jboss.com/jira/browse/JGRP-377 and
 * https://issues.jboss.org/browse/JGRP-2327
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_OOB_Test {
    protected JChannel a, b;

    protected static final long XMIT_INTERVAL=500;

    @BeforeMethod
    void setup() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        a.connect("UNICAST_OOB_Test");
        b.connect("UNICAST_OOB_Test");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(b,a);
    }


    public void testRegularMessages() throws Exception {
        sendMessages(false);
    }

    public void testOutOfBandMessages() throws Exception {
        sendMessages(true);
    }


    /**
     Tests the case where B sends B1 and B2, but A receives B2 first (discards it) and requests retransmission of B1.
     JIRA: https://issues.jboss.org/browse/JGRP-2327
     */
    public void testSecondMessageReceivedFirstRegular() throws Exception {
        _testSecondMessageReceivedFirst(false, false);
    }

    public void testSecondMessageReceivedFirstRegularBatched() throws Exception {
           _testSecondMessageReceivedFirst(false, true);
    }

    public void testSecondMessageReceivedFirstOOB() throws Exception {
        _testSecondMessageReceivedFirst(true, false);
    }

    public void testSecondMessageReceivedFirstOOBBatched() throws Exception {
        _testSecondMessageReceivedFirst(true, true);
    }

    protected void _testSecondMessageReceivedFirst(boolean oob, boolean use_batches) throws Exception {
        Address dest=a.getAddress(), src=b.getAddress();
        UNICAST3 u_a=a.getProtocolStack().findProtocol(UNICAST3.class), u_b=b.getProtocolStack().findProtocol(UNICAST3.class);
        u_a.removeReceiveConnection(src);
        u_a.removeSendConnection(src);
        u_b.removeReceiveConnection(dest);
        u_b.removeSendConnection(dest);
        System.out.println("=============== removed connection between A and B ===========");

        REVERSE reverse=new REVERSE().numMessagesToReverse(5)
          .filter(msg -> msg.getDest() != null && src.equals(msg.getSrc()) && (msg.getFlags() == 0 || msg.isFlagSet(Message.Flag.OOB)));
        a.getProtocolStack().insertProtocol(reverse, ProtocolStack.Position.BELOW, UNICAST3.class);

        if(use_batches) {
            MAKE_BATCH mb=new MAKE_BATCH().localAddress(dest).unicasts(true);
            a.getProtocolStack().insertProtocol(mb, ProtocolStack.Position.BELOW, UNICAST3.class);
            mb.start();
        }

        MyReceiver r=new MyReceiver();
        a.setReceiver(r);

        System.out.println("========== B sends messages 1-5 to A ==========");
        long start=System.currentTimeMillis();
        for(int i=1; i <= 5; i++) {
            Message msg=new Message(dest, (long)i);
            if(oob) msg.setFlag(Message.Flag.OOB);
            b.send(msg);
        }
        Util.waitUntil(10000, 10, () -> r.size() == 5);
        long time=System.currentTimeMillis() - start;
        System.out.printf("===== list: %s (in %d ms)\n", r.getSeqnos(), time);
        assert time < XMIT_INTERVAL *2;
    }

    /**
     * Check that 4 is received before 3
     */
    private void sendMessages(boolean oob) throws Exception {
        DISCARD_PAYLOAD discard=new DISCARD_PAYLOAD();
        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);

        // the first channel will discard the unicast messages with seqno #3 two times, the let them pass down
        ProtocolStack stack=a.getProtocolStack();
        Protocol neighbor=stack.findProtocol(Util.getUnicastProtocols());
        System.out.println("Found unicast protocol " + neighbor.getClass().getSimpleName());
        stack.insertProtocolInStack(discard,neighbor,ProtocolStack.Position.BELOW);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        Address dest=b.getAddress();
        for(int i=1; i <=5; i++) {
            Message msg=new Message(dest,(long)i);
            if(i == 4 && oob)
                msg.setFlag(Message.Flag.OOB);
            System.out.println("-- sending message #" + i);
            a.send(msg);
            Util.sleep(100);
        }

        // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well
        long target_time=System.currentTimeMillis() + 30000;
        do {
            if(receiver.size() >= 5)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());


        List<Long> seqnos=receiver.getSeqnos();
        System.out.println("-- sequence numbers: " + seqnos);
        assert seqnos.size() == 5;

        if(!oob) {
            for(int i=0; i < 5; i++)
                assert seqnos.get(i) == i+1 : " seqno is " + seqnos.get(i) + ", but expected " + i+1;
        }
        else {
            // 4 needs to be received before 3. Reason: 4 is sent OOB,  does *not* wait until 3 has been retransmitted !
            int index_3=-1, index_4=-1;
            for(int i=0; i < 5; i++) {
                if(seqnos.get(i) == 3)
                    index_3=i;
                if(seqnos.get(i) == 4)
                    index_4=i;
            }
            assert index_4 < index_3 : "4 must come before 3 in list " + seqnos;
        }
    }


    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new UNICAST3().setXmitInterval(XMIT_INTERVAL),
          new GMS())
          .name(name);
    }



    public static class MyReceiver extends ReceiverAdapter {
        /** List<Long> of unicast sequence numbers */
        List<Long> seqnos=Collections.synchronizedList(new LinkedList<>());

        public MyReceiver() {
        }

        public List<Long> getSeqnos() {
            return seqnos;
        }

        public void receive(Message msg) {
            if(msg != null) {
                Long num=msg.getObject();
                System.out.println(">> received " + num);
                seqnos.add(num);
            }
        }

        public int size() {return seqnos.size();}
    }

  
}

package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests the UNICAST3 protocol for OOB msgs, tests https://issues.redhat.com/browse/JGRP-377 and
 * https://issues.redhat.com/browse/JGRP-2327
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="createUnicast")
public class UNICAST_OOB_Test {
    protected JChannel a, b;
    protected static final long XMIT_INTERVAL=500;

    @DataProvider
    static Object[][] createUnicast() {
        return new Object[][]{
          {UNICAST3.class},
          {UNICAST4.class}
        };
    }

    protected void setup(Class<? extends Protocol> unicast_class) throws Exception {
        a=createChannel("A", unicast_class);
        b=createChannel("B", unicast_class);
        a.connect("UNICAST_OOB_Test");
        b.connect("UNICAST_OOB_Test");
        Util.waitUntilAllChannelsHaveSameView(3000, 100, a,b);
        System.out.printf("-- cluster formed: %s\n", b.view());
    }

    @AfterMethod
    protected void tearDown() throws Exception {Util.close(b,a);}


    public void testRegularMessages(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        sendMessages(false);
    }

    public void testOutOfBandMessages(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        sendMessages(true);
    }

    /**
     Tests the case where B sends B1 and B2, but A receives B2 first (discards it) and requests retransmission of B1.
     JIRA: https://issues.redhat.com/browse/JGRP-2327
     */
    public void testSecondMessageReceivedFirstRegular(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        _testSecondMessageReceivedFirst(false, false);
    }

    public void testSecondMessageReceivedFirstRegularBatched(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        _testSecondMessageReceivedFirst(false, true);
    }

    public void testSecondMessageReceivedFirstOOB(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        _testSecondMessageReceivedFirst(true, false);
    }

    // @Test(invocationCount=100)
    public void testSecondMessageReceivedFirstOOBBatched(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        _testSecondMessageReceivedFirst(true, true);
    }

    protected void _testSecondMessageReceivedFirst(boolean oob, boolean use_batches) throws Exception {
        Address dest=a.getAddress(), src=b.getAddress();
        Protocol u_a=a.getProtocolStack().findProtocol(Util.getUnicastProtocols()),
          u_b=b.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        for(int i=0; i < 10; i++) {
            Util.invoke(u_a, "removeReceiveConnection", src);
            Util.invoke(u_a, "removeSendConnection", src);
            Util.invoke(u_b, "removeReceiveConnection", dest);
            Util.invoke(u_b, "removeSendConnection", dest);
            int num_connections=(int)Util.invoke(u_a, "getNumConnections") +
              (int)Util.invoke(u_b, "getNumConnections");
            if(num_connections == 0)
                break;
            Util.sleep(100);
        }
        System.out.println("=============== removed connections between A and B ===========");

        Protocol reverse=new REVERSE().numMessagesToReverse(5)
          .filter(msg -> msg.getDest() != null && src.equals(msg.src()) && (msg.getFlags(false) == 0 || msg.isFlagSet(Message.Flag.OOB)));
        // REVERSE2 reverse=new REVERSE2().filter(m -> m.dest() != null && m.isFlagSet(Message.Flag.OOB) && src.equals(m.src()));
        a.getProtocolStack().insertProtocol(reverse, ProtocolStack.Position.BELOW, UNICAST3.class,UNICAST4.class);

        if(use_batches) {
            MAKE_BATCH mb=new MAKE_BATCH().unicasts(true).setAddress(dest);
            a.getProtocolStack().insertProtocol(mb, ProtocolStack.Position.BELOW, UNICAST3.class,UNICAST4.class);
            mb.start();
        }

        MyReceiver<Long> r=new MyReceiver<Long>().name(a.getName()).verbose(true);
        a.setReceiver(r);

        System.out.printf("========== B sending %s messages 1-5 to A ==========\n", oob? "OOB" : "regular");
        //u_a.setLevel("trace"); u_b.setLevel("trace");

        long start=System.currentTimeMillis();
        for(int i=1; i <= 5; i++) {
            Message msg=new ObjectMessage(dest, (long)i);
            if(oob) msg.setFlag(Message.Flag.OOB);
            b.send(msg);
            System.out.printf("-- %s: sent %s, hdrs: %s\n", b.address(), msg, msg.printHeaders());
        }
        if(reverse instanceof REVERSE2) {
            REVERSE2 rr=((REVERSE2)reverse);
            Util.waitUntilTrue(2000, 100, () -> rr.size() == 5);
            rr.filter(null); // from now on, all msgs are passed up
            rr.deliver();
        }

        Util.waitUntil(5000, 100, () -> r.size() == 5,
                       () -> String.format("expected 5 messages but got %s", r.list()));
        long time=System.currentTimeMillis() - start;
        System.out.printf("===== list: %s (in %d ms)\n", r.list(), time);
        long expected_time=XMIT_INTERVAL * 10; // increased because times might increase with the increase in parallel tests
        assert time < expected_time : String.format("expected a time < %d ms, but got %d ms", expected_time, time);
    }

    /** Check that 4 is received before 3 */
    private void sendMessages(boolean oob) throws Exception {
        DISCARD_PAYLOAD discard=new DISCARD_PAYLOAD();
        MyReceiver<Long> receiver=new MyReceiver<>();
        b.setReceiver(receiver);

        // the first channel will discard the unicast messages with seqno #3 two times, the let them pass down
        ProtocolStack stack=a.getProtocolStack();
        Protocol neighbor=stack.findProtocol(Util.getUnicastProtocols());
        System.out.println("Found unicast protocol " + neighbor.getClass().getSimpleName());
        stack.insertProtocolInStack(discard,neighbor,ProtocolStack.Position.BELOW);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        Address dest=b.getAddress();
        for(int i=1; i <=5; i++) {
            Message msg=new ObjectMessage(dest, (long)i);
            if(i == 4 && oob)
                msg.setFlag(Message.Flag.OOB);
            System.out.println("-- sending message #" + i);
            a.send(msg);
        }

        // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well
        Util.waitUntilTrue(3000, 500, () -> receiver.size() >= 5);

        List<Long> seqnos=receiver.list();
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

    protected static JChannel createChannel(String name, Class<? extends Protocol> unicast_class) throws Exception {
        Protocol p=unicast_class.getConstructor().newInstance();
        Util.invoke(p, "setXmitInterval", XMIT_INTERVAL);
        return new JChannel(
          new SHARED_LOOPBACK(), // .bundler("nb"),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          p,
          new GMS().printLocalAddress(false).setViewAckCollectionTimeout(100))
          .name(name);
    }



}

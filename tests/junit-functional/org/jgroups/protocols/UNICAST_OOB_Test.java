package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests the UNICAST protocol for OOB msgs, tests http://jira.jboss.com/jira/browse/JGRP-377
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_OOB_Test {
    JChannel a, b;


    void setUp(Class<? extends Protocol> unicast_class) throws Exception {
        a=createChannel(unicast_class, "A");
        b=createChannel(unicast_class, "B");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(b,a);
    }

    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {UNICAST.class},
          {UNICAST2.class},
          {UNICAST3.class}
        };
    }

    @Test(dataProvider="configProvider")
    public void testRegularMessages(Class<? extends Protocol> unicast_class) throws Exception {
        setUp(unicast_class);
        sendMessages(false);
    }

    @Test(dataProvider="configProvider")
    public void testOutOfBandMessages(Class<? extends Protocol> unicast_class) throws Exception {
        setUp(unicast_class);
        sendMessages(true);
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
        stack.insertProtocolInStack(discard,neighbor,ProtocolStack.BELOW);

        a.connect("UNICAST_OOB_Test");
        b.connect("UNICAST_OOB_Test");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        Address dest=b.getAddress();
        for(int i=1; i <=5; i++) {
            Message msg=new Message(dest, null,(long)i);
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
        System.out.println("sequence numbers: " + seqnos);
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


    protected JChannel createChannel(Class<? extends Protocol> unicast_class, String name) throws Exception {
        Protocol unicast=unicast_class.newInstance().setValue("xmit_interval",500);
        if(unicast instanceof UNICAST2)
            unicast.setValue("stable_interval", 1000);
        return new JChannel(
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new NAKACK2(),
          new DISCARD(),
          unicast,
          new GMS())
          .name(name);
    }



    public static class MyReceiver extends ReceiverAdapter {
        /** List<Long> of unicast sequence numbers */
        List<Long> seqnos=Collections.synchronizedList(new LinkedList<Long>());

        public MyReceiver() {
        }

        public List<Long> getSeqnos() {
            return seqnos;
        }

        public void receive(Message msg) {
            if(msg != null) {
                Long num=(Long)msg.getObject();
                System.out.println(">> received " + num);
                seqnos.add(num);
            }
        }

        public int size() {return seqnos.size();}
    }

  
}

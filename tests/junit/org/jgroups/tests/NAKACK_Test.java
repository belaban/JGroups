package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.DISCARD_PAYLOAD;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests the NAKACK protocol for OOB and regular msgs, tests http://jira.jboss.com/jira/browse/JGRP-379
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class NAKACK_Test extends ChannelTestBase {
    JChannel c1, c2, c3;


    @BeforeMethod
    void setUp() throws Exception {
        c1=createChannel(true, 3);
        c2=createChannel(c1);
        c3=createChannel(c1);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c3, c2, c1);
    }


    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-379: we send 1, 2, 3, 4(OOB) and 5 to the cluster.
     * Message with seqno 3 is discarded two times, so retransmission will make the receivers receive it *after* 4.
     * Note that OOB messages *destroys* FIFO ordering (or whatever ordering properties are set) !
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testOutOfBandMessages() throws Exception {
        NAKACK_Test.MyReceiver receiver1=new NAKACK_Test.MyReceiver();
        NAKACK_Test.MyReceiver receiver2=new NAKACK_Test.MyReceiver();
        NAKACK_Test.MyReceiver receiver3=new NAKACK_Test.MyReceiver();
        c1.setReceiver(receiver1);
        c2.setReceiver(receiver2);
        c3.setReceiver(receiver3);

        c1.getProtocolStack().insertProtocol(new DISCARD_PAYLOAD(),ProtocolStack.BELOW,NAKACK.class,NAKACK2.class);

        c1.connect("NAKACK_OOB_Test");
        c2.connect("NAKACK_OOB_Test");
        c3.connect("NAKACK_OOB_Test");

        assert c3.getView().getMembers().size() == 3 : "view is " + c3.getView() + ", expected view of 3 members";

        for(int i=1; i <=5; i++) {
            Message msg=new Message(null, null,(long)i);
            if(i == 4)
                msg.setFlag(Message.Flag.OOB);
            System.out.println("-- sending message #" + i);
            c1.send(msg);
            Util.sleep(100);
        }

        Collection<Long> seqnos1=receiver1.getSeqnos();
        Collection<Long> seqnos2=receiver2.getSeqnos();
        Collection<Long> seqnos3=receiver3.getSeqnos();

        // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well
        long target_time=System.currentTimeMillis() + 20000;
        do {
            if(seqnos1.size() >= 5 && seqnos2.size() >= 5 && seqnos3.size() >= 5)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());

        System.out.println("sequence numbers:");
        System.out.println("c1: " + seqnos1);
        System.out.println("c2: " + seqnos2);
        System.out.println("c3: " + seqnos3);
        checkOrder(seqnos1, seqnos2, seqnos3);
    }

    /**
     * Checks whether the numbers are in order *after* removing 4: the latter is OOB and can therefore appear anywhere
     * in the sequence
     * @param lists
     */
    private static void checkOrder(Collection<Long> ... lists) throws Exception {
        for(Collection<Long> list: lists) {
            list.remove(4L);
            long prev_val=0;
            for(long val: list) {
                if(val <= prev_val)
                    throw new Exception("elements are not ordered in list: " + list);
                prev_val=val;
            }
        }
    }



    public static class MyReceiver extends ReceiverAdapter {
        /** List<Long> of unicast sequence numbers */
        Collection<Long> seqnos=new ConcurrentLinkedQueue<Long>();

        public MyReceiver() {
        }

        public Collection<Long> getSeqnos() {
            return seqnos;
        }

        public void receive(Message msg) {
            if(msg != null) {
                Long num=(Long)msg.getObject();
                seqnos.add(num);
            }
        }

        public int size() {return seqnos.size();}
    }

}

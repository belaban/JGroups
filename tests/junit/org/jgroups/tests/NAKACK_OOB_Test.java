package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.DISCARD_PAYLOAD;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests the NAKACK protocol for OOB msgs, tests http://jira.jboss.com/jira/browse/JGRP-379
 * @author Bela Ban
 * @version $Id: NAKACK_OOB_Test.java,v 1.5 2008/04/08 07:18:57 belaban Exp $
 */
public class NAKACK_OOB_Test {
    JChannel ch1, ch2, ch3;
    final String props="udp.xml";

    public NAKACK_OOB_Test(String name) {
    }

    @BeforeMethod
    public void setUp() throws Exception {
        ;
        ch1=new JChannel(props);
        ch2=new JChannel(props);
        ch3=new JChannel(props);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if(ch1 != null)
            ch1.close();
        if(ch2 != null)
            ch2.close();
        if(ch3 != null)
            ch3.close();
        ;
    }


    /**
     * Tests http://jira.jboss.com/jira/browse/JGRP-379: we send 1, 2, 3, 4(OOB) and 5 to the cluster.
     * Message with seqno 3 is discarded two times, so retransmission will make the receivers receive it *after* 4.
     * Because 4 is marked as OOB, we will deliver 4 *immediately* (before 3 and 5), so the sequence of the messages
     * at the receivers is 1 - 2 - 4 -3 - 5.
     * Note that OOB messages *destroys* FIFO ordering (or whatever ordering properties are set) !
     * @throws Exception
     */
    @org.testng.annotations.Test
    public void testOutOfBandMessages() throws Exception {
        NAKACK_OOB_Test.MyReceiver receiver1=new NAKACK_OOB_Test.MyReceiver();
        NAKACK_OOB_Test.MyReceiver receiver2=new NAKACK_OOB_Test.MyReceiver();
        NAKACK_OOB_Test.MyReceiver receiver3=new NAKACK_OOB_Test.MyReceiver();
        ch1.setReceiver(receiver1);
        ch2.setReceiver(receiver2);
        ch3.setReceiver(receiver3);

        // all channels will discard messages with seqno #3 two times, the let them pass
        ch1.getProtocolStack().insertProtocol(new DISCARD_PAYLOAD(), ProtocolStack.BELOW, "NAKACK");
        ch2.getProtocolStack().insertProtocol(new DISCARD_PAYLOAD(), ProtocolStack.BELOW, "NAKACK");
        ch3.getProtocolStack().insertProtocol(new DISCARD_PAYLOAD(), ProtocolStack.BELOW, "NAKACK");

        ch1.connect("x");
        ch2.connect("x");
        ch3.connect("x");

        Assert.assertEquals(3, ch3.getView().getMembers().size());

        for(int i=1; i <=5; i++) {
            Message msg=new Message(null, null, new Long(i));
            if(i == 4)
                msg.setFlag(Message.OOB);
            System.out.println("-- sending message #" + i);
            ch1.send(msg);
            Util.sleep(100);
        }

        Util.sleep(5000); // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well

        List seqnos1=receiver1.getSeqnos();
        List seqnos2=receiver2.getSeqnos();
        List seqnos3=receiver3.getSeqnos();

        System.out.println("sequence numbers:");
        System.out.println("ch1: " + seqnos1);
        System.out.println("ch2: " + seqnos2);
        System.out.println("ch3: " + seqnos3);

        // expected sequence is: 1 2 4 3 5 ! Reason: 4 is sent OOB,  does *not* wait until 3 has been retransmitted !!
        Long[] expected_seqnos=new Long[]{new Long(1), new Long(2), new Long(4), new Long(3), new Long(5)};
        for(int i=0; i < expected_seqnos.length; i++) {
            Long expected_seqno=expected_seqnos[i];

            Long received_seqno=(Long)seqnos1.get(i);
            Assert.assertEquals(expected_seqno, received_seqno);
            received_seqno=(Long)seqnos2.get(i);
            Assert.assertEquals(expected_seqno, received_seqno);
            received_seqno=(Long)seqnos3.get(i);
            Assert.assertEquals(expected_seqno, received_seqno);
        }
    }


    public static class MyReceiver extends ReceiverAdapter {
        /** List<Long> of unicast sequence numbers */
        List seqnos=new LinkedList();

        public MyReceiver() {
        }

        public List getSeqnos() {
            return seqnos;
        }

        public void receive(Message msg) {
            if(msg != null) {
                Long num=(Long)msg.getObject();
                seqnos.add(num);
            }
        }
    }

    public static Test suite() {
        return new TestSuite(NAKACK_OOB_Test.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(NAKACK_OOB_Test.suite());
    }
}

package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.Address;
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
 * Tests the UNICAST protocol for OOB msgs, tests http://jira.jboss.com/jira/browse/JGRP-377
 * @author Bela Ban
 * @version $Id: UNICAST_OOB_Test.java,v 1.7 2008/04/14 08:34:46 belaban Exp $
 */
public class UNICAST_OOB_Test extends ChannelTestBase {
    JChannel ch1, ch2;


    @BeforeMethod
    void setUp() throws Exception {
        ch1=createChannel();
        ch2=createChannel();
    }

    @AfterMethod
    void tearDown() throws Exception {
        if(ch1 != null)
            ch1.close();
        if(ch2 != null)
            ch2.close();
    }


    @Test
    public void testRegularMessages() throws Exception {
        sendMessages(false);
    }

    @Test
    public void testOutOfBandMessages() throws Exception {
        sendMessages(true);
    }


    /**
     */
    private void sendMessages(boolean oob) throws Exception {
        DISCARD_PAYLOAD prot1=new DISCARD_PAYLOAD();
        MyReceiver receiver=new MyReceiver();
        ch2.setReceiver(receiver);

        // the second channel will discard the unicast messages with seqno #3 two times, the let them pass
        ch2.getProtocolStack().insertProtocol(prot1, ProtocolStack.BELOW, "UNICAST");

        ch1.connect("x");
        ch2.connect("x");
        Assert.assertEquals(2, ch2.getView().getMembers().size());

        Address dest=ch2.getLocalAddress();
        for(int i=1; i <=5; i++) {
            Message msg=new Message(dest, null, new Long(i));
            if(i == 4 && oob)
                msg.setFlag(Message.OOB);
            System.out.println("-- sending message #" + i);
            ch1.send(msg);
            Util.sleep(100);
        }

        Util.sleep(5000); // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well

        List seqnos=receiver.getSeqnos();
        System.out.println("sequence numbers: " + seqnos);

        // expected sequence is: 1 2 4 3 5 ! Reason: 4 is sent OOB,  does *not* wait until 3 has been retransmitted !!
        Long[] expected_seqnos=oob?
                new Long[]{new Long(1), new Long(2), new Long(4), new Long(3), new Long(5)} : // OOB
                new Long[]{new Long(1), new Long(2), new Long(3), new Long(4), new Long(5)};  // regular
        for(int i=0; i < expected_seqnos.length; i++) {
            Long expected_seqno=expected_seqnos[i];
            Long received_seqno=(Long)seqnos.get(i);
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

  
}

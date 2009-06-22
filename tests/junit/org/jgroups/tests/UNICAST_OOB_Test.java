package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.DISCARD_PAYLOAD;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests the UNICAST protocol for OOB msgs, tests http://jira.jboss.com/jira/browse/JGRP-377
 * @author Bela Ban
 * @version $Id: UNICAST_OOB_Test.java,v 1.11 2009/06/22 10:33:22 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class UNICAST_OOB_Test extends ChannelTestBase {
    JChannel c1, c2;


    @BeforeMethod
    void setUp() throws Exception {
        c1=createChannel(true, 2);
        c2=createChannel(c1);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c2, c1);
    }


    public void testRegularMessages() throws Exception {
        sendMessages(false);
    }

    public void testOutOfBandMessages() throws Exception {
        sendMessages(true);
    }


    /**
     * Check that 4 is received before 3
     */
    private void sendMessages(boolean oob) throws Exception {
        DISCARD_PAYLOAD prot1=new DISCARD_PAYLOAD();
        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);

        // the first channel will discard the unicast messages with seqno #3 two times, the let them pass down
        c1.getProtocolStack().insertProtocol(prot1, ProtocolStack.BELOW, UNICAST.class);

        c1.connect("UNICAST_OOB_Test");
        c2.connect("UNICAST_OOB_Test");
        assert c2.getView().size() == 2 : "ch2.view is " + c2.getView();

        Address dest=c2.getAddress();
        for(int i=1; i <=5; i++) {
            Message msg=new Message(dest, null, new Long(i));
            if(i == 4 && oob)
                msg.setFlag(Message.OOB);
            System.out.println("-- sending message #" + i);
            c1.send(msg);
            Util.sleep(100);
        }

        // wait until retransmission of seqno #3 happens, so that 4 and 5 are received as well
        long target_time=System.currentTimeMillis() + 5000;
        do {
            if(receiver.size() >= 5)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());


        List<Long> seqnos=receiver.getSeqnos();
        System.out.println("sequence numbers: " + seqnos);

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
                seqnos.add(num);
            }
        }

        public int size() {return seqnos.size();}
    }

  
}

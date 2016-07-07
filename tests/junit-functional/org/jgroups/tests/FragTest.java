
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Class to test FRAG protocol. It uses ProtocolTester to assemble a minimal stack which only consists of
 * FRAG and LOOPBACK (messages are immediately resent up the stack). Sends NUM_MSGS with MSG_SIZE size down
 * the stack, they should be received as well.
 *
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class FragTest {
    public static final long NUM_MSGS  =   1000;
    public static final int  MSG_SIZE  = 100000;
    public static final int  FRAG_SIZE =  24000;

    protected JChannel ch;


    private static Message createMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) buf[i]=(byte)'x';
        return new Message(null, buf);
    }


    @BeforeClass
    protected void setup() throws Exception {
        ch=createChannel();
        ch.connect("FragTestCluster");
    }

    @AfterClass
    protected void destroy() {
        Util.close(ch);
    }



    public void testRegularMessages() throws Exception {
        FragReceiver frag_receiver=new FragReceiver();
        ch.setReceiver(frag_receiver);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message big_msg=createMessage(MSG_SIZE);
            ch.send(big_msg);
        }
        System.out.println("-- done sending");
        for(int i=0; i < 10; i++) {
            int num_msgs=frag_receiver.getNumMsgs();
            if(num_msgs >= NUM_MSGS)
                break;
            Util.sleep(500);
        }
        assert frag_receiver.getNumMsgs() == NUM_MSGS;
    }


    public void testMessagesWithOffsets() throws Exception {
        FragReceiver frag_receiver=new FragReceiver();
        ch.setReceiver(frag_receiver);
        byte[] big_buffer=new byte[(int)(MSG_SIZE * NUM_MSGS)];
        int offset=0;

        for(int i=1; i <= NUM_MSGS; i++) {
            Message big_msg=new Message(null, big_buffer, offset, MSG_SIZE);
            ch.send(big_msg);
            offset+=MSG_SIZE;
        }

        System.out.println("-- done sending");
        for(int i=0; i < 10; i++) {
            int num_msgs=frag_receiver.getNumMsgs();
            if(num_msgs >= NUM_MSGS)
                break;
            Util.sleep(500);
        }
        assert frag_receiver.getNumMsgs() == NUM_MSGS;
    }

    /**
     * Tests potential ordering violation by sending small, unfragmented messages, followed by a large message
     * which generates 3 fragments, followed by a final small message. Verifies that the message assembled from the
     * 3 fragments is in the right place and not at the end. JIRA=https://issues.jboss.org/browse/JGRP-1648
     */
    public void testMessageOrdering() throws Exception {
        OrderingReceiver receiver=new OrderingReceiver();
        ch.setReceiver(receiver);
        Protocol frag=ch.getProtocolStack().findProtocol(FRAG2.class, FRAG.class);
        frag.setValue("frag_size", 5000);

        Message first=new Message(null, new Payload(1, 10));
        Message big=new Message(null, new Payload(2, 12000)); // frag_size is 5000, so FRAG{2} will create 3 fragments
        Message last=new Message(null, new Payload(3, 10));

        ch.send(first);
        ch.send(big);
        ch.send(last);

        List<Integer> list=receiver.getList();
        for(int i=0; i < 10; i++) {
            if(list.size() == 3)
                break;
            Util.sleep(1000);
        }
        System.out.println("list = " + list);
        assert list.size() == 3;

        // assert that the ordering is [1 2 3], *not* [1 3 2]
        for(int i=0; i < list.size(); i++) {
            assert list.get(i) == i+1 : "element at index " + i + " is " + list.get(i) + ", was supposed to be " + (i+1);
        }
    }

    protected static JChannel createChannel() throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new NAKACK2().setValue("use_mcast_xmit", false),
                            new UNICAST3(),
                            new STABLE().setValue("max_bytes", 50000),
                            new GMS().setValue("print_local_addr", false),
                            new UFC(),
                            new MFC(),
                            new FRAG2().setValue("frag_size", FRAG_SIZE));
    }


    protected static class FragReceiver extends ReceiverAdapter {
        int num_msgs=0;

        public int getNumMsgs() {
            return num_msgs;
        }

        public void receive(Message msg) {
            num_msgs++;
            if(num_msgs % 100 == 0)
                System.out.println("received " + num_msgs + " / " + NUM_MSGS);
        }
    }

    protected static class OrderingReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new ArrayList<>();

        public List<Integer> getList() {return list;}

        public void receive(Message msg) {
            Payload payload=msg.getObject();
            list.add(payload.seqno);
        }
    }

    protected static class Payload implements Serializable {
        private static final long serialVersionUID=-1989899280425578506L;
        protected int    seqno;
        protected byte[] buffer;

        protected Payload(int seqno, int size) {
            this.seqno=seqno;
            this.buffer=new byte[size];
        }
    }


}



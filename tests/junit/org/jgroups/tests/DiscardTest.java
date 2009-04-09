package org.jgroups.tests;


import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.Properties;

/**
 * Tests the NAKACK (retransmission) and STABLE (garbage collection) protocols
 * by discarding 10% of all network-bound messages
 * 
 * @author Bela Ban
 * @version $Id: DiscardTest.java,v 1.22 2009/04/09 09:11:17 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class DiscardTest extends ChannelTestBase {
    JChannel ch1, ch2;
    static final long NUM_MSGS=10000;
    static final int MSG_SIZE=1000;
    private static final String GROUP=getUniqueClusterName("DiscardTest");
    final Promise<Long> ch1_all_received=new Promise<Long>();
    final Promise<Long> ch2_all_received=new Promise<Long>();



    @BeforeMethod
    protected void setUp() throws Exception {
        ch1_all_received.reset();
        ch2_all_received.reset();
    }
    
    @AfterMethod
    protected void tearDown() throws Exception{
        Util.close(ch2,ch1);
    }

    public void testDiscardProperties() throws Exception {
        _testLosslessReception(true);
    }

    public void testFastProperties() throws Exception {
        _testLosslessReception(false);
    }

    private void _testLosslessReception(boolean discard) throws Exception {
        Address ch1_addr, ch2_addr;
        long start, stop;

        ch1=createChannel(true);
        ch1.setReceiver(new MyReceiver(ch1_all_received, NUM_MSGS, "ch1"));
        ch2=createChannel(ch1);
        ch2.setReceiver(new MyReceiver(ch2_all_received, NUM_MSGS, "ch2"));

        if(discard) {
            DISCARD discard_prot=new DISCARD();
            Properties properties=new Properties();
            properties.setProperty("down", "0.1");

            ch1.getProtocolStack().insertProtocol(discard_prot, ProtocolStack.BELOW, "MERGE2");
            discard_prot=new DISCARD();
            properties=new Properties();
            properties.setProperty("down", "0.1");
            ch2.getProtocolStack().insertProtocol(discard_prot, ProtocolStack.BELOW, "MERGE2");
        }

        ch1.connect(GROUP);
        ch1_addr=ch1.getAddress();
        ch2.connect(GROUP);
        ch2_addr=ch2.getAddress();

        Util.sleep(2000);
        View v=ch2.getView();
        System.out.println("**** ch2's view: " + v);
        Assert.assertEquals(2, v.size());
        assertTrue(v.getMembers().contains(ch1_addr));
        assertTrue(v.getMembers().contains(ch2_addr));

        System.out.println("sending " + NUM_MSGS + " 1K messages to all members (including myself)");
        start=System.currentTimeMillis();
        for(int i=0;i < NUM_MSGS;i++) {
            Message msg=createMessage(MSG_SIZE);
            ch1.send(msg);
            if(i % 1000 == 0)
                System.out.println("-- sent " + i + " messages");
        }

        System.out.println("-- waiting for ch1 and ch2 to receive " + NUM_MSGS + " messages");
        Long num_msgs;
        num_msgs=ch1_all_received.getResult();
        System.out.println("-- received " + num_msgs + " messages on ch1");

        num_msgs=ch2_all_received.getResult();
        stop=System.currentTimeMillis();
        System.out.println("-- received " + num_msgs + " messages on ch2");

        long diff=stop - start;
        double msgs_sec=NUM_MSGS / (diff / 1000.0);
        System.out.println("== Sent and received " + NUM_MSGS
                           + " in "
                           + diff
                           + "ms, "
                           + msgs_sec
                           + " msgs/sec");
    }

    static class MyReceiver extends ReceiverAdapter {
        final Promise<Long> p;
        final long num_msgs_expected;
        long num_msgs=0;
        String channel_name;
        boolean operational=true;

        public MyReceiver(final Promise<Long> p,final long num_msgs_expected,String channel_name) {
            this.p=p;
            this.num_msgs_expected=num_msgs_expected;
            this.channel_name=channel_name;
        }

        public void receive(Message msg) {
            if(!operational)
                return;
            num_msgs++;

            if(num_msgs > 0 && num_msgs % 1000 == 0)
                System.out.println("-- received " + num_msgs + " on " + channel_name);

            if(num_msgs >= num_msgs_expected) {
                System.out.println("SUCCESS: received all " + num_msgs_expected
                                   + " messages on "
                                   + channel_name);
                operational=false;
                p.setResult(new Long(num_msgs));
            }
        }

        public void viewAccepted(View new_view) {
            System.out.println("-- view (" + channel_name + "): " + new_view);
        }
    }

    private static Message createMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0;i < buf.length;i++)
            buf[i]=(byte)'x';
        return new Message(null, null, buf);
    }


}

package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.TCP_NIO2;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Tests the NAKACK (retransmission) and STABLE (garbage collection) protocols
 * by discarding 10% of all network-bound messages
 * 
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class DiscardTest extends ChannelTestBase {
    JChannel a, b;
    static final long NUM_MSGS=10000;
    static final int MSG_SIZE=1000;
    private static final String GROUP="DiscardTest";
    final Promise<Long> ch1_all_received=new Promise<>();
    final Promise<Long> ch2_all_received=new Promise<>();



    @BeforeMethod
    protected void setUp() throws Exception {
        ch1_all_received.reset();
        ch2_all_received.reset();
    }
    
    @AfterMethod
    protected void tearDown() throws Exception {
        TP tp_a=a.getProtocolStack().getTransport(), tp_b=b.getProtocolStack().getTransport();
        if(tp_a instanceof TCP_NIO2) {
            System.out.printf("partial writes in A: %d, partial writes in B: %d\n",
                              ((TCP_NIO2)tp_a).numPartialWrites(), ((TCP_NIO2)tp_b).numPartialWrites());
        }
        Util.close(b, a);
    }

    public void testDiscardProperties() throws Exception {
        _testLosslessReception(true);
    }

    public void testFastProperties() throws Exception {
        _testLosslessReception(false);
    }

    private void _testLosslessReception(boolean discard) throws Exception {
        long start, stop;

        a=createChannel(true).name("A");
        a.setReceiver(new MyReceiver(ch1_all_received, NUM_MSGS, "A"));
        b=createChannel(a).name("B");
        b.setReceiver(new MyReceiver(ch2_all_received, NUM_MSGS, "B"));

        a.connect(GROUP);
        b.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        if(discard) {
            DISCARD discard_prot=new DISCARD();
            Properties properties=new Properties();
            properties.setProperty("down", "0.1");

            a.getProtocolStack().insertProtocol(discard_prot, ProtocolStack.Position.BELOW, MERGE3.class);
            discard_prot=new DISCARD();
            properties=new Properties();
            properties.setProperty("down", "0.1");
            b.getProtocolStack().insertProtocol(discard_prot, ProtocolStack.Position.BELOW, MERGE3.class);
        }

        System.out.printf("sending %d %d-byte messages to all members (including myself)\n", NUM_MSGS, MSG_SIZE);
        start=System.currentTimeMillis();
        for(int i=0;i < NUM_MSGS;i++) {
            Message msg=createMessage(MSG_SIZE);
            a.send(msg);
            if(i % 1000 == 0)
                System.out.println("-- sent " + i + " messages");
        }

        System.out.println("-- waiting for ch1 and ch2 to receive " + NUM_MSGS + " messages");
        Long num_msgs;
        num_msgs=ch1_all_received.getResultWithTimeout(10000);
        System.out.println("-- received " + num_msgs + " messages on ch1");

        num_msgs=ch2_all_received.getResultWithTimeout(10000);
        stop=System.currentTimeMillis();
        System.out.println("-- received " + num_msgs + " messages on ch2");

        long diff=stop - start;
        double msgs_sec=NUM_MSGS / (diff / 1000.0);
        System.out.printf("== Sent and received %d in %d ms, %.2f msgs/sec\n", NUM_MSGS, diff, msgs_sec);
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
                System.out.printf("-- received %d on %s\n", num_msgs, channel_name);

            if(num_msgs >= num_msgs_expected) {
                System.out.printf("SUCCESS: received all %d messages on %s\n", num_msgs_expected, channel_name);
                operational=false;
                p.setResult(num_msgs);
            }
        }

        public void viewAccepted(View new_view) {
            System.out.printf("-- view (%s): %s\n", channel_name, new_view);
        }
    }

    private static Message createMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0;i < buf.length;i++)
            buf[i]=(byte)'x';
        return new Message(null, buf);
    }


}

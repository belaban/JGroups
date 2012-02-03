
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class to test FRAG protocol. It uses ProtocolTester to assemble a minimal stack which only consists of
 * FRAG and LOOPBACK (messages are immediately resent up the stack). Sends NUM_MSGS with MSG_SIZE size down
 * the stack, they should be received as well.
 *
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class FragTest {
    public static final long NUM_MSGS=1000;
    public static final int MSG_SIZE=100000;
    public static final int FRAG_SIZE=24000;

    protected JChannel ch;


    private static Message createBigMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) buf[i]=(byte)'x';
        return new Message(null, null, buf);
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
            Message big_msg=createBigMessage(MSG_SIZE);
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
           Message big_msg=new Message(null, null, big_buffer, offset, MSG_SIZE);
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

    protected static JChannel createChannel() throws Exception {
        JChannel ch=new JChannel(false);
        ProtocolStack stack=new ProtocolStack();
        ch.setProtocolStack(stack);
        stack.addProtocol(new SHARED_LOOPBACK())
          .addProtocol(new PING())
          .addProtocol(new NAKACK2().setValue("use_mcast_xmit", false))
          .addProtocol(new UNICAST2())
          .addProtocol(new STABLE().setValue("max_bytes", 50000))
          .addProtocol(new GMS().setValue("print_local_addr", false))
          .addProtocol(new UFC())
          .addProtocol(new MFC())
          .addProtocol(new FRAG2().setValue("frag_size", FRAG_SIZE));
        stack.init();
        return ch;
    }


    private static class FragReceiver extends ReceiverAdapter {
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


}



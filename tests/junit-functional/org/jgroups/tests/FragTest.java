
package org.jgroups.tests;



import org.testng.annotations.*;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Global;
import org.jgroups.debug.ProtocolTester;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;


/**
 * Class to test FRAG protocol. It uses ProtocolTester to assemble a minimal stack which only consists of
 * FRAG and LOOPBACK (messages are immediately resent up the stack). Sends NUM_MSGS with MSG_SIZE size down
 * the stack, they should be received as well.
 *
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class FragTest {
    public static final long NUM_MSGS=10;
    public static final int MSG_SIZE=100000;
    public static final int FRAG_SIZE=24000;


    private static Message createBigMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) buf[i]=(byte)'x';
        return new Message(null, null, buf);
    }



    public void testRegularMessages() throws Exception {
        FragReceiver frag_receiver=new FragReceiver(this);
        ProtocolTester t=new ProtocolTester("FRAG2(frag_size=" + FRAG_SIZE + ')', frag_receiver);
        Message big_msg;
        IpAddress local_addr=new IpAddress(5555);

        System.out.println("\nProtocol for protocol tester: " + t.getProtocolSpec() + '\n');

        for(int i=0; i < NUM_MSGS; i++) {
            big_msg=createBigMessage(MSG_SIZE);
            big_msg.setSrc(local_addr);
            System.out.println("sending msg #" + i + " [" + big_msg.getLength() + " bytes]");
            frag_receiver.down(new Event(Event.MSG, big_msg));
            Util.sleep(10);
        }
        t.stop();
    }


    public void testMessagesWithOffsets() throws Exception {
        FragReceiver frag_receiver=new FragReceiver(this);
        ProtocolTester t=new ProtocolTester("FRAG2(frag_size=" + FRAG_SIZE + ')', frag_receiver);
        Message big_msg;
        IpAddress local_addr=new IpAddress(5555);

        System.out.println("\nProtocol for protocol tester: " + t.getProtocolSpec() + '\n');

        byte[] big_buffer=new byte[(int)(MSG_SIZE * NUM_MSGS)];

        int offset=0;

        for(int i=0; i < NUM_MSGS; i++) {
            big_msg=new Message(null, null, big_buffer, offset, MSG_SIZE);
            big_msg.setSrc(local_addr);
            System.out.println("sending msg #" + i + " [" + big_msg.getLength() + " bytes]");
            frag_receiver.down(new Event(Event.MSG, big_msg));
            Util.sleep(10);
            offset+=MSG_SIZE;
        }
        t.stop();
    }



    private static class FragReceiver extends Protocol {
        long num_msgs=0;
        FragTest t=null;

        FragReceiver(FragTest t) {
            this.t=t;
        }

        public Object up(Event evt) {
            Message msg=null;
            Address sender;

            if(evt == null || evt.getType() != Event.MSG)
                return null;
            msg=(Message)evt.getArg();
            sender=msg.getSrc();
            if(sender == null) {
                log.error("FragTest.FragReceiver.up(): sender is null; discarding msg");
                return null;
            }
            System.out.println("Received msg from " + sender + " [" + msg.getLength() + " bytes]");
            return null;
        }

    }


}



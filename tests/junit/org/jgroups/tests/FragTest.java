// $Id: FragTest.java,v 1.7 2005/04/29 08:37:08 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
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
public class FragTest extends TestCase {
    public final long NUM_MSGS=10;
    public final int MSG_SIZE=100000;
    public final int FRAG_SIZE=24000;


    public FragTest(String name) {
        super(name);
    }




    private Message createBigMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) buf[i]=(byte)'x';
        return new Message(null, null, buf);
    }


    public void test0() throws Exception {
        Object mutex=new Object();
        FragReceiver frag_receiver=new FragReceiver(this, mutex);
        ProtocolTester t=new ProtocolTester("FRAG(frag_size=" + FRAG_SIZE + ')', frag_receiver);
        Message big_msg;
        IpAddress local_addr=new IpAddress(5555);

        System.out.println("\nProtocol for protocol tester: " + t.getProtocolSpec() + '\n');

        synchronized(mutex) {
            for(int i=0; i < NUM_MSGS; i++) {
                big_msg=createBigMessage(MSG_SIZE);
                big_msg.setSrc(local_addr);
                System.out.println("sending msg #" + i + " [" + big_msg.getLength() + " bytes]");
                frag_receiver.down(new Event(Event.MSG, big_msg));
                Util.sleep(10);
            }
        }
        t.stop();
    }


    public static Test suite() {
        TestSuite s=new TestSuite(FragTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }


    private static class FragReceiver extends Protocol {
        long num_msgs=0;
        FragTest t=null;
        Object mut=null;

        FragReceiver(FragTest t, Object mut) {
            this.t=t;
            this.mut=mut;
        }

        public String getName() {
            return "FragReceiver";
        }


        public void up(Event evt) {
            Message msg=null;
            Address sender;

            if(evt == null || evt.getType() != Event.MSG)
                return;
            msg=(Message)evt.getArg();
            sender=msg.getSrc();
            if(sender == null) {
                System.err.println("FragTest.FragReceiver.up(): sender is null; discarding msg");
                return;
            }
            System.out.println("Received msg from " + sender + " [" + msg.getLength() + " bytes]");
        }

    }


}



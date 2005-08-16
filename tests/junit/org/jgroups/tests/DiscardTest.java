// $Id: DiscardTest.java,v 1.3 2005/08/16 10:15:17 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;


/**
 * Tests the NAKACK (retransmission) and STABLE (garbage collection) protocols
 * by discarding 10% of all network-bound messages
 * @author Bela Ban
 * @version $Id: DiscardTest.java,v 1.3 2005/08/16 10:15:17 belaban Exp $
 */
public class DiscardTest extends TestCase {
    JChannel ch1, ch2;

    final String discard_props="discard.xml";             // located in JGroups/conf, needs to be in the classpath
    final String fast_props="fc-fast-minimalthreads.xml"; // located in JGroups/conf, needs to be in the classpath
    final long NUM_MSGS=10000;
    final int  MSG_SIZE=1000;
    private static final String GROUP="DiscardTestGroup";


    public DiscardTest(String name) {
        super(name);
    }

    public void testDiscardProperties() throws Exception {
        _testLosslessReception(discard_props);
    }

    public void testFastProperties() throws Exception {
        _testLosslessReception(fast_props);
    }

    public void _testLosslessReception(String props) throws Exception {
        Address ch1_addr, ch2_addr;
        long start, stop;
        ch1=new JChannel(props);
        ch2=new JChannel(props);

        ch1.connect(GROUP);
        ch1_addr=ch1.getLocalAddress();
        ch2.connect(GROUP);
        ch2_addr=ch2.getLocalAddress();

        View v=(View)ch1.receive(2000);
        assertEquals(v.size(), 1);
        assertTrue(v.getMembers().contains(ch1_addr));

        v=(View)ch1.receive(20000);
        assertEquals(v.size(), 2);
        assertTrue(v.getMembers().contains(ch1_addr));
        assertTrue(v.getMembers().contains(ch2_addr));

        v=(View)ch2.receive(5000);
        assertTrue(v.getMembers().contains(ch1_addr));
        assertTrue(v.getMembers().contains(ch2_addr));

        System.out.println("View ch1=" + ch1.getView());
        System.out.println("View ch2=" + ch2.getView());
        System.out.println("sending " + NUM_MSGS + " 1K messages to all members (including myself)");
        start=System.currentTimeMillis();
        for(int i=0; i < NUM_MSGS; i++) {
            Message msg=createMessage(MSG_SIZE);
            ch1.send(msg);
            if(i % 1000 == 0)
                System.out.println("-- sent " + i + " messages");
        }

        System.out.println("receiving messages on ch1");
        long received_msgs=0;
        Object obj;
        while(true) {
            obj=ch1.receive(10000);
            if(obj instanceof Message) {
                received_msgs++;
                if(received_msgs % 1000 == 0)
                    System.out.println("-- received " + received_msgs + " on ch1");
                if(received_msgs >= NUM_MSGS) {
                    System.out.println("SUCCESS: received all " + NUM_MSGS + " messages on ch1");
                    break;
                }
            }
        }

        System.out.println("receiving messages on ch2");
        received_msgs=0;
        while(true) {
            obj=ch2.receive(5000);
            if(obj instanceof Message) {
                received_msgs++;
                if(received_msgs % 1000 == 0)
                    System.out.println("-- received " + received_msgs + " on ch2");
                if(received_msgs >= NUM_MSGS) {
                    System.out.println("SUCCESS: received all " + NUM_MSGS + " messages on ch2");
                    break;
                }
            }
        }
        stop=System.currentTimeMillis();
        long diff=stop-start;
        double msgs_sec=NUM_MSGS / (diff / 1000.0);
        System.out.println("== Sent and received " + NUM_MSGS + " in " + diff + "ms, " +
                           msgs_sec + " msgs/sec");

        ch2.close();
        ch1.close();
    }



    private Message createMessage(int size) {
        byte[] buf=new byte[size];
        for(int i=0; i < buf.length; i++) buf[i]=(byte)'x';
        return new Message(null, null, buf);
    }




    public static Test suite() {
        TestSuite s=new TestSuite(DiscardTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }




}



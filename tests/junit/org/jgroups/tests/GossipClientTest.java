package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestCase;


/**
 * @author Bela Ban
 * @version $Id: GossipClientTest.java,v 1.2 2006/03/27 08:34:24 belaban Exp $
 */
public class GossipClientTest extends TestCase {


    public void testConnectDisconnectSequence() {
        // System.setProperty("org.apache.commons.logging.Log","org.apache.commons.logging.impl.SimpleLog");
        String props="TCP(start_port=7800;bind_addr=localhost;loopback=true):" +
                "TCPGOSSIP(timeout=3000;initial_hosts=localhost[7500]num_initial_members=1):" +
                "FD(timeout=2000;max_tries=4):" +
                "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=600,1200,2400,4800):" +
                "pbcast.GMS(print_local_addr=true;join_timeout=5000;join_retry_timeout=2000;" +
                "shun=true)";
        try {
            JChannel jChannel=new JChannel(props);
            jChannel.setOpt(Channel.LOCAL, Boolean.FALSE);
            jChannel.connect("testGroup");
            assertTrue(jChannel.isConnected());
            assertTrue(jChannel.isOpen());
            System.out.println(jChannel.printProtocolSpec(true));

            jChannel.close();
            assertFalse(jChannel.isConnected());
            assertFalse(jChannel.isOpen());
        }
        catch(Exception e) {
            System.out.println("Error channel " + e);
            fail(e.toString());
        }
        System.out.println("done");
    }


    public static Test suite() {
        TestSuite s=new TestSuite(GossipClientTest.class);
        return s;
    }

    public static void main(String[] args) {
        String[] testCaseName={GossipClientTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}


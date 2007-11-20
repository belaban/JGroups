package org.jgroups.tests;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;

/**
 * Tests custom protocol.
 * Author: Lenny Phan
 * Version: $Id: CustomProtocolTest.java,v 1.1.4.1 2007/11/20 08:53:45 belaban Exp $
 */
public class CustomProtocolTest extends TestCase {

    static final String PROTOCOL_STACK = "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):" +
            "org.jgroups.tests.CustomProtocolTest$MyProtocol:" +
            "PING(timeout=3000;num_initial_members=6):" +
            "FD(timeout=3000):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "FRAG:" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=true;print_local_addr=true)";

    public void testMyProtocol() throws Exception {
        System.out.println("PROTOCOL_STACK: " + PROTOCOL_STACK);
        JChannel channel = new JChannel(PROTOCOL_STACK);
        assertTrue(true);
    }

    // --------- BOILERPLATE CODE -----------------------------------

    /**
     * Constructor. Normally called by JUnit framework.
     *
     * @param testName The test name.
     * @throws Exception
     */
    public CustomProtocolTest(String testName) throws Exception {
        super(testName);
    }


    /**
     * A main() to allow running this test case by itself.
     *
     * @param args The command-line arguments.
     */
    public static void main(String args[]) {
        String[] testCaseName = {"com.oracle.jgroups.protocols.CustomProtocolTest"};
        junit.textui.TestRunner.main(testCaseName);
    }


    /**
     * Compose a suite of test cases.
     *
     * @return The suite.
     */
    public static TestSuite suite() {
        return new TestSuite(CustomProtocolTest.class);
    }

    public static class MyProtocol extends Protocol {

        public String getName() {
            return "MyProtocol";
        }
    }
}

package org.jgroups.tests;

import org.jgroups.JChannel;
import org.jgroups.Global;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests custom protocol.
 * Author: Lenny Phan
 */
public class CustomProtocolTest {

    static final String PROTOCOL_STACK = "UDP(mcast_port=45566;ip_ttl=32):" +
            "org.jgroups.tests.CustomProtocolTest$MyProtocol:" +
            "PING:" +
            "FD(timeout=3000):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK2(xmit_interval=500):" +
            "UNICAST3(xmit_interval=500):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "FRAG2:" +
            "pbcast.GMS(join_timeout=5000;" +
            "print_local_addr=true)";

    @Test(groups=Global.FUNCTIONAL)
    public static void testMyProtocol() throws Exception {
        System.out.println("PROTOCOL_STACK: " + PROTOCOL_STACK);
        JChannel channel = new JChannel(PROTOCOL_STACK);
        System.out.println("channel = " + channel);
        assert true;
    }


    public static class MyProtocol extends Protocol {

    }
}

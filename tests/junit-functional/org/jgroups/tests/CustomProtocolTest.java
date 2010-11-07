package org.jgroups.tests;

import org.testng.annotations.*;
import org.jgroups.JChannel;
import org.jgroups.Global;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests custom protocol.
 * Author: Lenny Phan
 * Version: $Id: CustomProtocolTest.java,v 1.7 2009/10/28 15:58:38 belaban Exp $
 */
public class CustomProtocolTest {

    static final String PROTOCOL_STACK = "UDP(mcast_port=45566;ip_ttl=32):" +
            "org.jgroups.tests.CustomProtocolTest$MyProtocol:" +
            "PING(timeout=3000;num_initial_members=6):" +
            "FD(timeout=3000):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
            "pbcast.STABLE(desired_avg_gossip=10000):" +
            "FRAG:" +
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

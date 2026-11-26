package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.testng.annotations.Test;

/**
 * Tests custom protocol.
 * Author: Lenny Phan
 */
public class CustomProtocolTest {

    @Test(groups=Global.FUNCTIONAL)
    public static void testMyProtocol() throws Exception {
        JChannel channel=new JChannel(new UDP(),
                                      new org.jgroups.tests.CustomProtocolTest.MyProtocol(),
                                      new PING(),
                                      new FD_ALL3(), new VERIFY_SUSPECT(), new NAKACK2(), new UNICAST3(),
                                      new STABLE(), new FRAG2(), new GMS());
        System.out.println("channel = " + channel);
        assert true;
    }


    public static class MyProtocol extends Protocol {

    }
}

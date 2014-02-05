package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests that unicast messages are not endlessly retransmitted when an ACK is dropped
 * (https://issues.jboss.org/browse/JGRP-1578)
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_DroppedAckTest {
    protected JChannel a, b;

    @BeforeMethod
    protected void setup() throws Exception {
        a=createChannel("A");
        b=createChannel("B");
        a.connect("UNICAST_DroppedAckTest");
        b.connect("UNICAST_DroppedAckTest");
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, a, b);
    }

    @AfterMethod protected void destroy() {Util.close(b, a);}

    public void testNotEndlessXmits() throws Exception {
        DISCARD discard_a=(DISCARD)a.getProtocolStack().findProtocol(DISCARD.class);

        discard_a.setDropDownUnicasts(5); // drops the next 5 ACKs

        for(int i=1; i <= 5; i++)
            b.send(a.getAddress(), i);

        UNICAST unicast_b=(UNICAST)b.getProtocolStack().findProtocol(UNICAST.class);
        for(int i=0; i < 10; i++) {
            int num_unacked_msgs=unicast_b.getNumUnackedMessages();
            System.out.println("num_unacked_msgs=" + num_unacked_msgs);
            if(num_unacked_msgs == 0)
                break;
            Util.sleep(1000);
        }

        assert unicast_b.getNumUnackedMessages() == 0 : "num_unacked_msgs on B should be 0 but is " + unicast_b.getNumUnackedMessages();
    }


    protected JChannel createChannel(String name) throws Exception {
        return new JChannel(new Protocol[] {
          new SHARED_LOOPBACK(),
          new PING().setValue("timeout", 300),
          new NAKACK2(),
          new DISCARD(),
          new UNICAST(),
          new GMS()
        }).name(name);
    }
}

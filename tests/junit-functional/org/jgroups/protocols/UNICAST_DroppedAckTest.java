package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests that unicast messages are not endlessly retransmitted when an ACK is dropped
 * (https://issues.redhat.com/browse/JGRP-1578)
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_DroppedAckTest {
    protected JChannel a, b;

    protected void setup(Class<? extends UNICAST3> unicast_class) throws Exception {
        a=createChannel(unicast_class, "A");
        b=createChannel(unicast_class, "B");
        a.connect("UNICAST_DroppedAckTest");
        b.connect("UNICAST_DroppedAckTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
    }

    @AfterMethod protected void destroy() {Util.close(b, a);}

    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {UNICAST3.class}
        };
    }

    @Test(dataProvider="configProvider")
    public void testNotEndlessXmits(Class<? extends UNICAST3> unicast_class) throws Exception {
        setup(unicast_class);

        DISCARD discard_a=a.getProtocolStack().findProtocol(DISCARD.class);
        discard_a.dropDownUnicasts(5); // drops the next 5 ACKs

        for(int i=1; i <= 5; i++)
            b.send(a.getAddress(), i);

        UNICAST3 ub=b.getProtocolStack().findProtocol(UNICAST3.class);
        for(int i=0; i < 10; i++) {
            int num_unacked_msgs=ub.getNumUnackedMessages();
            System.out.println("num_unacked_msgs=" + num_unacked_msgs);
            if(num_unacked_msgs == 0)
                break;
            Util.sleep(1000);
        }
        assert ub.getNumUnackedMessages() == 0 : "num_unacked_msgs on B should be 0 but is " + ub.getNumUnackedMessages();
    }


    protected static JChannel createChannel(Class<? extends UNICAST3> unicast_class, String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new MERGE3().setMaxInterval(3000).setMinInterval(1000),
                            new NAKACK2(),
                            new DISCARD(),
                            unicast_class.getDeclaredConstructor().newInstance().setXmitInterval(500),
                            new GMS()).name(name);
    }
}

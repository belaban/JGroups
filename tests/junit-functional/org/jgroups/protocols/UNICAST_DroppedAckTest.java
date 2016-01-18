package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.AbstractProtocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
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

    protected void setup(Class<? extends AbstractProtocol> unicast_class) throws Exception {
        a=createChannel(unicast_class, "A");
        b=createChannel(unicast_class, "B");
        a.connect("UNICAST_DroppedAckTest");
        b.connect("UNICAST_DroppedAckTest");
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, a, b);
    }

    @AfterMethod protected void destroy() {Util.close(b, a);}

    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {UNICAST.class},
          {UNICAST3.class}
        };
    }

    @Test(dataProvider="configProvider")
    public void testNotEndlessXmits(Class<? extends AbstractProtocol> unicast_class) throws Exception {
        setup(unicast_class);

        DISCARD discard_a=(DISCARD)a.getProtocolStack().findProtocol(DISCARD.class);
        discard_a.setDropDownUnicasts(5); // drops the next 5 ACKs

        for(int i=1; i <= 5; i++)
            b.send(a.getAddress(), i);

        AbstractProtocol unicast_b=b.getProtocolStack().findProtocol(UNICAST.class, UNICAST3.class);
        for(int i=0; i < 10; i++) {
            int num_unacked_msgs=numUnackedMessages(unicast_b);
            System.out.println("num_unacked_msgs=" + num_unacked_msgs);
            if(num_unacked_msgs == 0)
                break;
            Util.sleep(1000);
        }

        assert numUnackedMessages(unicast_b) == 0 : "num_unacked_msgs on B should be 0 but is " + numUnackedMessages(unicast_b);
    }

    protected int numUnackedMessages(AbstractProtocol unicast) {
        if(unicast instanceof UNICAST)
            return ((UNICAST)unicast).getNumUnackedMessages();
        if(unicast instanceof UNICAST3)
            return ((UNICAST3)unicast).getNumUnackedMessages();
        throw new IllegalArgumentException("Protocol " + unicast.getClass().getSimpleName() + " needs to be UNICAST or UNICAST3");
    }


    protected JChannel createChannel(Class<? extends AbstractProtocol> unicast_class, String name) throws Exception {
        return new JChannel(new AbstractProtocol[] {
          new SHARED_LOOPBACK(),
          new SHARED_LOOPBACK_PING(),
          new MERGE3().setValue("max_interval", 3000).setValue("min_interval", 1000),
          new NAKACK2(),
          new DISCARD(),
          unicast_class.newInstance().setValue("xmit_interval", 500),
          new GMS()
        }).name(name);
    }
}

package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  5.6.0
 */
@Test(groups=Global.FUNCTIONAL)
public class VERSION_Test {

    public void testMatchingVersions() throws Exception {
        try(JChannel a=new JChannel(Util.getTestStack()).name("A").connect("vertest");
            JChannel b=new JChannel(Util.getTestStack()).name("B").connect("vertest")) {
            Util.waitUntilAllChannelsHaveSameView(5000, 100, a,b);

            VERSION v1=new VERSION().version(1), v2=new VERSION().version(1);
            a.stack().insertProtocol(v1, ProtocolStack.Position.ABOVE, TP.class);
            b.stack().insertProtocol(v2, ProtocolStack.Position.ABOVE, TP.class);

            MyReceiver<String> ra=new MyReceiver<String>().name("A").verbose(true),
              rb=new MyReceiver<String>().name("B").verbose(true);
            a.setReceiver(ra);
            b.setReceiver(rb);

            a.send(null, "from A");
            b.send(null, "from B");
            Util.waitUntil(5000, 100, () -> ra.size() == 2 && rb.size() == 2);
            ra.verbose(false); rb.verbose(false);
            assert v1.dropped() == 0;
            assert v2.dropped() == 0;
        }
    }

    public void testNonMatchingVersions() throws Exception {
        try(JChannel a=new JChannel(Util.getTestStack()).name("A");
            JChannel b=new JChannel(Util.getTestStack()).name("B")) {
            VERSION v1=new VERSION().version(1), v2=new VERSION().version(2);
            a.stack().insertProtocol(v1, ProtocolStack.Position.ABOVE, TP.class);
            b.stack().insertProtocol(v2, ProtocolStack.Position.ABOVE, TP.class);
            GMS gms=b.stack().findProtocol(GMS.class);
            gms.setJoinTimeout(1000).setMaxJoinAttempts(1);
            a.connect("ver");
            b.connect("ver");

            // won't be true
            Util.waitUntilTrue(2000, 100, () -> a.view().size() == 2 && b.view().size() == 2);
            assert a.view().size() == 1;
            assert b.view().size() == 1;
            assert v1.dropped() > 0;
            assert v2.dropped() == 0; // B doesn't receive any traffic because A drops B's messages
        }
    }

}

package org.jgroups.protocols;

import org.jgroups.BytesMessage;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Table;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Stream;

/**
 * Tests sending of STABLE messages when DONT_LOOPBACK is set (https://issues.redhat.com/browse/JGRP-2605)
 * @author Bela Ban
 * @since  5.2.1, 4.2.20
 */
@Test(groups=Global.FUNCTIONAL)
public class STABLE_Test {
    protected JChannel           a,b,c;
    protected MyReceiver<byte[]> r1, r2, r3;
    protected final String       GRP=STABLE_Test.class.getSimpleName();

    @BeforeMethod protected void setup() throws Exception {
        a=create("A").connect(GRP);
        b=create("B").connect(GRP);
        c=create("C").connect(GRP);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
        r1=new MyReceiver<byte[]>().rawMsgs(true);
        r2=new MyReceiver<byte[]>().rawMsgs(true);
        r3=new MyReceiver<byte[]>().rawMsgs(true);
        a.setReceiver(r1);
        b.setReceiver(r2);
        c.setReceiver(r3);
    }

    @AfterMethod protected void destroy() {Util.closeReverse(a,b,c);}

    public void testStableWithDontLoopback() throws Exception {
        byte[] payload=new byte[5000];
        for(int i=0; i < 10; i++) {
            Message msg=new BytesMessage(null, payload).setFlag(Message.TransientFlag.DONT_LOOPBACK);
            b.send(msg);
            Util.sleep(200); // prevents batches, which trigger STABLE msgs in non-10000 increments
        }
        Util.waitUntil(5000, 500, () -> Stream.of(r1,r3).allMatch(r -> r.size() == 10));
        assert r2.size() == 0;

        Util.waitUntilTrue(5000, 500, () -> Stream.of(a, b, c)
          .map(c -> ((NAKACK2)c.getProtocolStack().findProtocol(NAKACK2.class)).getWindow(b.getAddress()))
          .allMatch(t -> t.getHighestReceived() == 10 && t.getHighestDelivered() == 10 && t.getLow() == 10));

        for(JChannel ch: List.of(a, b, c)) {
            NAKACK2 n=ch.getProtocolStack().findProtocol(NAKACK2.class);
            Table<Message> t=n.getWindow(b.getAddress());
            assert t.getHighestReceived() == 10 && t.getHighestDelivered() == 10 && t.getLow() == 10
              : String.format("table for %s is %s (low is probably 0)", ch.getName(), t);
        }
    }

    protected static JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStack()).name(name);
        STABLE stable=ch.getProtocolStack().findProtocol(STABLE.class);
        stable.setDesiredAverageGossip(0) // disabled periodical stable
          .setMaxBytes(10000);

        ch.getProtocolStack().getTransport().getDiagnosticsHandler().setEnabled(true);
        return ch;
    }
}

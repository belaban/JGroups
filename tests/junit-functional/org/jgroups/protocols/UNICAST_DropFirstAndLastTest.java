package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests UNICAST2. Created to test the last-message-dropped problem, see https://issues.jboss.org/browse/JGRP-1548.
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class UNICAST_DropFirstAndLastTest {
    protected JChannel            a, b;
    protected MyReceiver<Integer> rb;
    protected DISCARD             discard; // on A

    protected void setup(Class<? extends Protocol> unicast_class) throws Exception {
        a=createChannel(unicast_class, "A");
        discard=(DISCARD)a.getProtocolStack().findProtocol(DISCARD.class);
        assert discard != null;
        a.connect("UNICAST_DropFirstAndLastTest");
        rb=new MyReceiver<Integer>().name("B").verbose(true);
        b=createChannel(unicast_class, "B").receiver(rb);
        b.connect("UNICAST_DropFirstAndLastTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
    }

    @AfterMethod protected void destroy() {setLevel("warn", a, b); Util.close(b, a); rb.reset();}


    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {UNICAST3.class}
        };
    }

    /**
     * A sends unicast messages 1-5 to B, but we drop message 5. The code in
     * https://issues.jboss.org/browse/JGRP-1548 now needs to make sure message 5 is retransmitted to B
     * within a short time period, and we don't have to rely on the stable task to kick in.
     */
    @Test(dataProvider="configProvider")
    public void testLastMessageDropped(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);
        setLevel("trace", a, b);
        Address dest=b.getAddress();
        for(int i=1; i <= 5; i++) {
            Message msg=new Message(dest, i);
            if(i == 5)
                discard.setDropDownUnicasts(1); // drops the next unicast
            a.send(msg);
        }

        List<Integer> msgs=rb.list();
        Util.waitUntilListHasSize(msgs, 5, 10000, 1000);
        System.out.println("list=" + msgs);
    }

    /**
     * A sends unicast message 1 to B, but we drop message 1. The code in
     * https://issues.jboss.org/browse/JGRP-1563 now needs to make sure message 1 is retransmitted to B
     * within a short time period, and we don't have to rely on the stable task to kick in.
     */
    @Test(dataProvider="configProvider")
    public void testFirstMessageDropped(Class<? extends Protocol> unicast_class) throws Exception {
        setup(unicast_class);

        System.out.println("**** closing all connections ****");
        // close all connections, so we can start from scratch and send message A1 to B
        for(JChannel ch: Arrays.asList(a,b)) {
            Protocol unicast=ch.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            removeAllConnections(unicast);
        }

        setLevel("trace", a, b);

        System.out.println("--> A sending first message to B (dropped before it reaches B)");
        discard.setDropDownUnicasts(1); // drops the next unicast
        a.send(new Message(b.getAddress(), 1));

        List<Integer> msgs=rb.list();
        try {
            Util.waitUntilListHasSize(msgs, 1, 500000, 500);
        }
        catch(AssertionError err) {
            printConnectionTables(a, b);
            throw err;
        }
        System.out.println("list=" + msgs);

        printConnectionTables(a, b);
        // assert ((UNICAST2)a.getProtocolStack().findProtocol(UNICAST2.class)).connectionEstablished(b.getAddress());
    }


    protected JChannel createChannel(Class<? extends Protocol> unicast_class, String name) throws Exception {
        Protocol unicast=unicast_class.newInstance();
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new NAKACK2().setValue("use_mcast_xmit", false),
                            new DISCARD(),
                            unicast.setValue("xmit_interval", 500),
                            new GMS().setValue("print_local_addr", false))
          .name(name);
    }

    protected void printConnectionTables(JChannel ... channels) {
        System.out.println("**** CONNECTIONS:");
        for(JChannel ch: channels) {
            Protocol ucast=ch.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            System.out.println(ch.getName() + ":\n" + printConnections(ucast) + "\n");
        }
    }

    protected void setLevel(String level, JChannel ... channels) {
        for(JChannel ch: channels)
            ch.getProtocolStack().findProtocol(Util.getUnicastProtocols()).level(level);
    }

    protected String printConnections(Protocol prot) {
        if(prot instanceof UNICAST3) {
            UNICAST3 unicast=(UNICAST3)prot;
            return unicast.printConnections();
        }
        throw new IllegalArgumentException("prot (" + prot + ") needs to be UNICAST3");
    }

    protected void removeAllConnections(Protocol prot) {
        if(prot instanceof UNICAST3) {
            UNICAST3 unicast=(UNICAST3)prot;
            unicast.removeAllConnections();
        }
        else
            throw new IllegalArgumentException("prot (" + prot + ") needs to be UNICAST3");
    }


}

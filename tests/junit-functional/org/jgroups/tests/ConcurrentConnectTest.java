package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.LOCAL_PING;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.List;

/**
 * Tests that A and B (UNICAST3 being absent) connecting to each other concurrently don't lose messages
 * (https://issues.redhat.com/browse/JGRP-2684)
 * @author Bela Ban
 * @since  5.2.14
 */
@Test(groups= Global.FUNCTIONAL)
public class ConcurrentConnectTest {
    protected JChannel           a,b;
    protected Address            addr_a, addr_b;
    protected MyReceiver<String> ra, rb;
    protected static final int   NUM=5;

    @BeforeMethod protected void setup() throws Exception {
        InetAddress lo=Util.getLoopback();
        ra=new MyReceiver<>();
        rb=new MyReceiver<>();
        a=create("A", lo).setReceiver(ra).connect(ConcurrentConnectTest.class.getSimpleName());
        b=create("B", lo).setReceiver(rb).connect(ConcurrentConnectTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b);
        addr_a=a.getAddress();
        addr_b=b.getAddress();
    }

    @AfterMethod protected void destroy() {
        Util.close(b, a);
    }



    /**
     * Creates {A,B}, then injects view {A} into A and {B} into B, this removes all TCP connections between A and B.
     * Then make A send 5 messages to B and vice versa. This causes concurrent connection establishment. Even with
     * UNICAST3 being absent, both A and B should receive all of the other's messages.
     */
    // @Test(invocationCount=10)
    public void testConcurrentConnect() throws Exception {
        View v1=View.create(addr_a, a.getView().getViewId().getId() +1, addr_a);
        View v2=View.create(addr_b, b.getView().getViewId().getId()+1, addr_b);
        GMS gms=a.getProtocolStack().findProtocol(GMS.class);
        gms.installView(v1);
        gms=b.getProtocolStack().findProtocol(GMS.class);
        gms.installView(v2);
        Util.waitUntil(5000, 100, () -> a.getView().size() == 1 && b.getView().size() == 1);
        Util.sleep(500); // wait for VIEW-ACK
        assert openConnections(a,b) == 0 : printConnections(a,b);

        List<String> la=ra.list(), lb=rb.list();
        for(int i=1; i <= NUM; i++)
            a.send(addr_b, String.format("%s%d", addr_a, i));
        for(int i=1; i <= NUM; i++)
            b.send(addr_a, String.format("%s%d", addr_b, i));
        Util.waitUntil(5000, 100, () -> la.size() == 5 && lb.size() == 5,
                       () -> String.format("A: %s, B: %s", la, lb));
        System.out.printf("### conns after sending:\n%s\n", printConnections(a,b));

        System.out.printf("A: %s\nB: %s\n", la, lb);
        for(int i=1; i <= NUM; i++) {
            assert la.get(i-1).equals("B" + i);
            assert lb.get(i-1).equals("A" + i);
        }
    }

    protected static String printConnections(JChannel... channels) {
        StringBuilder sb=new StringBuilder();
        for(JChannel ch: channels) {
            TCP tcp=(TCP)ch.getProtocolStack().getTransport();
            sb.append(String.format("%s: %s", ch.getAddress(), tcp.printConnections()));
        }
        return sb.toString();
    }

    protected static int openConnections(JChannel... channels) {
        int ret=0;
        for(JChannel ch: channels) {
            TCP tcp=(TCP)ch.getProtocolStack().getTransport();
            ret+=tcp.getOpenConnections();
        }
        return ret;
    }

    protected static JChannel create(String name, InetAddress bind_addr) throws Exception {
        TCP tcp=new TCP().useAcks(true).setBindAddr(bind_addr);
        tcp.getThreadPool().setRejectionPolicy("run");
        Protocol[] prots={tcp,
          new LOCAL_PING(), new NAKACK2().setXmitInterval(0), new GMS()};
        return new JChannel(prots).name(name);
    }
}

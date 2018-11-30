package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.Discovery;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Tests a leak in Discovery.ping_responses (https://issues.jboss.org/browse/JGRP-1983)
 * @author Bela Ban
 * @since  3.6.7
 */
@Test(groups=Global.FUNCTIONAL)
public class DiscoveryTest {
    protected JChannel a, b, c, d;

    @BeforeMethod
    protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A");
        b=new JChannel(Util.getTestStack()).name("B");
        c=new JChannel(Util.getTestStack()).name("C");
        d=new JChannel(Util.getTestStack()).name("D");

        a.connect("cluster");
        b.connect("cluster");
        c.connect("cluster");
        d.connect("cluster");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b, c, d);
        System.out.printf("view is %s\n", a.getView());
    }

    @AfterMethod protected void destroy() {Util.close(d, c, b, a);}


    /** Makes sure the leak caused by https://issues.jboss.org/browse/JGRP-1983 is not present anymore */
    public void testLeakFromCoord() throws Exception {
        testLeak(a);
    }

    public void testLeakFromParticipant() throws Exception {
        testLeak(d);
    }

    protected static void testLeak(JChannel discovery_initiator) throws Exception {
        ProtocolStack stack=discovery_initiator.getProtocolStack();
        Discovery ping=stack.findProtocol(Discovery.class);
        ping.discoveryRspExpiryTime(1000);
        Field ping_rsps_field=Util.getField(Discovery.class, "ping_responses");
        Map<Long,Responses> ping_rsps=(Map<Long,Responses>)Util.getField(ping_rsps_field, ping);
        for(int i=1; i <= 10; i++) {
            ping.down(new Event(Event.FIND_INITIAL_MBRS, 1000L));
        }
        for(int i=0; i < 10; i++) {
            Util.sleep(500);
            ping.weedOutCompletedDiscoveryResponses();
            if(ping_rsps.isEmpty())
                break;
            System.out.printf("responses map has %d elements:\n%s\n", ping_rsps.size(), print(ping_rsps));
        }
        assert ping_rsps.isEmpty() : String.format("responses map has %d elements:\n%s\n", ping_rsps.size(), print(ping_rsps));
    }

    protected static String print(Map<Long,Responses> map) {
        StringBuilder sb=new StringBuilder();
        for(Responses rsp: map.values())
            sb.append(rsp).append("\n");
        return sb.toString();
    }
}

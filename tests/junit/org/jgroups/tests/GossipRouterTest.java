package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.stream.Stream;

/**
 * @author Bela Ban
 */
@Test(groups={Global.STACK_INDEPENDENT,Global.GOSSIP_ROUTER,Global.EAP_EXCLUDED},singleThreaded=true)
public class GossipRouterTest {
    protected GossipRouter        router;
    protected JChannel            a, b;
    protected int                 gossip_router_port;
    protected String              gossip_router_hosts;
    protected InetAddress         bind_addr;
    protected String              bind_addr_str;

    @BeforeClass
    protected void setUp() throws Exception {
        bind_addr=Util.getLoopback();
        gossip_router_port=ResourceManager.getNextTcpPort(bind_addr);
        gossip_router_hosts=bind_addr.getHostAddress() + "[" + gossip_router_port + "]";
    }


    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(b,a);
        router.stop();
    }

    /**
     * Tests the following scenario (http://jira.jboss.com/jira/browse/JGRP-682):
     * - First node is started with tunnel.xml, cannot connect
     * - Second node is started *with* GossipRouter
     * - Now first node should be able to connect and first and second node should be able to merge into a group
     * - SUCCESS: a view of 2
     */
    public void testLateStart() throws Exception {
        a=createTunnelChannel("A").connect("demo");
        b=createTunnelChannel("B").connect("demo");
        System.out.println("-- starting GossipRouter");
        router=new GossipRouter(bind_addr_str, gossip_router_port).useNio(false);
        router.start();
        System.out.println("-- waiting for merge to happen --");
        Util.waitUntil(20000, 1000, () -> Stream.of(a, b).allMatch(c -> c.getView().size() == 2));
    }

    protected JChannel createTunnelChannel(String name) throws Exception {
        Protocol[] protocols={
          new TUNNEL().setReconnectInterval(1000).setGossipRouterHosts(gossip_router_hosts).setBindAddress(bind_addr),
          new PING(),
          new MERGE3().setMinInterval(1000).setMaxInterval(3000),
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().setJoinTimeout(1000)
        };
        return new JChannel(protocols).setName(name);
    }


}

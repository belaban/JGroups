package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests the TCPGOSSIP protocol
 * 
 * @author Vladimir Blagojevic
 * 
 **/
@Test(groups = { Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER, Global.EAP_EXCLUDED }, sequential = true)
public class TCPGOSSIP_Test {
    private JChannel            channel, coordinator;
    private final static String GROUP = "TCPGOSSIP_Test";
    private GossipRouter        gossipRouter;
    private int                 gossip_router_port;
    protected InetAddress       bind_addr;


    @BeforeClass
    void startRouter() throws Exception {
        String tmp=Util.getProperty(Global.BIND_ADDR);
        if(tmp == null) {
            StackType type=Util.getIpStackType();
            tmp=type == StackType.IPv6? "::1" : "127.0.0.1";
        }

        bind_addr=InetAddress.getByName(tmp);
        gossip_router_port=ResourceManager.getNextTcpPort(bind_addr);
        gossipRouter=new GossipRouter(null, gossip_router_port);
        gossipRouter.start();
    }


    @AfterClass(alwaysRun = true)
    void stopRouter() throws Exception {
        gossipRouter.stop();
    }

    @AfterMethod(alwaysRun = true)
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
    }

    /**
     * Tests connect-disconnect-connect sequence for a group with two members (using the default configuration).
     **/
    public void testDisconnectConnectTwo() throws Exception {
        coordinator=createTcpgossipChannel("A");
        channel=createTcpgossipChannel("B");
        coordinator.connect(GROUP);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);
        View view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getAddress());
        assert view.containsMember(coordinator.getAddress());
    }
    
    public void testAddInitialHosts() throws Exception {
        coordinator=createTcpgossipChannel("A");
        channel=createTcpgossipChannel("B");
        coordinator.connect(GROUP);
        channel.connect(GROUP);
        TCPGOSSIP p = (TCPGOSSIP) channel.getProtocolStack().findProtocol(TCPGOSSIP.class);
        String tmp_bind_addr = bind_addr.getHostAddress();
        assert p.removeInitialHost(tmp_bind_addr, gossip_router_port);
        p.addInitialHost(tmp_bind_addr, gossip_router_port);
       
        View view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getAddress());
        assert view.containsMember(coordinator.getAddress());
    }

    public void testConnectThree() throws Exception {
        JChannel third = null;
        try {
            coordinator=createTcpgossipChannel("A");
            channel=createTcpgossipChannel("B");
            coordinator.connect(GROUP);
            channel.connect(GROUP);
            third=createTcpgossipChannel("C");
            third.connect(GROUP);
            View view = channel.getView();
            assert channel.getView().size() == 3;
            assert third.getView().size() == 3;
            assert view.containsMember(channel.getAddress());
            assert view.containsMember(coordinator.getAddress());
        } finally {
            Util.close(third);
        }
    }

    public void testConnectThreeChannelsWithGRDown() throws Exception {
        JChannel third = null;
        try {
            coordinator=createTcpgossipChannel("A");
            channel=createTcpgossipChannel("B");
            coordinator.connect("testConnectThreeChannelsWithGRDown");
            channel.connect("testConnectThreeChannelsWithGRDown");

            // kill router
            gossipRouter.stop();
            

            // cannot discover others since GR is down
            third=createTcpgossipChannel("C");
            third.connect("testConnectThreeChannelsWithGRDown");

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameView(60000, 1000, coordinator, channel, third);

            // confirm they found each other
            View view = channel.getView();
            assert channel.getView().size() == 3;
            assert third.getView().size() == 3;
            assert view.containsMember(channel.getAddress());
            assert view.containsMember(coordinator.getAddress());
        } finally {
            Util.close(third);
        }
    }

    public void testConnectThreeChannelsWithGRAlreadyDown() throws Exception {
        JChannel third = null;
        try {
            coordinator=createTcpgossipChannel("A");
            channel=createTcpgossipChannel("B");

            // kill router
            gossipRouter.stop();
            
            // cannot discover others since GR is down
            coordinator.connect("testConnectThreeChannelsWithGRAlreadyDown");          
            channel.connect("testConnectThreeChannelsWithGRAlreadyDown");

            third=createTcpgossipChannel("C");
            third.connect("testConnectThreeChannelsWithGRAlreadyDown");

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameView(60000, 1000, coordinator, channel, third);

            // confirm they found each other
            View view = channel.getView();
            assert channel.getView().size() == 3;
            assert third.getView().size() == 3;
            assert view.containsMember(channel.getAddress());
            assert view.containsMember(coordinator.getAddress());
        } finally {
            Util.close(third);
        }
    }


    protected JChannel createTcpgossipChannel(String name) throws Exception {
        TCPGOSSIP gossip=new TCPGOSSIP();
        List<InetSocketAddress> initial_hosts=new ArrayList<>();
        initial_hosts.add(new InetSocketAddress(bind_addr, gossip_router_port));
        gossip.setInitialHosts(initial_hosts);

        JChannel ch=new JChannel(new TCP().setValue("use_send_queues",true)
                                   .setValue("sock_conn_timeout",300).setValue("bind_addr", bind_addr),
                                 gossip,
                                 new MERGE3().setValue("min_interval",1000).setValue("max_interval",3000),
                                 new FD().setValue("timeout",2000).setValue("max_tries",2),
                                 new VERIFY_SUSPECT(),
                                 new NAKACK2().setValue("use_mcast_xmit",false),
                                 new UNICAST3(), new STABLE(), new GMS().joinTimeout(1000));
        if(name != null)
            ch.setName(name);
        return ch;
    }



    protected void changeGossipRouter(JChannel channel, String host, int port) {
        TCPGOSSIP tcp_gossip_prot=(TCPGOSSIP)channel.getProtocolStack().findProtocol(TCPGOSSIP.class);
        List<InetSocketAddress> initial_hosts=tcp_gossip_prot.getInitialHosts();
        initial_hosts.clear();
        initial_hosts.add(new InetSocketAddress(host, port));
    }


}
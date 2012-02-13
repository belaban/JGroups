package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.TCPGOSSIP;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Tests the TCPGOSSIP protocol
 * 
 * @author Vladimir Blagojevic
 * 
 **/
@Test(groups = { Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER }, sequential = true)
public class TCPGOSSIP_Test {
    private JChannel channel, coordinator;
    private final static String GROUP = "TCPGOSSIP_Test";
    private GossipRouter gossipRouter;
    private static final String props = "tcpgossip.xml";

    @BeforeClass
    void startRouter() throws Exception {
        String bind_addr = getRouterBindAddress();

        System.setProperty("jgroups.bind_addr", bind_addr);

        gossipRouter = new GossipRouter(12001, bind_addr); // binds the GR to 127.0.0.1:12001
        gossipRouter.start();
    }

    private static String getRouterBindAddress() {
        StackType type = Util.getIpStackType();
        if (type == StackType.IPv6)
            return "::1";
        else
            return "127.0.0.1";
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
        coordinator=createChannel("A");
        channel=createChannel("B");
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
        coordinator=createChannel("A");
        channel=createChannel("B");
        coordinator.connect(GROUP);
        channel.connect(GROUP);
        TCPGOSSIP p = (TCPGOSSIP) channel.getProtocolStack().findProtocol(TCPGOSSIP.class);
        String bind_addr = getRouterBindAddress();
        assert p.removeInitialHost(bind_addr, 12001);
        p.addInitialHost(bind_addr, 12001);
       
        View view = channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getAddress());
        assert view.containsMember(coordinator.getAddress());
    }

    public void testConnectThree() throws Exception {
        JChannel third = null;
        try {
            coordinator=createChannel("A");
            channel=createChannel("B");
            coordinator.connect(GROUP);
            channel.connect(GROUP);
            third=createChannel("C");
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
            coordinator=createChannel("A");
            channel=createChannel("B");
            changeMergeInterval(coordinator, channel);
            coordinator.connect("testConnectThreeChannelsWithGRDown");
            channel.connect("testConnectThreeChannelsWithGRDown");

            // kill router
            gossipRouter.stop();
            

            // cannot discover others since GR is down
            third=createChannel("C");
            changeMergeInterval(third);
            third.connect("testConnectThreeChannelsWithGRDown");

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameSize(60000, 1000, coordinator, channel, third);

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
            coordinator=createChannel("A");
            channel=createChannel("B");
            changeMergeInterval(coordinator, channel);
            
            // kill router
            gossipRouter.stop();
            
            // cannot discover others since GR is down
            coordinator.connect("testConnectThreeChannelsWithGRAlreadyDown");          
            channel.connect("testConnectThreeChannelsWithGRAlreadyDown");

            third=createChannel("C");
            changeMergeInterval(third);
            third.connect("testConnectThreeChannelsWithGRAlreadyDown");

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameSize(60000, 1000, coordinator, channel, third);

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

    protected JChannel createChannel(String name) throws Exception {
        JChannel retval=new JChannel(props);
        retval.setName(name);
        changeGossipRouter(retval,getRouterBindAddress(),12001);
        return retval;
    }

    protected void changeGossipRouter(JChannel channel, String host, int port) {
        TCPGOSSIP tcp_gossip_prot=(TCPGOSSIP)channel.getProtocolStack().findProtocol(TCPGOSSIP.class);
        List<InetSocketAddress> initial_hosts=tcp_gossip_prot.getInitialHosts();
        initial_hosts.clear();
        initial_hosts.add(new InetSocketAddress(host, port));
    }

    protected void changeMergeInterval(JChannel ... channels) {
        for(JChannel ch: channels) {
            Protocol p=ch.getProtocolStack().findProtocol(MERGE2.class,MERGE3.class);
            if(p instanceof MERGE2) {
                ((MERGE2)p).setMinInterval(1000);
                ((MERGE2)p).setMaxInterval(3000);
            }
            else if(p instanceof MERGE3) {
                ((MERGE3)p).setMaxInterval(1000);
                ((MERGE3)p).setMaxInterval(3000);
            }
        }
    }
}
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.TCPGOSSIP;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests TCPGOSSIP protocol
 * 
 * @author Vladimir Blagojevic
 * 
 **/
@Test(groups = { Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER }, sequential = true)
public class TCPGOSSIP_Test extends ChannelTestBase {
    private JChannel channel, coordinator;
    private final static String GROUP = "TCPGOSSIP_Test";
    private GossipRouter gossipRouter;
    private static final String props = "tcpgossip.xml";

    @BeforeClass
    void startRouter() throws Exception {
        String bind_addr = getRouterBindAddress();
        gossipRouter = new GossipRouter(12001, bind_addr);
        gossipRouter.start();
    }

    private String getRouterBindAddress() {
        String bind_addr = Util.getProperty(Global.BIND_ADDR);
        if (bind_addr == null) {
            StackType type = Util.getIpStackType();
            if (type == StackType.IPv6)
                bind_addr = "::1";
            else
                bind_addr = "127.0.0.1";
        }
        return bind_addr;
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
     * Tests connect-disconnect-connect sequence for a group with two members (using default
     * configuration).
     **/
    public void testDisconnectConnectTwo() throws Exception {
        coordinator = new JChannel(props);
        channel = new JChannel(props);
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
        coordinator = new JChannel(props);
        channel = new JChannel(props);
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
            coordinator = new JChannel(props);
            channel = new JChannel(props);
            coordinator.connect(GROUP);
            channel.connect(GROUP);
            third = new JChannel(props);
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
            coordinator = new JChannel(props);
            channel = new JChannel(props);
            coordinator.connect("testConnectThreeChannelsWithGRDown");
            channel.connect("testConnectThreeChannelsWithGRDown");

            // kill router
            gossipRouter.stop();
            

            // cannot discover others since GR is down
            third = new JChannel(props);
            third.connect("testConnectThreeChannelsWithGRDown");
           

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameSize(60000, 500, coordinator, channel, third);

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
            coordinator = new JChannel(props);
            channel = new JChannel(props);
            
            // kill router
            gossipRouter.stop();
            
            // cannot discover others since GR is down
            coordinator.connect("testConnectThreeChannelsWithGRAlreadyDown");          
            channel.connect("testConnectThreeChannelsWithGRAlreadyDown");

            third = new JChannel(props);
            third.connect("testConnectThreeChannelsWithGRAlreadyDown");

            // restart and....
            gossipRouter.start();
            Util.waitUntilAllChannelsHaveSameSize(60000, 500, coordinator, channel, third);

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
}
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Promise;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Ensures that a disconnected channel reconnects correctly, for different stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 **/
@Test(groups={Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER, Global.EAP_EXCLUDED},singleThreaded=true)
public class TUNNEL_Test extends ChannelTestBase {
    protected JChannel            channel, coordinator, c;
    protected final static String GROUP="TUNNEL_Test";
    protected GossipRouter        gossipRouter;
    protected int                 gossip_router_port;
    protected String              gossip_router_hosts;
    protected InetAddress         gossip_router_bind_addr;

    @BeforeClass
    void startRouter() throws Exception {
        gossip_router_bind_addr=Util.getLoopback();
        gossip_router_port=ResourceManager.getNextTcpPort(gossip_router_bind_addr);
        gossip_router_hosts=gossip_router_bind_addr.getHostAddress() + "[" + gossip_router_port + "]";
        gossipRouter=new GossipRouter(gossip_router_bind_addr, gossip_router_port);
        gossipRouter.start();
    }
    
    @AfterClass(alwaysRun=true)
    void stopRouter() throws Exception {
        gossipRouter.stop();
    }

    @AfterMethod(alwaysRun=true)
    void tearDown() throws Exception {
        Util.close(c, channel, coordinator);
    }




    /**
     * Tests if the channel has a null local address after disconnect (using TUNNEL).
     **/
    public void testNullLocalAddress_TUNNEL() throws Exception {
        channel=createTunnelChannel("A");
        channel.connect(GROUP);
        assert channel.getAddress() != null;
        channel.disconnect();
        assert channel.getAddress() == null;
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using default configuration).
     **/
    public void testDisconnectConnectOne_Default() throws Exception {
        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect("DisconnectTest.testgroup-2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getAddress());
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using default configuration).
     **/
    public void testDisconnectConnectTwo_Default() throws Exception {
        coordinator=createTunnelChannel("B", false);
        channel=createTunnelChannel("A", false);
        coordinator.connect(GROUP);
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);
        View view=channel.getView();
        assert view.size() == 2;
        assert view.containsMember(channel.getAddress());
        assert view.containsMember(coordinator.getAddress());
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members, using the default stack configuration. Assumes that default
     * configuration includes pbcast.NAKACK. Test case introduced before fixing
     * pbcast.NAKACK bug, which used to leave pbcast.NAKACK in a broken state
     * after DISCONNECT. Because of this problem, the channel couldn't be used
     * to multicast messages.
     **/
    public void testDisconnectConnectSendTwo_Default() throws Exception {
        final Promise<Message> msgPromise=new Promise<>();
        coordinator=createTunnelChannel("B");
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new BytesMessage(null, "payload"));

        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }


    /**
      * Tests connect-disconnect-connect sequence for a group with one member
      * (using TUNNEL).
      **/
     public void testDisconnectConnectOne_TUNNEL() throws Exception {
        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect("DisconnectTest.testgroup-2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getAddress());
    }
     
     public void testFailureDetection() throws Exception {
         coordinator=createTunnelChannel("B");
         coordinator.setName("coord");
         coordinator.connect(GROUP);
         
         channel=createTunnelChannel("A");
         channel.setName("participant");
         channel.connect(GROUP);

         System.out.println("shutting down the participant channel");
         Util.shutdown(channel);

         GMS coord_gms=coordinator.getProtocolStack().findProtocol(GMS.class);
         if(coord_gms != null)
             coord_gms.setLevel("trace");

         View view;
         long end_time=System.currentTimeMillis() + 20000;
         while(System.currentTimeMillis() < end_time) {
             view=coordinator.getView();
             if(view.size() == 1)
                 break;
             Util.sleep(500);
         }
         view=coordinator.getView();
         assert view.size() == 1 : "coordinator's view is " + view + ", but we expected a view of 1 member";
         if(coord_gms != null)
             coord_gms.setLevel("warn");
     }
     
     public void testConnectThree() throws Exception {
         coordinator=createTunnelChannel("B");

         channel=createTunnelChannel("A");

         coordinator.connect(GROUP);
         channel.connect(GROUP);
         
         c=createTunnelChannel("C");
         c.connect(GROUP);
         
         Util.waitUntilAllChannelsHaveSameView(10000, 1000, coordinator, channel, c);
         View view=c.getView();

         assert view.containsMember(channel.getAddress());
         assert view.containsMember(coordinator.getAddress());
     }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {
         coordinator=createTunnelChannel("B");
         coordinator.connect(GROUP);
         channel=createTunnelChannel("A");
         channel.connect("DisconnectTest.testgroup-1");
         channel.disconnect();
         channel.connect(GROUP);

         Thread.sleep(1000);

         View view=channel.getView();
         assert view.size() == 2;
         assert view.containsMember(channel.getAddress());
         assert view.containsMember(coordinator.getAddress());
     }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
      * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
      * bug, which used to leave pbcast.NAKACK in a broken state after
      * DISCONNECT. Because of this problem, the channel couldn't be used to
      * multicast messages.
      **/
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {
        final Promise<Message> msgPromise=new Promise<>();
        coordinator=createTunnelChannel("B");
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new BytesMessage(null, "payload"));

        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }


    protected JChannel createTunnelChannel(String name) throws Exception {
        return createTunnelChannel(name, true);
    }

    protected JChannel createTunnelChannel(String name, boolean include_failure_detection) throws Exception {
        TUNNEL tunnel=new TUNNEL().setBindAddress(gossip_router_bind_addr);
        FD_ALL3 fd_all=new FD_ALL3().setTimeout(2000).setInterval(500);
        tunnel.setGossipRouterHosts(gossip_router_hosts);
        List<Protocol> protocols=new ArrayList<>(Arrays.asList(tunnel, new PING(),
                                                               new MERGE3().setMinInterval(1000)
                                                                 .setMaxInterval(3000)));
        if(include_failure_detection) {
            List<Protocol> tmp=new ArrayList<>(2);
            tmp.add(fd_all);
            tmp.add(new VERIFY_SUSPECT());
            protocols.addAll(tmp);
        }
        protocols.addAll(Arrays.asList(new NAKACK2().useMcastXmit(false),
                                       new UNICAST3(), new STABLE(), new GMS().setJoinTimeout(1000)));
        JChannel ch=new JChannel(protocols);
        if(name != null)
            ch.setName(name);
        return ch;
    }



    private static class PromisedMessageListener implements Receiver {
        private final Promise<Message> promise;

        public PromisedMessageListener(Promise<Message> promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }

}
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Promise;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 **/
@Test(groups={Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER},sequential=true)
public class TUNNEL_Test extends ChannelTestBase{
    private JChannel            channel, coordinator;
    private final static String GROUP="TUNNEL_Test";
    private GossipRouter        gossipRouter;
    private int                 gossip_router_port;
    private String              gossip_router_hosts;

    @BeforeClass
    void startRouter() throws Exception {
        String bind_addr=Util.getProperty(Global.BIND_ADDR);
        if(bind_addr == null) {
            StackType type=Util.getIpStackType();
            if(type == StackType.IPv6)
                bind_addr="::1";
            else
                bind_addr="127.0.0.1";
        }

        gossip_router_port=ResourceManager.getNextTcpPort(InetAddress.getByName(bind_addr));
        gossip_router_hosts=bind_addr + "[" + gossip_router_port + "]";
        gossipRouter=new GossipRouter(gossip_router_port, null);
        gossipRouter.start();
    }
    
    @AfterClass(alwaysRun=true)
    void stopRouter() throws Exception {
        gossipRouter.stop();
    }

    @AfterMethod(alwaysRun=true)
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
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
        coordinator=createTunnelChannel("B");

        channel=createTunnelChannel("A");

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
        final Promise<Message> msgPromise=new Promise<Message>();
        coordinator=createTunnelChannel("B");
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new Message(null, null, "payload"));

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

         GMS coord_gms=(GMS)coordinator.getProtocolStack().findProtocol(GMS.class);
         if(coord_gms != null)
             coord_gms.setLevel("trace");

         View view;
         long end_time=System.currentTimeMillis() + 10000;
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
         
         JChannel third = createTunnelChannel("C");
         third.connect(GROUP);
         
         View view=channel.getView();
         assert channel.getView().size() == 3;
         assert third.getView().size() == 3;
         assert view.containsMember(channel.getAddress());
         assert view.containsMember(coordinator.getAddress());
         
         Util.close(third);
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
        final Promise<Message> msgPromise=new Promise<Message>();
        coordinator=createTunnelChannel("B");
        coordinator.connect(GROUP);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));

        channel=createTunnelChannel("A");
        channel.connect("DisconnectTest.testgroup-1");
        channel.disconnect();
        channel.connect(GROUP);

        channel.send(new Message(null, null, "payload"));

        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert "payload".equals(msg.getObject());
    }


    protected JChannel createTunnelChannel(String name) throws Exception {
        TUNNEL tunnel=(TUNNEL)new TUNNEL().setValue("enable_bundling",false);
        tunnel.setGossipRouterHosts(gossip_router_hosts);
        JChannel ch=Util.createChannel(tunnel,
                                       new PING(),
                                       new MERGE2().setValue("min_interval", 1000).setValue("max_interval", 3000),
                                       new FD().setValue("timeout", 2000).setValue("max_tries", 2),
                                       new VERIFY_SUSPECT(),
                                       new NAKACK2().setValue("use_mcast_xmit", false),
                                       new UNICAST(), new STABLE(), new GMS());
        if(name != null)
            ch.setName(name);
        return ch;
    }



    private static class PromisedMessageListener extends ReceiverAdapter {
        private final Promise<Message> promise;

        public PromisedMessageListener(Promise<Message> promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }

}
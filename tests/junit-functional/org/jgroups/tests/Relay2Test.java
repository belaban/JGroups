package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.Relayer;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Various RELAY2-related tests
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class Relay2Test {
    protected JChannel a, b, c;  // members in site "lon"
    protected JChannel x, y, z; // members in site "sfo"

    protected static final String BRIDGE_CLUSTER="global";
    protected static final String LON_CLUSTER="lon-cluster";
    protected static final String SFO_CLUSTER="sfo-cluster";

    @AfterMethod protected void destroy() {Util.close(z,y,x,c,b,a);}


    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.jboss.org/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge() throws Exception {
        a=createNode("lon", "A", LON_CLUSTER, null);
        b=createNode("lon", "B", LON_CLUSTER, null);
        Util.waitUntilAllChannelsHaveSameSize(30000, 500, a,b);

        x=createNode("sfo", "X", SFO_CLUSTER, null);
        assert x.getView().size() == 1;

        RELAY2 ar=(RELAY2)a.getProtocolStack().findProtocol(RELAY2.class),
          xr=(RELAY2)x.getProtocolStack().findProtocol(RELAY2.class);

        assert ar != null && xr != null;

        JChannel a_bridge=null, x_bridge=null;
        for(int i=0; i < 20; i++) {
            a_bridge=ar.getBridge("sfo");
            x_bridge=xr.getBridge("lon");
            if(a_bridge != null && x_bridge != null && a_bridge.getView().size() == 2 && x_bridge.getView().size() == 2)
                break;
            Util.sleep(500);
        }

        assert a_bridge != null && x_bridge != null;

        System.out.println("A's bridge channel: " + a_bridge.getView());
        System.out.println("X's bridge channel: " + x_bridge.getView());
        assert a_bridge.getView().size() == 2 : "bridge view is " + a_bridge.getView();
        assert x_bridge.getView().size() == 2 : "bridge view is " + x_bridge.getView();

        Relayer.Route route=xr.getRoute("lon");
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        // Now inject a partition into site "lon"
        System.out.println("Creating partition between A and B:");
        createPartition(a, b);

        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());
        assert a.getView().size() == 1 && b.getView().size() == 1;

        route=xr.getRoute("lon");
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        View bridge_view=xr.getBridgeView(BRIDGE_CLUSTER);
        System.out.println("bridge_view = " + bridge_view);

        // Now make A and B form a cluster again:
        View merge_view=new MergeView(a.getAddress(), 10, Arrays.asList(a.getAddress(), b.getAddress()),
                                      Arrays.asList(Util.createView(a.getAddress(), 5, a.getAddress()),
                                                    Util.createView(b.getAddress(), 5, b.getAddress())));
        GMS gms=(GMS)a.getProtocolStack().findProtocol(GMS.class);
        gms.installView(merge_view, null);
        gms=(GMS)b.getProtocolStack().findProtocol(GMS.class);
        gms.installView(merge_view, null);

        Util.waitUntilAllChannelsHaveSameSize(30000, 500, a, b);
        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());

        for(int i=0; i < 20; i++) {
            bridge_view=xr.getBridgeView(BRIDGE_CLUSTER);
            if(bridge_view != null && bridge_view.size() == 2)
                break;
            Util.sleep(500);
        }

        route=xr.getRoute("lon");
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;
    }


    /**
     * Tests whether the bridge channel connects and disconnects ok.
     * @throws Exception
     */
    public void testConnectAndReconnectOfBridgeStack() throws Exception {
        a=new JChannel(createBridgeStack());
        a.setName("A");
        b=new JChannel(createBridgeStack());
        b.setName("B");

        a.connect(BRIDGE_CLUSTER);
        b.connect(BRIDGE_CLUSTER);
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a, b);

        b.disconnect();
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a);

        b.connect(BRIDGE_CLUSTER);
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, a, b);
    }



    /**
     * Tests sites LON and SFO, with SFO disconnecting (bridge view on LON should be 1) and reconnecting (bridge view on
     * LON and SFO should be 2)
     * @throws Exception
     */
    public void testDisconnectAndReconnect() throws Exception {
        a=createNode("lon", "A", LON_CLUSTER, null);
        x=createNode("sfo", "X", SFO_CLUSTER, null);

        System.out.println("Started A and X; waiting for bridge view of 2 on A and X");
        waitForBridgeView(2, 20000, 500, a, x);

        System.out.println("Disconnecting X; waiting for a bridge view on 1 on A");
        x.disconnect();
        waitForBridgeView(1, 20000, 500, a);

        System.out.println("Reconnecting X again; waiting for a bridge view of 2 on A and X");
        x.connect(SFO_CLUSTER);
        waitForBridgeView(2, 20000, 500, a, x);
    }





    /**
     * Tests that queued messages are forwarded successfully. The scenario is:
     * <ul>
     *     <li>Node A in site LON, node X in site SFO</li>
     *     <li>Node X is brought down (gracefully)</li>
     *     <li>Node A sends a few unicast messages to the site master of SFO (queued)</li>
     *     <li>Node X is started again</li>
     *     <li>The queued messages on A should be forwarded to the site master of SFO</li>
     * </ul>
     * https://issues.jboss.org/browse/JGRP-1528
     */
    public void testQueueingAndForwarding() throws Exception {
        MyReceiver rx=new MyReceiver();
        a=createNode("lon", "A", LON_CLUSTER, null);
        x=createNode("sfo", "X", SFO_CLUSTER, rx);

        System.out.println("Waiting for site SFO to be UP");
        RELAY2 relay_a=(RELAY2)a.getProtocolStack().findProtocol(RELAY2.class);
        Relayer.Route sfo_route=relay_a.getRoute("sfo");

        for(int i=0; i < 20; i++) {
            if(sfo_route.getStatus() == RELAY2.RouteStatus.UP)
                break;
            Util.sleep(500);
        }
        System.out.println("Route to SFO: " + sfo_route);
        assert sfo_route.getStatus() == RELAY2.RouteStatus.UP;

        Address sm_sfo=new SiteMaster("sfo");
        System.out.println("Sending message 0 to the site master of SFO");
        a.send(sm_sfo, 0);

        List<Integer> list=rx.getList();
        for(int i=0; i < 20; i++) {
            if(!list.isEmpty())
                break;
            Util.sleep(500);
        }
        System.out.println("list = " + list);
        assert list.size() == 1 && list.get(0) == 0;
        rx.clear();

        x.disconnect();
        System.out.println("Waiting for site SFO to be UNKNOWN");

        for(int i=0; i < 20; i++) {
            if(sfo_route.getStatus() == RELAY2.RouteStatus.UNKNOWN)
                break;
            Util.sleep(500);
        }
        System.out.println("Route to SFO: " + sfo_route);
        assert sfo_route.getStatus() == RELAY2.RouteStatus.UNKNOWN;


        System.out.println("sending 5 messages from A to site master SFO - they should all get queued");
        for(int i=1; i <= 5; i++)
            a.send(sm_sfo, i);

        System.out.println("Starting X again; the queued messages should now get re-sent");
        x.connect(SFO_CLUSTER);

        for(int i=0; i < 20; i++) {
            if(list.size() == 5)
                break;
            Util.sleep(500);
        }
        System.out.println("list = " + list);
        assert list.size() == 5;
        for(int i=1; i <= 5; i++)
            assert list.contains(i);
    }




    protected JChannel createNode(String site_name, String node_name, String cluster_name, Receiver receiver) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new PING().setValue("timeout", 300).setValue("num_initial_members", 2),
                                 new NAKACK2(),
                                 new UNICAST2(),
                                 new GMS(),
                                 new FORWARD_TO_COORD(),
                                 createRELAY2(site_name));
        ch.setName(node_name);
        if(receiver != null)
            ch.setReceiver(receiver);
        if(cluster_name != null)
            ch.connect(cluster_name);
        return ch;
    }


    protected RELAY2 createRELAY2(String site_name) {
        RELAY2 relay=new RELAY2().site(site_name).enableAddressTagging(false);

        RelayConfig.SiteConfig lon_cfg=new RelayConfig.SiteConfig("lon", (short)0),
          sfo_cfg=new RelayConfig.SiteConfig("sfo", (short)1);

        lon_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));
        sfo_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));

        relay.addSite("lon", lon_cfg).addSite("sfo", sfo_cfg);
        return relay;
    }

    protected static Protocol[] createBridgeStack() {
        return new Protocol[]{
          new SHARED_LOOPBACK(),
          new PING().setValue("timeout", 500).setValue("num_initial_members", 2),
          new NAKACK2(),
          new UNICAST2(),
          new GMS()
        };
    }

    /** Creates a singleton view for each channel listed and injects it */
    protected static void createPartition(JChannel ... channels) {
        for(JChannel ch: channels) {
            View view=Util.createView(ch.getAddress(), 5, ch.getAddress());
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected void waitForBridgeView(int expected_size, long timeout, long interval, JChannel ... channels) {
        long deadline=System.currentTimeMillis() + timeout;

        while(System.currentTimeMillis() < deadline) {
            boolean views_correct=true;
            for(JChannel ch: channels) {
                RELAY2 relay=(RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class);
                View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
                if(bridge_view == null || bridge_view.size() != expected_size) {
                    views_correct=false;
                    break;
                }
            }
            if(views_correct)
                break;
            Util.sleep(interval);
        }

        System.out.println("Bridge views:\n");
        for(JChannel ch: channels) {
            RELAY2 relay=(RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            System.out.println(ch.getAddress() + ": " + bridge_view);
        }

        for(JChannel ch: channels) {
            RELAY2 relay=(RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(BRIDGE_CLUSTER);
            assert bridge_view != null && bridge_view.size() == expected_size
              : ch.getAddress() + ": bridge view=" + bridge_view + ", expected=" + expected_size;
        }
    }


    protected static class MyReceiver extends ReceiverAdapter {
        protected final List<Integer> list=new ArrayList<Integer>(5);

        public List<Integer> getList()            {return list;}
        public void          clear()              {list.clear();}

        public void          receive(Message msg) {
            list.add((Integer)msg.getObject());
            System.out.println("<-- " + msg.getObject());
        }
    }

}

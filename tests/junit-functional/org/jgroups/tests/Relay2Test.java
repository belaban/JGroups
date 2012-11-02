package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FORWARD_TO_COORD;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST2;
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
    protected JChannel x, y, z;  // members in site "sfo

    protected static final String BRIDGE_CLUSTER = "global";
    protected static final String LON_CLUSTER    = "lon-cluster";
    protected static final String SFO_CLUSTER    = "sfo-cluster";
    protected static final String SFO            = "sfo", LON="lon";

    @AfterMethod protected void destroy() {Util.close(z,y,x,c,b,a);}


    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.jboss.org/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge() throws Exception {
        a=createNode(LON, "A", LON_CLUSTER, null);
        b=createNode(LON, "B", LON_CLUSTER, null);
        Util.waitUntilAllChannelsHaveSameSize(30000, 500, a,b);

        x=createNode(SFO, "X", SFO_CLUSTER, null);
        assert x.getView().size() == 1;

        RELAY2 ar=(RELAY2)a.getProtocolStack().findProtocol(RELAY2.class),
          xr=(RELAY2)x.getProtocolStack().findProtocol(RELAY2.class);

        assert ar != null && xr != null;

        JChannel a_bridge=null, x_bridge=null;
        for(int i=0; i < 20; i++) {
            a_bridge=ar.getBridge(SFO);
            x_bridge=xr.getBridge(LON);
            if(a_bridge != null && x_bridge != null && a_bridge.getView().size() == 2 && x_bridge.getView().size() == 2)
                break;
            Util.sleep(500);
        }

        assert a_bridge != null && x_bridge != null;

        System.out.println("A's bridge channel: " + a_bridge.getView());
        System.out.println("X's bridge channel: " + x_bridge.getView());
        assert a_bridge.getView().size() == 2 : "bridge view is " + a_bridge.getView();
        assert x_bridge.getView().size() == 2 : "bridge view is " + x_bridge.getView();

        Relayer.Route route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;

        // Now inject a partition into site LON
        System.out.println("Creating partition between A and B:");
        createPartition(a, b);

        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());
        assert a.getView().size() == 1 && b.getView().size() == 1;

        route=getRoute(x, LON);
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

        Util.waitUntilAllChannelsHaveSameSize(20000, 500, a, b);
        System.out.println("A's view: " + a.getView() + "\nB's view: " + b.getView());

        for(int i=0; i < 20; i++) {
            bridge_view=xr.getBridgeView(BRIDGE_CLUSTER);
            if(bridge_view != null && bridge_view.size() == 2)
                break;
            Util.sleep(500);
        }

        route=getRoute(x, LON);
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;
    }


    /**
     * Tests whether the bridge channel connects and disconnects ok.
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
     */
    public void testDisconnectAndReconnect() throws Exception {
        a=createNode(LON, "A", LON_CLUSTER, null);
        x=createNode(SFO, "X", SFO_CLUSTER, null);

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
        a=createNode(LON, "A", LON_CLUSTER, null);
        x=createNode(SFO, "X", SFO_CLUSTER, rx);

        System.out.println("A: waiting for site SFO to be UP");
        waitUntilStatus(SFO, RELAY2.RouteStatus.UP, 20000, 500, a);

        Address sm_sfo=new SiteMaster(SFO);
        System.out.println("A: sending message 0 to the site master of SFO");
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
        waitUntilStatus(SFO, RELAY2.RouteStatus.UNKNOWN, 20000, 500, a);

        System.out.println("A: sending 5 messages to site SFO - they should all get queued");
        for(int i=1; i <= 5; i++)
            a.send(sm_sfo, i);

        System.out.println("Starting X again; the queued messages should now get re-sent from A to X");
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


    /**
     * Tests the following scenario:
     * <ul>
     *     <li>Nodes A in LON and B in SFO, both are up</li>
     *     <li>B goes down</li>
     *     <li>The status of site SFO in LON is set to UNKNOWN and a task T is started which will set SFO's status
     *     to DOWN in site_down_timeout ms</li>
     *     <li>Before T kicks in, B in SFO is started again</li>
     *     <li>The status of site SFO in LON is now UP</li>
     *     <li>Make sure T is cancelled when transitioning from UNKNOWN to UP, or else it'll set the status
     *     of SFO to DOWN when it triggers</li>
     * </ul>
     */
    public void testUnknownAndUpStateTransitions() throws Exception {
        a=createNode(LON, "A", LON_CLUSTER, null);
        x=createNode(SFO, "X", SFO_CLUSTER, null);
        waitForBridgeView(2, 20000, 500, a, x);

        RELAY2 ra=(RELAY2)a.getProtocolStack().findProtocol(RELAY2.class);
        ra.siteDownTimeout(3000);

        System.out.println("Disconnecting X");
        x.disconnect();
        System.out.println("A: waiting for site SFO to be UNKNOWN");
        waitUntilStatus(SFO, RELAY2.RouteStatus.UNKNOWN, 20000, 500, a);

        System.out.println("Reconnecting X, waiting for 5 seconds to see if the route is marked as DOWN");
        x.connect(SFO_CLUSTER);
        Util.sleep(5000);
        Relayer.Route route=getRoute(a, SFO);
        assert route != null && route.status() == RELAY2.RouteStatus.UP : "route is " + route + " (expected to be UP)";

        route=getRoute(x, LON);
        assert route != null && route.status() == RELAY2.RouteStatus.UP : "route is " + route + " (expected to be UP)";
    }



    protected JChannel createNode(String site_name, String node_name, String cluster_name,
                                  Receiver receiver) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new PING().setValue("timeout", 300).setValue("num_initial_members", 2),
                                 new NAKACK2(),
                                 new UNICAST2(),
                                 new GMS().setValue("print_local_addr", false),
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
        RELAY2 relay=new RELAY2().site(site_name).enableAddressTagging(false).asyncRelayCreation(true);

        RelayConfig.SiteConfig lon_cfg=new RelayConfig.SiteConfig(LON, (short)0),
          sfo_cfg=new RelayConfig.SiteConfig(SFO, (short)1);

        lon_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));
        sfo_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig(BRIDGE_CLUSTER, createBridgeStack()));
        relay.addSite(LON, lon_cfg).addSite(SFO, sfo_cfg);
        return relay;
    }

    protected static Protocol[] createBridgeStack() {
        return new Protocol[]{
          new SHARED_LOOPBACK(),
          new PING().setValue("timeout", 500).setValue("num_initial_members", 2),
          new NAKACK2(),
          new UNICAST2(),
          new GMS().setValue("print_local_addr", false)
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


    protected void waitUntilStatus(String site_name, RELAY2.RouteStatus expected_status,
                                   long timeout, long interval, JChannel ch) throws Exception {
        RELAY2 relay=(RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class);
        if(relay == null)
            throw new IllegalArgumentException("Protocol RELAY2 not found");
        Relayer.Route route=null;
        long deadline=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < deadline) {
            route=relay.getRoute(site_name);
            if(route != null && route.status() == expected_status)
                break;
            Util.sleep(interval);
        }
        assert route.status() == expected_status : "status=" + (route != null? route.status() : "n/a")
          + ", expected status=" + expected_status;
    }

    protected Relayer.Route getRoute(JChannel ch, String site_name) {
        RELAY2 relay=(RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class);
        return relay.getRoute(site_name);
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

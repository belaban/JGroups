package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.protocols.FORWARD_TO_COORD;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.SHARED_LOOPBACK;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.Relayer;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Various RELAY2-related tests
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class Relay2Test {
    protected JChannel a, b, c;  // members in site "lon"
    protected JChannel x, y, z; // members in site "sfo"

    @AfterMethod protected void destroy() {Util.close(z,y,x,c,b,a);}


    /**
     * Tests that routes are correctly registered after a partition and a subsequent merge
     * (https://issues.jboss.org/browse/JGRP-1524)
     */
    public void testMissingRouteAfterMerge() throws Exception {
        a=createNode("lon", "A", "london-cluster");
        b=createNode("lon", "B", "london-cluster");
        Util.waitUntilAllChannelsHaveSameSize(30000, 500, a,b);

        x=createNode("sfo", "X", "sfo-cluster");
        assert x.getView().size() == 1;

        RELAY2 ar=(RELAY2)a.getProtocolStack().findProtocol(RELAY2.class),
          xr=(RELAY2)x.getProtocolStack().findProtocol(RELAY2.class);

        assert ar != null && xr != null;

        JChannel a_bridge=ar.getBridge("sfo");
        JChannel x_bridge=xr.getBridge("lon");
        assert a_bridge != null && x_bridge != null;

        for(int i=0; i < 20; i++) {
            if(a_bridge.getView().size() == 2 && x_bridge.getView().size() == 2)
                break;
            Util.sleep(500);
        }

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

        route=xr.getRoute("lon");
        System.out.println("Route at sfo to lon: " + route);
        assert route != null;
    }



    protected JChannel createNode(String site_name, String node_name, String cluster_name) throws Exception {
        JChannel ch=new JChannel(new SHARED_LOOPBACK(),
                                 new PING().setValue("timeout", 300).setValue("num_initial_members", 2),
                                 new NAKACK2(),
                                 new UNICAST2(),
                                 new GMS(),
                                 new FORWARD_TO_COORD().setValue("resend_delay", 500),
                                 createRELAY2(site_name));
        ch.setName(node_name);
        ch.connect(cluster_name);
        return ch;
    }


    protected RELAY2 createRELAY2(String site_name) {
        RELAY2 relay=new RELAY2().site(site_name).enableAddressTagging(false);

        RelayConfig.SiteConfig lon_cfg=new RelayConfig.SiteConfig("lon", (short)0),
          sfo_cfg=new RelayConfig.SiteConfig("sfo", (short)1);

        lon_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig("global", createBridgeStack()));
        sfo_cfg.addBridge(new RelayConfig.ProgrammaticBridgeConfig("global", createBridgeStack()));

        relay.addSite("lon", lon_cfg).addSite("sfo", sfo_cfg);
        return relay;
    }

    protected static Protocol[] createBridgeStack() {
        return new Protocol[]{new SHARED_LOOPBACK(),
          new PING().setValue("timeout", 300).setValue("num_initial_members", 2),
          new NAKACK2(),
          new UNICAST2(),
          new GMS()};
    }

    /** Creates a singleton view for each channel listed and injects it */
    protected static void createPartition(JChannel ... channels) {
        for(JChannel ch: channels) {
            View view=Util.createView(ch.getAddress(), 5, ch.getAddress());
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

}

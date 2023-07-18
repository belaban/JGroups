package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.LOCAL_PING;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.Route;
import org.jgroups.protocols.relay.RouteStatusListener;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Common functionality for relay tests
 * @author Bela Ban
 * @since  5.2.17
 */
public class RelayTests {
    protected final Log                log=LogFactory.getLog(getClass());
    protected static final InetAddress LOOPBACK;

    static {
        LOOPBACK=InetAddress.getLoopbackAddress();
    }


    protected static Protocol[] defaultStack(Protocol ... additional_protocols) {
        Protocol[] protocols={
          new TCP().setBindAddress(LOOPBACK),
          new LOCAL_PING(),
          new MERGE3().setMaxInterval(3000).setMinInterval(1000),
          new NAKACK2().useMcastXmit(false),
          new UNICAST3(),
          new GMS().printLocalAddress(false)
        };
        if(additional_protocols == null)
            return protocols;
        Protocol[] tmp=Arrays.copyOf(protocols, protocols.length + additional_protocols.length);
        System.arraycopy(additional_protocols, 0, tmp, protocols.length, additional_protocols.length);
        return tmp;
    }

    /**
     * Creates a node in a local site that connects to all given sites
     * @param site The local site
     * @param name The name of the node
     * @param bridge The name of the bridge cluster
     * @param sites The sites to which this site connects
     */
    protected static JChannel createNode(String site, String name, String bridge, String ... sites) throws Exception {
        RELAY2 relay=createSymmetricRELAY2(site, bridge, sites);
        JChannel ch=new JChannel(defaultStack(relay)).name(name);
        if(site != null)
            ch.connect(site);
        return ch;
    }

    /** Creates a symmetric scenario for local_site, where all sites are connected via the bridge cluster */
    protected static RELAY2 createSymmetricRELAY2(String local_site, String bridge, String ... sites)
      throws UnknownHostException {
        RELAY2 relay=new RELAY2().site(local_site).asyncRelayCreation(false);
        for(String site: sites) {
            RelayConfig.SiteConfig cfg=new RelayConfig.SiteConfig(site)
              .addBridge(new RelayConfig.ProgrammaticBridgeConfig(bridge, defaultStack()));
            relay.addSite(site, cfg);
        }
        return relay;
    }

    protected static void waitUntilRoute(String site_name, boolean present,
                                         long timeout, long interval, JChannel ch) throws Exception {
        RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
        if(relay == null)
            throw new IllegalArgumentException("Protocol RELAY2 not found");

        Util.waitUntil(timeout, interval, () -> {
            Route route=relay.getRoute(site_name);
             return ((route != null && present) || (route == null && !present));
        });
    }

    protected static Route getRoute(JChannel ch, String site_name) {
        RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
        return relay.getRoute(site_name);
    }

    /** Creates a singleton view for each channel listed and injects it */
    protected static void injectSingletonPartitions(JChannel ... channels) {
        for(JChannel ch: channels) {
            View view=View.create(ch.getAddress(), ch.getView().getViewId().getId()+1, ch.getAddress());
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(view);
        }
    }

    protected static void waitForBridgeView(int expected_size, long timeout, long interval, String cluster,
                                            JChannel... channels) {
        Util.waitUntilTrue(timeout, interval, () -> Stream.of(channels)
          .map(ch -> (RELAY2)ch.getProtocolStack().findProtocol(RELAY2.class))
          .map(r -> r.getBridgeView(cluster)).allMatch(v -> v != null && v.size() == expected_size));

        System.out.println("Bridge views:\n");
        for(JChannel ch: channels) {
            RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(cluster);
            System.out.println(ch.getAddress() + ": " + bridge_view);
        }

        for(JChannel ch: channels) {
            RELAY2 relay=ch.getProtocolStack().findProtocol(RELAY2.class);
            View bridge_view=relay.getBridgeView(cluster);
            assert bridge_view != null && bridge_view.size() == expected_size
              : ch.getAddress() + ": bridge view=" + bridge_view + ", expected=" + expected_size;
        }
    }

    protected static class MyRouteStatusListener implements RouteStatusListener {
        protected final Address      local_addr;
        protected final List<String> up=new ArrayList<>(), down=new ArrayList<>();
        protected boolean            verbose;

        protected MyRouteStatusListener(Address local_addr) {
            this.local_addr=local_addr;
        }

        protected List<String>          up()               {return up;}
        protected List<String>          down()             {return down;}
        protected MyRouteStatusListener verbose(boolean b) {this.verbose=b; return this;}
        protected boolean               verbose()          {return verbose;}

        @Override public synchronized void sitesUp(String... sites) {
            if(verbose)
                System.out.printf("%s: UP(%s)\n", local_addr, Arrays.toString(sites));
            up.addAll(Arrays.asList(sites));
        }

        @Override public synchronized void sitesDown(String... sites) {
            if(verbose)
                System.out.printf("%s: DOWN(%s)\n", local_addr, Arrays.toString(sites));
            down.addAll(Arrays.asList(sites));
        }

        protected synchronized MyRouteStatusListener clear() {up.clear(); down.clear(); return this;}

        @Override
        public String toString() {
            return String.format("down: %s, up: %s", down, up);
        }
    }

}

package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer {
    protected final RelayConfig.SiteConfig site_config;

    /** The routing table. Site IDs are used as indices, e.g. the route for site=2 is at index 2. */
    protected Route[]                      routes;

    /** The bridges which are used to connect to different sites */
    protected final List<Bridge>           bridges;

    protected final short                  my_site_id;

    /** The name of the local channel; we'll prepend "sitemaster_" to it */
    protected final String                 local_name;


    public Relayer(final int num_routes, RelayConfig.SiteConfig site_config, String local_name) {
        this.site_config=site_config;
        my_site_id=site_config.getId();
        routes=new Route[num_routes];
        bridges=new ArrayList<Bridge>(num_routes);
        this.local_name=local_name;
    }

    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     */
    // todo: parallelize
    public void start() throws Throwable {
        try {
            for(RelayConfig.BridgeConfig bridge_config: site_config.getBridges()) {
                Bridge bridge=new Bridge(bridge_config.getConfig(), bridge_config.getName(), local_name,
                                         new AddressGenerator() {
                                             public Address generateAddress() {
                                                 UUID uuid=UUID.randomUUID();
                                                 return new SiteUUID(uuid, my_site_id);
                                             }
                                         });
                bridges.add(bridge);
            }
        }
        catch(Throwable t) {
            stop();
            throw t;
        }

        try {
            for(Bridge bridge: bridges)
                bridge.start();
        }
        catch(Throwable t) {
            stop();
            throw t;
        }
    }

    /**
     * Disconnects and destroys all bridges
     */
    public void stop() {
        for(Bridge bridge: bridges)
            bridge.stop();
    }


    public String printRoutes() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < routes.length; i++) {
            Route route=routes[i];
            if(route == null)
                break;
            sb.append(i + " --> " + route + "\n");
        }

        return sb.toString();
    }


    protected void addRoute(short site, Route route) {
        Route old_route;
        if((old_route=routes[site]) == null)
            setRoute(site, route);
        else if(!old_route.site_master.equals(route.site_master) || old_route.bridge != route.bridge)
            setRoute(site, route);
    }

    protected void setRoute(short site, Route route) {
        if(site >= routes.length-1) {
            Route[] tmp_routes=new Route[routes.length * 2];
            System.arraycopy(routes, 0, tmp_routes, 0, routes.length);
            routes=tmp_routes;
        }
        routes[site]=route;
    }


    /**
     * Includes information about the site master of the route and the channel to be used
     */
    protected static class Route {
        protected final Address  site_master;
        protected final JChannel bridge;

        public Route(Address site_master, JChannel bridge) {
            this.site_master=site_master;
            this.bridge=bridge;
        }

        public String toString() {
            return site_master.toString();
        }
    }


    protected class Bridge extends ReceiverAdapter {
        protected JChannel     channel;
        protected final String cluster_name;

        protected Bridge(final String config, final String cluster_name, String channel_name, AddressGenerator addr_generator) throws Exception {
            channel=new JChannel(config);
            channel.setName(channel_name);
            channel.setReceiver(this);
            channel.setAddressGenerator(addr_generator);
            this.cluster_name=cluster_name;
        }

        protected void start() throws Exception {
            channel.connect(cluster_name);
        }

        protected void stop() {
            Util.close(channel);
        }

        public void receive(Message msg) {

        }

        public void viewAccepted(View view) {
            System.out.println("-- view=" + view);
        }
    }
}

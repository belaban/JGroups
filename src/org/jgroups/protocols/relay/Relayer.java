package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.RELAY;
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

    protected final Log                    log;

    protected final RELAY2                 relay;


    public Relayer(RelayConfig.SiteConfig site_config, String local_name, Log log, RELAY2 relay) {
        this.site_config=site_config;
        this.relay=relay;
        int num_routes=site_config.getBridges().size();
        my_site_id=site_config.getId();
        routes=new Route[num_routes];
        bridges=new ArrayList<Bridge>(num_routes);
        this.local_name=local_name;
        this.log=log;
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
            if(route != null) {
                String name=SiteUUID.getSiteName((short)i);
                if(name == null)
                    name=String.valueOf(i);
                sb.append(name + " --> " + route + "\n");
            }
        }

        return sb.toString();
    }


    protected void addRoute(short site, Route route) {
        Route old_route;
        ensureCapacity(site);
        if((old_route=routes[site]) == null)
            routes[site]=route;
        else if(!old_route.site_master.equals(route.site_master) || old_route.bridge != route.bridge)
            routes[site]=route;
    }

    protected Route removeRoute(short site) {
        if(site <= routes.length -1) {
            Route route=routes[site];
            routes[site]=null;
            return route;
        }
        return null;
    }

    protected Route getRoute(short site) {
        if(site <= routes.length -1)
            return routes[site];
        return null;
    }

    protected void ensureCapacity(short site) {
        if(site >= routes.length-1) {
            Route[] tmp_routes=new Route[routes.length * 2];
            System.arraycopy(routes, 0, tmp_routes, 0, routes.length);
            routes=tmp_routes;
        }
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
        protected View         view;

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
            RELAY2.Relay2Header hdr=(RELAY2.Relay2Header)msg.getHeader(relay.getId());
            if(hdr == null) {
                log.warn("received a message without a relay header; discarding it");
                return;
            }
            relay.handleMessage(hdr, msg);
        }

        public void viewAccepted(View view) {
            List<Address> left_mbrs=this.view != null? Util.determineLeftMembers(this.view.getMembers(),view.getMembers()) : null;
            this.view=view;

            if(log.isTraceEnabled())
                log.trace("[Relayer " + channel.getAddress() + "] view: " + view);

            for(Address addr: view.getMembers()) {
                if(addr instanceof SiteUUID) {
                    SiteUUID site_uuid=(SiteUUID)addr;
                    short site=site_uuid.getSite();
                    Route route=new Route(site_uuid, channel);
                    addRoute(site, route);
                }
            }

            if(left_mbrs != null) {
                for(Address addr: left_mbrs) {
                    if(addr instanceof SiteUUID) {
                        SiteUUID site_uuid=(SiteUUID)addr;
                        short site=site_uuid.getSite();
                        removeRoute(site);
                    }
                }
            }
        }
    }
}

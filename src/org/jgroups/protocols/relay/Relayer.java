package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
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
    /** The routing table. Site IDs are used as indices, e.g. the route for site=2 is at index 2. */
    protected Route[]                      routes;

    /** The bridges which are used to connect to different sites */
    protected List<Bridge>                 bridges;

    protected final Log                    log;

    protected final RELAY2                 relay;


    public Relayer(RELAY2 relay, Log log) {
        this.relay=relay;
        this.log=log;
    }

    
    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param bridge_configs A list of bridge configurations
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
     * @throws Throwable
     */
    public void start(List<RelayConfig.BridgeConfig> bridge_configs, String bridge_name, final short my_site_id)
      throws Throwable {
        routes=new Route[bridge_configs.size()];
        bridges=new ArrayList<Bridge>(bridge_configs.size());
        try {
            for(RelayConfig.BridgeConfig bridge_config: bridge_configs) {
                Bridge bridge=new Bridge(bridge_config.createChannel(), bridge_config.getClusterName(), bridge_name,
                                         new AddressGenerator() {
                                             public Address generateAddress() {
                                                 UUID uuid=UUID.randomUUID();
                                                 return new SiteUUID(uuid, null, my_site_id);
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


    public synchronized String printRoutes() {
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


    protected synchronized void addRoute(short site, Route route) {
        Route old_route;
        ensureCapacity(site);
        if((old_route=routes[site]) == null)
            _addRoute(site, route);
        else if(!old_route.site_master.equals(route.site_master) || old_route.bridge != route.bridge)
            _addRoute(site, route);
    }

    protected void _addRoute(short site, Route route) {
        if(log.isTraceEnabled())
            log.trace("added site " + SiteUUID.getSiteName(site));
        routes[site]=route;
    }

    protected synchronized Route removeRoute(short site) {
        if(site <= routes.length -1) {
            Route route=routes[site];
            routes[site]=null;
            if(log.isTraceEnabled())
                log.trace("removed route " + SiteUUID.getSiteName(site));
            return route;
        }
        return null;
    }

    protected synchronized Route getRoute(short site) {
        if(site <= routes.length -1)
            return routes[site];
        return null;
    }

    protected synchronized List<Route> getRoutes(short ... excluded_sites) {
        List<Route> retval=new ArrayList<Route>(routes.length);
        for(short i=0; i < routes.length; i++) {
            Route tmp=routes[i];
            if(tmp != null) {
                if(!isExcluded(tmp, excluded_sites))
                    retval.add(tmp);
            }
        }

        return retval;
    }

    protected static boolean isExcluded(Route route, short... excluded_sites) {
        if(excluded_sites == null)
            return false;
        short site=((SiteUUID)route.site_master).getSite();
        for(short excluded_site: excluded_sites)
            if(site == excluded_site)
                return true;
        return false;
    }

    protected synchronized void ensureCapacity(short site) {
        if(site >= routes.length) {
            Route[] tmp_routes=new Route[Math.max(site+1, routes.length * 2)];
            System.arraycopy(routes, 0, tmp_routes, 0, routes.length);
            routes=tmp_routes;
        }
    }



    /**
     * Includes information about the site master of the route and the channel to be used
     */
    public static class Route {
        protected final Address  site_master;
        protected final JChannel bridge;

        public Route(Address site_master, JChannel bridge) {
            this.site_master=site_master;
            this.bridge=bridge;
        }

        public JChannel getBridge() {return bridge;}

        public Address getSiteMaster() {return site_master;}

        public String toString() {
            return site_master.toString();
        }
    }


    protected class Bridge extends ReceiverAdapter {
        protected JChannel     channel;
        protected final String cluster_name;
        protected View         view;

        protected Bridge(final JChannel ch, final String cluster_name, String channel_name, AddressGenerator addr_generator) throws Exception {
            this.channel=ch;
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
            relay.handleRelayMessage(hdr, msg);
        }

        public void viewAccepted(View new_view) {
            List<Address> left_mbrs=this.view != null? Util.determineLeftMembers(this.view.getMembers(),new_view.getMembers()) : null;
            this.view=new_view;

            if(log.isTraceEnabled())
                log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);

            if(left_mbrs != null) {
                for(Address addr: left_mbrs) {
                    if(addr instanceof SiteUUID) {
                        SiteUUID site_uuid=(SiteUUID)addr;
                        short site=site_uuid.getSite();
                        removeRoute(site);
                    }
                }
            }

            for(Address addr: new_view.getMembers()) {
                if(addr instanceof SiteUUID) {
                    SiteUUID site_uuid=(SiteUUID)addr;
                    short site=site_uuid.getSite();
                    Route route=new Route(site_uuid, channel);
                    addRoute(site, route);
                }
            }
        }
    }
}

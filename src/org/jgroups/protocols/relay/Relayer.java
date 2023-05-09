package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maintains bridges and routing table. Does the routing of outgoing messages and dispatches incoming messages to
 * the right members.<p/>
 * A Relayer cannot be reused once it is stopped, but a new Relayer instance must be created.
 * @author Bela Ban
 * @since 3.2
 */
public class Relayer {
    /** The routing table. Site IDs are the keys (e.g. "sfo", and list of routes are the values */
    protected final Map<String,List<Route>>     routes=new ConcurrentHashMap<>(5);

    protected final Set<ForwardingRoute>        forward_routes=new ConcurrentSkipListSet<>();

    /** The bridges which are used to connect to different sites */
    protected final Collection<Bridge>          bridges=new ConcurrentLinkedQueue<>();

    protected final Log                         log;

    protected final RELAY2                      relay;

    /** Flag set when stop() is called. Since a Relayer should not be used after stop() has been called, a new
     * instance needs to be created */
    protected volatile boolean                  done;
    protected boolean                           stats;


    public Relayer(RELAY2 relay, Log log) {
        this.relay=relay;
        this.stats=relay.statsEnabled();
        this.log=log;
    }


    public boolean done() {return done;}

    
    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param site_cfg The SiteConfiguration
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
     * @throws Throwable
     */
    public void start(RelayConfig.SiteConfig site_cfg, String bridge_name, final String my_site_id) throws Throwable {
        if(done) {
            log.trace(relay.getAddress() + ": will not start the Relayer as stop() has been called");
            return;
        }
        try {
            // Add configured forward routes:
            List<RelayConfig.ForwardConfig> forward_configs=site_cfg.getForwards();
            for(RelayConfig.ForwardConfig cfg: forward_configs) {
                ForwardingRoute fr=new ForwardingRoute(cfg.to(), cfg.gateway());
                this.forward_routes.add(fr);
            }

            // Add configured bridges
            List<RelayConfig.BridgeConfig> bridge_configs=site_cfg.getBridges();
            for(RelayConfig.BridgeConfig cfg: bridge_configs) {
                Bridge bridge=new Bridge(cfg.createChannel(), cfg.getClusterName(), cfg.getTo(), bridge_name,
                                         () -> new SiteUUID(UUID.randomUUID(), null, my_site_id));
                bridges.add(bridge);
            }
            for(Bridge bridge: bridges)
                bridge.start();
        }
        catch(Throwable t) {
            stop();
            throw t;
        }
        finally {
            if(done) {
                log.trace(relay.getAddress() + ": stop() was called while starting the relayer; stopping the relayer now");
                stop();
            }
        }
    }


    /** Disconnects and destroys all bridges */
    public void stop() {
        done=true;
        bridges.forEach(Bridge::stop);
        bridges.clear();
    }


    public synchronized String printRoutes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,List<Route>> entry: routes.entrySet()) {
            List<Route> list=entry.getValue();
            if(list != null && !list.isEmpty())
                sb.append(entry.getKey() + " --> ").append(Util.print(list)).append("\n");
        }
        for(ForwardingRoute fr: this.forward_routes) {
            sb.append(String.format("%s --> %s [fwd]\n", fr.to(), fr.gateway()));
        }
        return sb.toString();
    }


    protected Route getRoute(String site) { return getRoute(site, null);}

    protected synchronized Route getRoute(String site, Address sender) {
        List<Route> list=routes.get(site);
        if(list == null)
            return null;
        if(list.size() == 1)
            return list.get(0);

        return relay.site_master_picker.pickRoute(site, list, sender);
    }

    /** Returns a Route matching any of the ForwardingRoutes, or null if none matches */
    protected synchronized Route getForwardingRouteMatching(String site, Address sender) {
        if(site == null)
            return null;
        for(ForwardingRoute fr: forward_routes) {
            if(fr.matches(site)) {
                Route r=getRoute(fr.gateway(), sender);
                if(r != null)
                    return r;
            }
        }
        return null;
    }

    protected List<String> getSiteNames() {
        return new ArrayList<>(routes.keySet());
    }

    protected synchronized List<Route> getRoutes(String ... excluded_sites) {
        List<Route> retval=new ArrayList<>(routes.size());
        for(List<Route> list: routes.values()) {
            for(Route route: list) {
                if(route != null && !isExcluded(route, excluded_sites)) {
                    retval.add(route);
                    break;
                }
            }
        }
        return retval;
    }

    protected View getBridgeView(String cluster_name) {
        if(cluster_name == null || bridges == null)
            return null;
        for(Bridge bridge: bridges) {
            if(Objects.equals(bridge.cluster_name, cluster_name))
                return bridge.view;
        }
        return null;
    }

    protected static boolean isExcluded(Route route, String... excluded_sites) {
        if(excluded_sites == null)
            return false;
        String site=((SiteUUID)route.site_master).getSite();
        for(String excluded_site: excluded_sites)
            if(site.equals(excluded_site))
                return true;
        return false;
    }






    protected class Bridge implements Receiver {
        protected JChannel channel;
        protected String   cluster_name;
        protected String   to;
        protected View     view;

        protected Bridge(final JChannel ch, final String cluster_name, String to, String channel_name,
                         AddressGenerator addr_generator) throws Exception {
            this.channel=ch;
            channel.setName(channel_name);
            channel.setReceiver(this);
            channel.addAddressGenerator(addr_generator);
            this.cluster_name=cluster_name;
            this.to=to;
        }

        protected void start() throws Exception {
            channel.connect(cluster_name);
            log.info("%s: joined bridge cluster '%s'", channel.getAddress(), cluster_name);
        }

        protected void stop() {
            log.info("%s: leaving bridge cluster '%s'", channel.getAddress(), channel.getClusterName());
            Util.close(channel);
        }

        public void receive(Message msg) {
            RELAY2.Relay2Header hdr=msg.getHeader(relay.getId());
            if(hdr == null) {
                log.warn("received a message without a relay header; discarding it");
                return;
            }
            switch(hdr.type) {
                case RELAY2.Relay2Header.TOPO_REQ:
                    RELAY2.Relay2Header rsp_hdr=new RELAY2.Relay2Header(RELAY2.Relay2Header.TOPO_RSP)
                      .setSites(relay.printLocalTopology());
                    Message topo_rsp=new EmptyMessage(msg.src()).putHeader(relay.getId(), rsp_hdr);
                    try {
                        channel.send(topo_rsp);
                    }
                    catch(Exception e) {
                        log.warn("%s: failed sending TOPO-RSP message to %s: %s", channel.getAddress(), msg.src(), e);
                    }
                    return; // not relayed
                case RELAY2.Relay2Header.TOPO_RSP:
                    String[] sites=hdr.getSites();
                    if(sites != null && sites.length > 0 && sites[0] != null)
                        relay.topo_collector.add(msg.src(), sites[0]);
                    return;
            }
            relay.handleRelayMessage(hdr, msg);
        }

        /** The view contains a list of SiteUUIDs. Adjust the routing table based on the SiteUUIDs and site */
        public void viewAccepted(View new_view) {
            View                      old_view=this.view;
            Map<String,List<Address>> sites=Util.getSites(new_view, relay.site());
            List<String>              removed_routes=removedRoutes(old_view, new_view);
            Set<String>               up=new HashSet<>(), down=new HashSet<>(removed_routes);

            this.view=new_view;
            // add new routes to routing table:
            for(String r: sites.keySet()) {
                if(!routes.containsKey(r))
                    up.add(r);
            }
            log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);
            for(Map.Entry<String,List<Address>> entry: sites.entrySet()) {
                String        key=entry.getKey();
                List<Address> val=entry.getValue();
                List<Route>   existing=routes.get(key);
                List<Route>   newRoutes=existing != null? new ArrayList<>(existing) : new ArrayList<>();

                // Remove routes not in the view anymore:
                newRoutes.removeIf(r -> !val.contains(r.siteMaster()));

                // Add routes that aren't yet in the routing table:
                val.stream().filter(addr -> !contains(newRoutes, addr))
                  .forEach(addr -> newRoutes.add(new Route(addr, channel, relay, log).stats(stats)));

                if(newRoutes.isEmpty()) {
                    routes.remove(key);
                    down.add(key);
                }
                else
                    routes.put(key, newRoutes);
            }

            // remove all routes which were dropped between the old and new view:
            if(!removed_routes.isEmpty() && log.isTraceEnabled())
                log.trace("%s: removing routes %s from routing table", removed_routes);
            removed_routes.forEach(routes.keySet()::remove);

            if(!down.isEmpty())
                relay.sitesChange(true, down.toArray(new String[0]));
            if(!up.isEmpty())
                relay.sitesChange(false, up.toArray(new String[0]));
        }

        @Override
        public String toString() {
            return String.format("bridge to %s [cluster: %s]", to, cluster_name);
        }

        protected boolean contains(List<Route> routes, Address addr) {
            return routes.stream().anyMatch(route -> route.siteMaster().equals(addr));
        }

        /** Returns a list of routes that were in old_view, but are no longer in new_view */
        protected List<String> removedRoutes(View old_view, View new_view) {
            List<String> l=new ArrayList<>();
            if(old_view == null)
                return l;
            List<String> old_routes=Stream.of(old_view.getMembersRaw()).filter(a -> a instanceof SiteUUID)
              .map(s -> ((SiteUUID)s).getSite()).collect(Collectors.toList());
            List<String> new_routes=Stream.of(new_view.getMembersRaw()).filter(a -> a instanceof SiteUUID)
              .map(s -> ((SiteUUID)s).getSite()).collect(Collectors.toList());
            old_routes.removeAll(new_routes);
            return old_routes;
        }


    }
}

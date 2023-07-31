package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.*;
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
public class Relayer3 extends Relayer {
    protected final Set<ForwardingRoute>    forward_routes=new ConcurrentSkipListSet<>();
    /** The bridges which are used to connect to different sites */
    protected final Collection<Bridge>      bridges=new ConcurrentLinkedQueue<>();


    public Relayer3(RELAY relay, Log log) {
        super(relay, log);
    }


    /**
     * Creates all bridges from site_config and connects them (joining the bridge clusters)
     * @param site_cfg The SiteConfiguration
     * @param bridge_name The name of the local bridge channel, prefixed with '_'.
     * @param my_site_id The ID of this site
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
                Bridge bridge=new Bridge(cfg.createChannel(), this, cfg.getClusterName(), bridge_name,
                                         new AddressGenerator() {
                                             @Override
                                             public Address generateAddress() {return generateAddress(null);}
                                             @Override
                                             public Address generateAddress(String name) {
                                                 return new SiteUUID(UUID.randomUUID(), name, my_site_id);
                                             }
                                         });
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
            if(done)
                stop();
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

    protected synchronized void addRoutes(String site, List<Route> new_routes) {
        this.routes.put(site, new_routes);
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

    protected synchronized boolean containsRoute(String r) {
        return routes.containsKey(r);
    }

    protected synchronized void removeRoute(String site) {
        routes.remove(site);
    }

    protected List<String> getSiteNames() {
        return Stream.concat(Stream.of(relay.site()), routes.keySet().stream())
                .collect(Collectors.toList());
    }

    /** Gets routes for a given site. The result is the real value, which can be modified, so make a copy! */
    protected synchronized List<Route> getRoutes(String site) {
        return routes.get(site);
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


}

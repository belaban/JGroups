package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for all relayers
 * @author Bela Ban
 * @since  5.2.17
 */

public abstract class Relayer {
    protected final RELAY                   relay;
    protected final Log                     log;
    /** Flag set when stop() is called. Since a Relayer should not be used after stop() has been called, a new
     * instance needs to be created */
    protected volatile boolean              done;
    protected boolean                       stats;
    /** The routing table. Site IDs are the keys (e.g. "sfo", and list of routes are the values */
    protected final Map<String,List<Route>> routes=new ConcurrentHashMap<>(5);
    protected final Set<ForwardingRoute>    forward_routes=new ConcurrentSkipListSet<>();

    public Relayer(RELAY relay, Log log) {
        this.relay=relay;
        this.log=log;
        this.stats=relay.statsEnabled();
    }

    public abstract void         stop();
    public RELAY                 relay() {return relay;}
    public Log                   log()   {return log;}
    public boolean               done()  {return done;}
    protected abstract View      getBridgeView(String cluster_name);
    protected Route              getRoute(String site) { return getRoute(site, null);}

    protected synchronized Route getRoute(String site, Address sender) {
        List<Route> list=routes.get(site);
        if(list == null)
            return null;
        if(list.size() == 1)
            return list.get(0);
        return relay.site_master_picker.pickRoute(site, list, sender);
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

    protected synchronized void addRoutes(String site, List<Route> new_routes) {
        this.routes.put(site, new_routes);
    }

    protected synchronized boolean containsRoute(String r) {
        return routes.containsKey(r);
    }

    protected synchronized void removeRoute(String site) {
        routes.remove(site);
    }

    /** Gets routes for a given site. The result is the real value, which can be modified, so make a copy! */
    protected synchronized List<Route> getRoutes(String site) {
        return routes.get(site);
    }

    protected synchronized String printRoutes() {
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
        return Stream.concat(Stream.of(relay.site()), routes.keySet().stream())
          .collect(Collectors.toList());
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

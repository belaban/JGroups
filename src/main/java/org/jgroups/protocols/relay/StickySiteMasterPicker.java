package org.jgroups.protocols.relay;

import org.jgroups.Address;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Picks a local site master or a route based on the sender and caches the decision. Next time, the same local site
 * master or route will be returned. If no route exists yet, a random local site master or route is picked.
 * @author Bela Ban
 * @since  5.3.1
 */
public class StickySiteMasterPicker extends RandomSiteMasterPicker {
    protected Map<Address,Address> local_sm_cache=new ConcurrentHashMap<>();
    protected Map<Address,Route>   route_cache=new ConcurrentHashMap<>();

    @Override
    public Address pickSiteMaster(List<Address> site_masters, Address original_sender) {
        if(original_sender == null)
            return super.pickSiteMaster(site_masters, original_sender);
        Address local_sm=local_sm_cache.get(original_sender);
        if(local_sm != null) {
            if(site_masters.contains(local_sm))
                return local_sm;
            local_sm_cache.remove(original_sender);
            local_sm=null;
        }

        // cache has no entry to original_sender or entry is stale
        local_sm=super.pickSiteMaster(site_masters, original_sender);
        Address existing=local_sm_cache.putIfAbsent(original_sender, local_sm);
        return existing != null? existing : local_sm;
    }

    @Override
    public Route pickRoute(String site, List<Route> routes, Address original_sender) {
        if(original_sender == null)
            return super.pickRoute(site, routes, original_sender);

        Route route=route_cache.get(original_sender);
        if(route != null) {
            if(routes.contains(route))
                return route;
            route_cache.remove(original_sender);
            route=null;
        }

        // cache has no entry to original_sender or entry is stale
        route=super.pickRoute(site, routes, original_sender);
        Route existing=route_cache.putIfAbsent(original_sender, route);
        return existing != null? existing : route;
    }
}

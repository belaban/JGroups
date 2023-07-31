package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.Util;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class which joins the bridge cluster and send and receives messages to/from other sites. Also generates
 * sitesUp()/sitesDown() notifications from view changes
 * @author Bela Ban
 * @since  5.2.17
 */
public class Bridge implements Receiver {
    protected final JChannel channel;
    protected final Relayer3 rel;
    protected final RELAY    relay;
    protected final Log      log;
    protected final String   cluster_name;
    protected       View     view;


    protected Bridge(final JChannel ch, final Relayer3 r, final String cluster_name,
                     String channel_name, AddressGenerator addr_generator) throws Exception {
        this.channel=ch;
        this.cluster_name=cluster_name;
        this.rel=r;
        this.relay=rel.relay();
        this.log=rel.log();
        channel.setName(channel_name).setReceiver(this).addAddressGenerator(addr_generator);
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
        relay.handleRelayMessage(msg);
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
            if(!rel.containsRoute(r))
                up.add(r);
        }
        log.trace("[Relayer " + channel.getAddress() + "] view: " + new_view);
        for(Map.Entry<String,List<Address>> entry: sites.entrySet()) {
            String        key=entry.getKey();
            List<Address> val=entry.getValue();
            List<Route>   existing=rel.getRoutes(key);
            List<Route>   newRoutes=existing != null? new ArrayList<>(existing) : new ArrayList<>();

            // Remove routes not in the view anymore:
            newRoutes.removeIf(r -> !val.contains(r.siteMaster()));

            // Add routes that aren't yet in the routing table:
            val.stream().filter(addr -> !contains(newRoutes, addr))
              .forEach(addr -> newRoutes.add(new Route(addr, channel, relay, log).stats(relay.statsEnabled())));

            if(newRoutes.isEmpty()) {
                rel.removeRoute(key);
                down.add(key);
            }
            else
                rel.addRoutes(key, newRoutes);
        }

        // remove all routes which were dropped between the old and new view:
        if(!removed_routes.isEmpty() && log.isTraceEnabled())
            log.trace("%s: removing routes %s from routing table", channel.getAddress(), removed_routes);
        // removed_routes.forEach(rel.routes.keySet()::remove);
        removed_routes.forEach(rel::removeRoute);

        if(!down.isEmpty())
            relay.sitesChange(true, down);
        if(!up.isEmpty())
            relay.sitesChange(false, up);
    }

    @Override
    public String toString() {
        return String.format("bridge %s", cluster_name);
    }

    protected static boolean contains(List<Route> routes, Address addr) {
        return routes.stream().anyMatch(route -> route.siteMaster().equals(addr));
    }

    /** Returns a list of routes that were in old_view, but are no longer in new_view */
    protected static List<String> removedRoutes(View old_view, View new_view) {
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

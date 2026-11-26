package org.jgroups.protocols.relay;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Provides a cache of all sites and their members (addresses, IP addresses, site masters etc) in a network of
 * autonomous sites. The cache is an approximation, and is refreshed on reception of {@link RelayHeader#SITES_UP}
 * or {@link RelayHeader#SITES_DOWN} notifications. A refresh can also be triggered programmatically.
 * <br/>
 * Used as a component in {@link RELAY2} and {@link RELAY3}.
 * @author Bela Ban
 * @since  5.2.15
 */
public class Topology {
    protected final RELAY             relay;
    protected final Map<String,View>  cache=new ConcurrentHashMap<>(); // cache of sites and members
    protected BiConsumer<String,View> view_handler;
    @Property(description="When true, members joining or leaving any site in the network are triggering the " +
      "view handler callback")
    protected boolean                 global_views=true; // might be set to false by default in the future

    public Topology(RELAY relay) {
        this.relay=Objects.requireNonNull(relay);
    }

    public Map<String,View> cache()                {return cache;}
    public boolean          globalViews()          {return global_views;}
    public Topology         globalViews(boolean b) {global_views=b; return this;}

    /**
     * Sets a view handler
     * @param c The view handler. Arguments are the site and the {@link View} of that site)
     */
    public Topology setViewHandler(BiConsumer<String,View> c) {
        view_handler=c; return this;}


    @ManagedOperation(description="Fetches information (site, address, IP address) from all members")
    public Topology refresh() {
        return refresh(null);
    }

    /**
     * Refreshes the topology for a given site.
     * @param site The site. If null, all sites will be refreshed.
     */
    @ManagedOperation(description="Fetches information (site, address, IP address) from all members of a given site")
    public Topology refresh(String site) {
        return refresh(site, false);
    }

    @ManagedOperation(description="Fetches the topology information for a given site")
    public Topology refresh(String site, boolean return_entire_cache) {
        Address dest=new SiteMaster(site); // sent to all site masters if site == null
        RelayHeader hdr=new RelayHeader(RelayHeader.TOPO_REQ)
          .setFinalDestination(dest).setOriginalSender(relay.getAddress())
          .returnEntireCache(return_entire_cache);
        Message topo_req=new EmptyMessage(dest).putHeader(relay.getId(), hdr);
        relay.down(topo_req);
        return this;
    }

    @ManagedOperation(description="Prints the cache information about all members")
    public String print() {
        return print(null);
    }

    /**
     * Dumps the members for a given site, or all sites
     * @param site The site name. Dumps all sites if null
     * @return A string of all sites and their members
     */
    @ManagedOperation(description="Prints the cache information about all members")
    public String print(String site) {
        if(site != null)
            return dumpSite(site);
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,View> e: cache.entrySet()) {
            sb.append(dumpSite(e.getKey())).append("\n");
        }
        return sb.toString();
    }

    public Topology removeAll(Collection<String> sites) {
        if(sites == null)
            cache.keySet().clear();
        else
            cache.keySet().removeAll(sites);
        return this;
    }

    @Override
    public String toString() {
        return String.format("%d sites", cache.size());
    }

    protected String dumpSite(String site) {
        View view=cache.get(site);
        if(view == null)
            return String.format("%s: no view found for site %s", relay.getAddress(), site);
        return String.format("%s: %s", site, view);
    }

    /** Called when a response has been received. Updates the internal cache */
    protected void put(String site, View v) {
        View existing=cache.get(site);
        if(!Objects.equals(existing, v)) {
            cache.put(site, v);
            if(view_handler != null)
                view_handler.accept(site, v);
        }
    }

}

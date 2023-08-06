package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.XmlNode;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.protocols.relay.config.RelayConfig.SiteConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.SuppressLog;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.conf.AttributeType.TIME;
import static org.jgroups.protocols.relay.RelayHeader.SITES_DOWN;
import static org.jgroups.protocols.relay.RelayHeader.SITES_UP;

/**
 * Common superclass for {@link RELAY2} and {@link RELAY3}
 * @author Bela Ban
 * @since  5.2.17
 */
@XmlInclude(schema="relay.xsd",type=XmlInclude.Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
@XmlElement(name="RelayConfiguration",type="relay:RelayConfigurationType")
public abstract class RELAY extends Protocol {
    // reserved flags
    public static final short              can_become_site_master_flag = 1 << 1;

    @Property(description="Name of the site; must be defined in the configuration",writable=false)
    protected String                       site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String                       config;

    @Property(description="Whether or not this node can become the site master. If false, " +
      "and we become the coordinator, we won't start the bridge(s)",writable=false)
    protected boolean                      can_become_site_master=true;

    @Property(description="Maximum number of site masters. Setting this to a value greater than 1 means that we can " +
      "have multiple site masters. If the value is greater than the number of cluster nodes, everyone in the site " +
      "will be a site master (and thus join the global cluster",writable=false)
    protected int                          max_site_masters=1;

    @Property(description="Ratio of members that are site masters, out of range [0..1] (0 disables this). The number " +
      "of site masters is computes as Math.min(max_site_masters, view.size() * site_masters_ratio). " +
       "See https://issues.redhat.com/browse/JGRP-2581 for details")
    protected double                       site_masters_ratio;

    @Property(description="Fully qualified name of a class implementing SiteMasterPicker")
    protected String                       site_master_picker_impl;

    @Property(description="If true, the creation of the relay channel (and the connect()) are done in the background. " +
      "Async relay creation is recommended, so the view callback won't be blocked")
    protected boolean                      async_relay_creation=true;

    @Property(description="Time during which identical errors about no route to host will be suppressed. " +
      "0 disables this (every error will be logged).",type= TIME)
    protected long                         suppress_time_no_route_errors=60000;

    @ManagedAttribute(description="Whether this member is a site master")
    protected volatile boolean             is_site_master;

    @ManagedAttribute(description="A list of site masters in this (local) site")
    protected volatile List<Address>       site_masters;

    @ManagedAttribute(description="The first of all site masters broadcasts route-up/down messages to all members of " +
      "the local cluster")
    protected volatile boolean             broadcast_route_notifications;

    protected volatile Relayer             relayer;

    @Component(description="Maintains a cache of sites and members",name="topo")
    protected final Topology               topo=new Topology(this);

    /** Number of messages forwarded to the local SiteMaster */
    protected final LongAdder              forward_to_site_master=new LongAdder();

    protected final LongAdder              forward_sm_time=new LongAdder();

    /** Number of messages relayed by the local SiteMaster to a remote SiteMaster */
    protected final LongAdder              relayed=new LongAdder();

    /** Total time spent relaying messages from the local SiteMaster to remote SiteMasters (in ns) */
    protected final LongAdder              relayed_time=new LongAdder();

    /** Number of messages (received from a remote Sitemaster and) delivered by the local SiteMaster to a local node */
    protected final LongAdder              forward_to_local_mbr=new LongAdder();
    protected final LongAdder              forward_to_local_mbr_time=new LongAdder();
    protected short[]                      prots_above; // protocol IDs above RELAY
    protected TimeScheduler                timer;
    protected SiteMasterPicker             site_master_picker;
    protected volatile List<Address>       members=new ArrayList<>(11);
    protected volatile RouteStatusListener route_status_listener;

    /** Listens for notifications about becoming site master (arg: true), or ceasing to be site master (arg: false) */
    protected Consumer<Boolean>            site_master_listener;

    /** A map containing site names (e.g. "LON") as keys and SiteConfigs as values */
    protected final Map<String,SiteConfig> sites=new HashMap<>();

    protected SiteConfig                   site_config;

    /** Log to suppress identical errors for messages to non-existing sites ('no route to site X') */
    protected SuppressLog<String>          suppress_log_no_route;


    @ManagedAttribute(description="Number of messages forwarded to the local SiteMaster")
    public long getNumForwardedToSiteMaster() {return forward_to_site_master.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to the local SiteMaster",type=TIME)
    public long getTimeForwardingToSM() {return MILLISECONDS.convert(forward_sm_time.sum(), NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to the local SiteMaster")
    public long getAvgMsgsForwardingToSM() {return getTimeForwardingToSM() > 0?
      (long)(getNumForwardedToSiteMaster() / (getTimeForwardingToSM()/1000.0)) : 0;}

    @ManagedAttribute(description="Number of messages sent by this SiteMaster to a remote SiteMaster")
    public long getNumRelayed() {return relayed.sum();}

    @ManagedAttribute(description="The total time (ms) spent relaying messages from this SiteMaster to remote SiteMasters"
      ,type=TIME)
    public long getTimeRelaying() {return MILLISECONDS.convert(relayed_time.sum(), NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for relaying messages from this SiteMaster to remote SiteMasters")
    public long getAvgMsgsRelaying() {return getTimeRelaying() > 0? (long)(getNumRelayed() / (getTimeRelaying()/1000.0)) : 0;}

    @ManagedAttribute(description="Number of messages (received from a remote Sitemaster and) delivered " +
      "by this SiteMaster to a local node")
    public long getNumForwardedToLocalMbr() {return forward_to_local_mbr.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to a member in the same site",
      type=TIME)
    public long getTimeForwardingToLocalMbr() {return MILLISECONDS.convert(forward_to_local_mbr_time.sum(), NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to a member in the same site")
    public long getAvgMsgsForwardingToLocalMbr() {return getTimeForwardingToLocalMbr() > 0?
      (long)(getNumForwardedToLocalMbr() / (getTimeForwardingToLocalMbr()/1000.0)) : 0;}

    @ManagedAttribute(description="Number of 'no route to site X' errors")
    public int getNumberOfNoRouteErrors() {return suppress_log_no_route.getCache().size();}

    @ManagedOperation(description="Clears the 'no route to site X' cache")
    public <T extends RELAY> T clearNoRouteCache() {suppress_log_no_route.getCache().clear(); return (T)this;}


    public String              getSite()                 {return site;}
    public String              site()                    {return site;}
    public <T extends RELAY> T setSite(String s)         {this.site=s; return (T)this;}
    public <T extends RELAY> T site(String s)            {site=s; return (T)this;}
    public Topology            topo()                    {return topo;}
    public List<Address>       members()                 {return members;}
    public String              config()                  {return config;}
    public <T extends RELAY> T config(String cfg)        {config=cfg; return (T)this;}
    public String              getConfig()               {return config;}
    public <T extends RELAY> T setConfig(String c)       {this.config=c; return (T)this;}
    public TimeScheduler       getTimer()                {return timer;}

    public void                incrementRelayed()        {relayed.increment();}
    public void                addToRelayedTime(long d)  {relayed_time.add(d);}

    public List<Address>       siteMasters()                     {return site_masters;}
    public boolean             canBecomeSiteMaster()             {return can_become_site_master;}
    public <T extends RELAY> T canBecomeSiteMaster(boolean f)    {can_become_site_master=f; return (T)this;}
    public int                 getMaxSiteMasters()               {return max_site_masters;}
    public <T extends RELAY> T setMaxSiteMasters(int m)          {this.max_site_masters=m; return (T)this;}

    public double              getSiteMastersRatio()             {return site_masters_ratio;}
    public <T extends RELAY> T setSiteMastersRatio(double r)     {site_masters_ratio=r; return (T)this;}

    public String              getSiteMasterPickerImpl()         {return site_master_picker_impl;}
    public <T extends RELAY> T setSiteMasterPickerImpl(String s) {this.site_master_picker_impl=s; return (T)this;}
    public <T extends RELAY> T siteMasterPicker(SiteMasterPicker s) {if(s != null) this.site_master_picker=s; return (T)this;}
    public boolean             asyncRelayCreation()                 {return async_relay_creation;}
    public <T extends RELAY> T asyncRelayCreation(boolean flag)     {async_relay_creation=flag;   return (T)this;}

    public boolean             broadcastRouteNotifications()          {return broadcast_route_notifications;}
    public <T extends RELAY> T broadcastRouteNotifications(boolean b) {this.broadcast_route_notifications=b; return (T)this;}
    public RouteStatusListener getRouteStatusListener()               {return route_status_listener;}
    public void                setRouteStatusListener(RouteStatusListener l) {this.route_status_listener=l;}

    public <T extends RELAY> T setSiteMasterListener(Consumer<Boolean> l)  {site_master_listener=l; return (T)this;}
    public <T extends RELAY> T addSite(String n, SiteConfig cfg)           {sites.put(n,cfg); return (T)this;}
    public List<String>        siteNames()                                 {return getSites();}

    @ManagedAttribute(description="Whether or not this instance is a site master")
    public boolean isSiteMaster() {return relayer != null;}

    public List<String> getSites() {
        return sites.isEmpty()? Collections.emptyList() : new ArrayList<>(sites.keySet());
    }

    @ManagedOperation(description="Prints the routes that are currently up. " +
      "Only available if we're the current coordinator (site master)")
    public String printSites() {
        return relayer != null? Util.print(relayer.getSiteNames()) : "n/a (not site master)";
    }

    @ManagedOperation(description="Prints the contents of the routing table. " +
      "Only available if we're the current coordinator (site master)")
    public String printRoutes() {
        return relayer != null? relayer.printRoutes() : "n/a (not site master)";
    }

    /**
     * Returns the route to a given site
     * @param site_name The site name, e.g. "SFO"
     * @return The route to the given site, or null if no route was found or we're not the coordinator
     */
    public Route getRoute(String site_name) {
        Relayer tmp=relayer;
        return tmp != null? tmp.getRoute(site_name): null;
    }

    /**
     * @return A {@link List} of sites name that are currently up or {@code null} if this node is not a Site Master (i.e.
     * {@link #isSiteMaster()} returns false).
     */
    public List<String> getCurrentSites() { // must return a List, as Infinispan expects a List (not a Collection)
        Relayer rel=relayer;
        return rel == null ? null : new ArrayList<>(rel.getSiteNames());
    }

    /**
     * Returns the bridge channel to a given site
     * @param site_name The site name, e.g. "SFO"
     * @return The JChannel to the given site, or null if no route was found or we're not the coordinator
     */
    public JChannel getBridge(String site_name) {
        Relayer tmp=relayer;
        Route route=tmp != null? tmp.getRoute(site_name): null;
        return route != null? route.bridge() : null;
    }

    public View getBridgeView(String cluster_name) {
        Relayer tmp=relayer;
        return tmp != null? tmp.getBridgeView(cluster_name) : null;
    }

    public void resetStats() {
        super.resetStats();
        forward_to_site_master.reset();
        forward_sm_time.reset();
        relayed.reset();
        relayed_time.reset();
        forward_to_local_mbr.reset();
        forward_to_local_mbr_time.reset();
        clearNoRouteCache();
    }

    public void init() throws Exception {
        super.init();
        configure();

        if(site_master_picker == null) {
            site_master_picker=new SiteMasterPicker() {
                public Address pickSiteMaster(List<Address> site_masters, Address original_sender) {
                    return Util.pickRandomElement(site_masters);
                }

                public Route pickRoute(String site, List<Route> routes, Address original_sender) {
                    return Util.pickRandomElement(routes);
                }
            };
        }

        if(suppress_time_no_route_errors <= 0)
            throw new IllegalArgumentException("suppress_time_no_route_errors has to be > 0");
        suppress_log_no_route=new SuppressLog<>(log, "RelayNoRouteToSite", "SuppressMsgRelay");
    }

    public void stop() {
        super.stop();
        is_site_master=false;
        log.trace("%s: ceased to be site master; closing bridges", local_addr);
        if(relayer != null)
            relayer.stop();
    }

    public void configure() throws Exception {
        timer=getTransport().getTimer();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        if(max_site_masters < 1) {
            log.warn("max_size_masters was " + max_site_masters + ", changed to 1");
            max_site_masters=1;
        }

        if(site_masters_ratio < 0) {
            log.warn("%s: changing incorrect site_masters_ratio of %.2f to 0", local_addr, site_masters_ratio);
            site_masters_ratio=0.0;
        }
        else if(site_masters_ratio > 1) {
            log.warn("%s: changing incorrect site_masters_ratio of %.2f to 1", local_addr, site_masters_ratio);
            site_masters_ratio=1.0;
        }

        if(site_master_picker_impl != null) {
            Class<SiteMasterPicker> clazz=(Class<SiteMasterPicker>)Util.loadClass(site_master_picker_impl, (Class<?>)null);
            this.site_master_picker=clazz.getDeclaredConstructor().newInstance();
        }

        if(config != null)
            parseSiteConfiguration(sites);

        site_config=sites.get(site);
        if(site_config == null)
            throw new Exception("site configuration for \"" + site + "\" not found in " + config);
        log.trace("site configuration:\n" + site_config);
        prots_above=getIdsAbove();
    }

    @Override
    public void parse(XmlNode node) throws Exception {
        RelayConfig.parse(node, sites);
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return up_prot.up(evt);
    }

    public abstract void handleView(View v);

    protected abstract void handleRelayMessage(Message msg);

    public String toString() {
        return String.format("%s%s", getClass().getSimpleName(), local_addr != null? String.format(" (%s)", local_addr) : "");
    }

    /** Parses the configuration by reading the config file */
    protected void parseSiteConfiguration(final Map<String,SiteConfig> map) throws Exception {
        try(InputStream input=ConfiguratorFactory.getConfigStream(config)) {
            RelayConfig.parse(input, map);
        }
    }

    /** Copies the message, but only the headers above the current protocol (RELAY) (or RpcDispatcher related headers) */
    protected Message copy(Message msg) {
        return Util.copy(msg, true, Global.BLOCKS_START_ID, this.prots_above);
    }

    protected void sitesChange(boolean down, Set<String> sites) {
        if(!broadcast_route_notifications || sites == null || sites.isEmpty())
            return;
        RelayHeader hdr=new RelayHeader(down? SITES_DOWN : SITES_UP, null, null)
          .addToSites(sites);
        down_prot.down(new EmptyMessage(null).putHeader(id, hdr));
    }

    protected void notifySiteMasterListener(boolean flag) {
        if(site_master_listener != null)
            site_master_listener.accept(flag);
    }

    protected PhysicalAddress getPhysicalAddress(Address mbr) {
        return mbr != null? (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, mbr)) : null;
    }

    /**
     * Iterates over the list of members and adds every member if the member's rank is below max_site_masters. Skips
     * members which cannot become site masters (can_become_site_master == false). If no site master can be found,
     * the first member of the view will be returned (even if it has can_become_site_master == false)
     */
    protected static List<Address> determineSiteMasters(View view, int max_num_site_masters) {
        List<Address> retval=new ArrayList<>(view.size());
        int selected=0;

        for(Address member: view) {
            if(member instanceof ExtendedUUID && !((ExtendedUUID)member).isFlagSet(can_become_site_master_flag))
                continue;

            if(selected++ < max_num_site_masters)
                retval.add(member);
        }

        if(retval.isEmpty()) {
            Address coord=view.getCoord();
            if(coord != null)
                retval.add(coord);
        }
        return retval;
    }

    /** Returns a site master from site_masters */
    protected Address pickSiteMaster(Address sender) {
        List<Address> masters=site_masters;
        if(masters.size() == 1)
            return masters.get(0);
        return site_master_picker.pickSiteMaster(masters, sender);
    }

    protected void triggerSiteUnreachableEvent(SiteAddress remoteSite) {
        up_prot.up(new Event(Event.SITE_UNREACHABLE, remoteSite));
        if(route_status_listener != null)
            route_status_listener.sitesUnreachable(remoteSite.getSite());
    }

}

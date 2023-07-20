package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.*;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.XmlNode;
import org.jgroups.protocols.relay.SiteAddress.Type;
import org.jgroups.protocols.relay.SiteStatus.Status;
import org.jgroups.protocols.relay.Topology.MemberInfo;
import org.jgroups.protocols.relay.Topology.Members;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.jgroups.protocols.relay.Relay2Header.*;

// todo: check if copy is needed in route(), passUp() and deliver(); possibly pass a boolean as parameter (copy or not)
// todo: use CompletableFutures in routeThen(); this could parallelize routing and delivery/passsing up
// todo: check if a message can bypass RELAY2 completely when NO_RELAY is set (in up(),down())
/**
 * Provides relaying of messages between autonomous sites.<br/>
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<br/>
 * JIRA: https://issues.redhat.com/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@XmlInclude(schema="relay.xsd",type=XmlInclude.Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
@XmlElement(name="RelayConfiguration",type="relay:RelayConfigurationType")
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {
    // reserved flags
    public    static final short                       can_become_site_master_flag = 1 << 1;
    protected static final String                      RELAY2_CL=RELAY2.class.getSimpleName();

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Name of the site; must be defined in the configuration",writable=false)
    protected String                                   site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String                                   config;

    @Property(description="Whether or not this node can become the site master. If false, " +
      "and we become the coordinator, we won't start the bridge(s)",writable=false)
    protected boolean                                  can_become_site_master=true;

    @Property(description="Maximum number of site masters. Setting this to a value greater than 1 means that we can " +
      "have multiple site masters. If the value is greater than the number of cluster nodes, everyone in the site " +
      "will be a site master (and thus join the global cluster",writable=false)
    protected int                                      max_site_masters=1;

    @Property(description="Ratio of members that are site masters, out of range [0..1] (0 disables this). The number " +
      "of site masters is computes as Math.min(max_site_masters, view.size() * site_masters_ratio). " +
      "See https://issues.redhat.com/browse/JGRP-2581 for details")
    protected double                                   site_masters_ratio;

    @Deprecated(since="5.2.17")
    @Property(description="Whether or not we generate our own addresses in which we use can_become_site_master. " +
      "If this property is false, can_become_site_master is ignored",deprecatedMessage="will be ignored")
    protected boolean                                  enable_address_tagging;

    @Deprecated
    @Property(description="Whether or not to relay multicast (dest=null) messages",deprecatedMessage="will be ignored")
    protected boolean                                  relay_multicasts=true;

    @Property(description="If true, the creation of the relay channel (and the connect()) are done in the background. " +
      "Async relay creation is recommended, so the view callback won't be blocked")
    protected boolean                                  async_relay_creation=true;

    @Property(description="Fully qualified name of a class implementing SiteMasterPicker")
    protected String                                   site_master_picker_impl;

    @Property(description="Time during which identical errors about no route to host will be suppressed. " +
      "0 disables this (every error will be logged).",type=AttributeType.TIME)
    protected long                                     suppress_time_no_route_errors=60000;

    /** A map containing site names (e.g. "LON") as keys and SiteConfigs as values */
    protected final Map<String,RelayConfig.SiteConfig> sites=new HashMap<>();

    protected RelayConfig.SiteConfig                   site_config;

    @ManagedAttribute(description="Whether this member is a site master")
    protected volatile boolean                         is_site_master;

    @ManagedAttribute(description="The first of all site masters broadcasts route-up/down messages to all members of " +
      "the local cluster")
    protected volatile boolean                         broadcast_route_notifications;

    // A list of site masters in this (local) site
    @ManagedAttribute(description="The current site masters")
    protected volatile List<Address>                   site_masters;

    protected SiteMasterPicker                         site_master_picker;

    /** Listens for notifications about becoming site master (arg: true), or ceasing to be site master (arg: false) */
    protected Consumer<Boolean>                        site_master_listener;

    protected volatile Relayer                         relayer;

    protected TimeScheduler                            timer;

    protected volatile List<Address>                   members=new ArrayList<>(11);

    @Property(description="If true, a site master forwards messages received from other sites to randomly chosen " +
      "members of the local site for load balancing, reducing work for itself",deprecatedMessage="ignored")
    @Deprecated(since="5.2.15",forRemoval=true)
    protected boolean                                  can_forward_local_cluster;

    @Property(description="Number of millis to wait for topology detection",type=AttributeType.TIME)
    protected long                                     topo_wait_time=2000;

    protected short[]                                  prots_above; // protocol IDs above RELAY2

    protected volatile RouteStatusListener             route_status_listener;

    // to prevent duplicate sitesUp()/sitesDown() notifications; this is needed in every member: routes are only
    // maintained by site masters (relayer != null)
    // todo: replace with topo once JGRP-2706 is in place
    @ManagedAttribute(description="A cache maintaining a list of sites that are up")
    protected final SiteStatus                         site_status=new SiteStatus();

    /** Number of messages forwarded to the local SiteMaster */
    protected final LongAdder                          forward_to_site_master=new LongAdder();

    protected final LongAdder                          forward_sm_time=new LongAdder();

    /** Number of messages relayed by the local SiteMaster to a remote SiteMaster */
    protected final LongAdder                          relayed=new LongAdder();

    /** Total time spent relaying messages from the local SiteMaster to remote SiteMasters (in ns) */
    protected final LongAdder                          relayed_time=new LongAdder();

    /** Number of messages (received from a remote Sitemaster and) delivered by the local SiteMaster to a local node */
    protected final LongAdder                          forward_to_local_mbr=new LongAdder();

    protected final LongAdder                          forward_to_local_mbr_time=new LongAdder();

    @Component(description="Maintains a cache of sites and members",name="topo")
    protected final Topology                           topo=new Topology(this);

    /** Log to suppress identical errors for messages to non-existing sites ('no route to site X') */
    protected SuppressLog<String>                      suppress_log_no_route;

    // Fluent configuration
    public RELAY2 site(String site_name)               {site=site_name;              return this;}
    public SiteStatus siteStatus()                     {return site_status;}
    public RELAY2 config(String cfg)                   {config=cfg;                  return this;}
    public RELAY2 canBecomeSiteMaster(boolean flag)    {can_become_site_master=flag; return this;}
    @Deprecated
    public RELAY2 enableAddressTagging(boolean flag)   {return this;}
    @Deprecated(since="5.2.15")
    public boolean relayMulticasts()                   {return true;}
    @Deprecated(since="5.2.15")
    public RELAY2 relayMulticasts(boolean ignoredFlag) {return this;}
    public RELAY2 asyncRelayCreation(boolean flag)     {async_relay_creation=flag;   return this;}
    public RELAY2 siteMasterPicker(SiteMasterPicker s) {if(s != null) this.site_master_picker=s; return this;}
    public Topology topo()                             {return topo;}
    public String  site()                              {return site;}
    public List<Address> siteMasters()                 {return site_masters;}
    public List<Address> members()                     {return members;}
    public List<String> siteNames()                    {return getSites();}
    public String  config()                            {return config;}
    public boolean canBecomeSiteMaster()               {return can_become_site_master;}
    public boolean enableAddressTagging()              {return true;}
    public boolean asyncRelayCreation()                {return async_relay_creation;}
    public TimeScheduler getTimer()                    {return timer;}
    public void incrementRelayed()                     {relayed.increment();}
    public void addToRelayedTime(long delta)           {relayed_time.add(delta);}

    public String  getSite()                              {return site;}
    public RELAY2  setSite(String s)                      {this.site=s; return this;}

    public String  getConfig()                            {return config;}
    public RELAY2  setConfig(String c)                    {this.config=c; return this;}

    public int     getMaxSiteMasters()                    {return max_site_masters;}
    public RELAY2  setMaxSiteMasters(int m)               {this.max_site_masters=m; return this;}

    public double  getSiteMastersRatio()                  {return site_masters_ratio;}
    public RELAY2  setSiteMastersRatio(double r)          {site_masters_ratio=r; return this;}

    public String  getSiteMasterPickerImpl()              {return site_master_picker_impl;}
    public RELAY2  setSiteMasterPickerImpl(String s)      {this.site_master_picker_impl=s; return this;}

    public boolean broadcastRouteNotifications()          {return broadcast_route_notifications;}
    public RELAY2  broadcastRouteNotifications(boolean b) {this.broadcast_route_notifications=b; return this;}

    public boolean canForwardLocalCluster()               {return false;}
    public RELAY2  canForwardLocalCluster(boolean ignored){return this;}

    public long    getTopoWaitTime()                      {return topo_wait_time;}
    public RELAY2  setTopoWaitTime(long t)                {this.topo_wait_time=t; return this;}



    public RouteStatusListener getRouteStatusListener()       {return route_status_listener;}
    public void setRouteStatusListener(RouteStatusListener l) {this.route_status_listener=l;}

    public RELAY2 setSiteMasterListener(Consumer<Boolean> l)  {site_master_listener=l; return this;}

    @ManagedAttribute(description="Number of messages forwarded to the local SiteMaster")
    public long getNumForwardedToSiteMaster()                 {return forward_to_site_master.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to the local SiteMaster"
      ,type=AttributeType.TIME)
    public long getTimeForwardingToSM() {return TimeUnit.MILLISECONDS.convert(forward_sm_time.sum(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to the local SiteMaster")
    public long getAvgMsgsForwardingToSM() {return getTimeForwardingToSM() > 0?
                                            (long)(getNumForwardedToSiteMaster() / (getTimeForwardingToSM()/1000.0)) : 0;}

    @ManagedAttribute(description="Number of messages sent by this SiteMaster to a remote SiteMaster")
    public long getNumRelayed() {return relayed.sum();}

    @ManagedAttribute(description="The total time (ms) spent relaying messages from this SiteMaster to remote SiteMasters"
      ,type=AttributeType.TIME)
    public long getTimeRelaying() {return TimeUnit.MILLISECONDS.convert(relayed_time.sum(), TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for relaying messages from this SiteMaster to remote SiteMasters")
    public long getAvgMsgsRelaying() {return getTimeRelaying() > 0? (long)(getNumRelayed() / (getTimeRelaying()/1000.0)) : 0;}

    @ManagedAttribute(description="Number of messages (received from a remote Sitemaster and) delivered " +
      "by this SiteMaster to a local node")
    public long getNumForwardedToLocalMbr() {return forward_to_local_mbr.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to a member in the same site",
      type=AttributeType.TIME)
    public long getTimeForwardingToLocalMbr() {return TimeUnit.MILLISECONDS.convert(forward_to_local_mbr_time.sum(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to a member in the same site")
    public long getAvgMsgsForwardingToLocalMbr() {return getTimeForwardingToLocalMbr() > 0?
                                                  (long)(getNumForwardedToLocalMbr() / (getTimeForwardingToLocalMbr()/1000.0)) : 0;}

    @ManagedAttribute(description="Whether or not this instance is a site master")
    public boolean isSiteMaster() {return relayer != null;}

    @ManagedAttribute(description="Number of 'no route to site X' errors")
    public int getNumberOfNoRouteErrors() {
        return suppress_log_no_route.getCache().size();
    }

    @ManagedOperation(description="Clears the 'no route to site X' cache")
    public RELAY2 clearNoRouteCache() {
        suppress_log_no_route.getCache().clear();
        return this;
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

    public View getBridgeView(String cluster_name) {
        Relayer tmp=relayer;
        return tmp != null? tmp.getBridgeView(cluster_name) : null;
    }


    public RELAY2 addSite(String site_name, RelayConfig.SiteConfig cfg) {
        sites.put(site_name,cfg);
        return this;
    }

    public List<String> getSites() {
        return sites.isEmpty()? Collections.emptyList() : new ArrayList<>(sites.keySet());
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
        JChannel ch=getProtocolStack().getChannel();
        ch.addAddressGenerator(new AddressGenerator() {
            @Override public Address generateAddress() {return generateAddress(null);}
            @Override public Address generateAddress(String name) {
                SiteUUID uuid=new SiteUUID(UUID.randomUUID(), name, site);
                if(can_become_site_master)
                    uuid.setFlag(can_become_site_master_flag);
                return uuid;
            }
        });
        prots_above=getIdsAbove();
    }


    public void stop() {
        super.stop();
        is_site_master=false;
        log.trace("%s: ceased to be site master; closing bridges", local_addr);
        if(relayer != null)
            relayer.stop();
    }

    /**
     * Parses the configuration by reading the config file.
     * @throws Exception
     */
    protected void parseSiteConfiguration(final Map<String,RelayConfig.SiteConfig> map) throws Exception {
        try(InputStream input=ConfiguratorFactory.getConfigStream(config)) {
            RelayConfig.parse(input, map);
        }
    }

    @Override
    public void parse(XmlNode node) throws Exception {
        RelayConfig.parse(node, sites);
    }

    @ManagedOperation(description="Prints the contents of the routing table. " +
      "Only available if we're the current coordinator (site master)")
    public String printRoutes() {
        return relayer != null? relayer.printRoutes() : "n/a (not site master)";
    }

    @ManagedOperation(description="Prints the routes that are currently up. " +
      "Only available if we're the current coordinator (site master)")
    public String printSites() {
        return relayer != null? Util.print(relayer.getSiteNames()) : "n/a (not site master)";
    }

    @ManagedOperation(description="Prints the topology (site masters and local members) of this site")
    public String printTopology(boolean all_sites) {
        if(!all_sites)
            return printLocalTopology();
        return topo.print();
    }

    @ManagedOperation(description="Prints the topology (site masters and local members) of this site")
    public String printLocalTopology() {
        return topo.print(this.site);
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
    public List<String> getCurrentSites() {
        Relayer rel=relayer;
        return rel == null ? null : rel.getSiteNames();
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        //if(msg.isFlagSet(Flag.NO_RELAY))
          //  return down_prot.down(msg);
        msg.src(local_addr);
        return process(true, msg);
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView(evt.getArg());
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
       // if(msg.isFlagSet(Flag.NO_RELAY))
         //   return up_prot.up(msg);
        Message copy=msg;
        Relay2Header hdr=msg.getHeader(id);
        if(hdr != null) {
            if(hdr.getType() == SITE_UNREACHABLE) {
                triggerSiteUnreachableEvent((SiteAddress)hdr.final_dest);
                return null;
            }
            //todo: check if copy is needed!
            copy=copy(msg).dest(hdr.final_dest).src(hdr.original_sender).putHeader(id, hdr);
        }
        return process(false, copy);
    }

    public void up(MessageBatch batch) {
        List<SiteAddress> unreachable_sites=null;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next(), copy=msg;
           // if(msg.isFlagSet(Flag.NO_RELAY))
             //   continue;
            Relay2Header hdr=msg.getHeader(id);
            it.remove();
            if(hdr != null) {
                if(hdr.getType() == SITE_UNREACHABLE) {
                    SiteAddress site_addr=(SiteAddress)hdr.final_dest;
                    String site_name=site_addr.getSite();
                    if(unreachable_sites == null)
                        unreachable_sites=new ArrayList<>();
                    boolean contains=unreachable_sites.stream().anyMatch(sa -> sa.getSite().equals(site_name));
                    if(!contains)
                        unreachable_sites.add(site_addr);
                    continue;
                }
                copy=copy(msg).dest(hdr.final_dest).src(hdr.original_sender).putHeader(id, hdr);
            }
            process(false, copy);
        }
        if(unreachable_sites != null) {
            for(SiteAddress sa: unreachable_sites)
                triggerSiteUnreachableEvent(sa); // https://issues.redhat.com/browse/JGRP-2586
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void sendResponseFor(List<Address> mbrs, Address dest) {
        Members m=new Members(this.site);
        for(Address mbr: mbrs) {
            SiteAddress addr=mbr instanceof SiteMaster? new SiteMaster(((SiteMaster)mbr).getSite())
              : new SiteUUID((UUID)mbr, NameCache.get(mbr), site);
            MemberInfo mi=new MemberInfo(this.site, addr, (IpAddress)getPhysicalAddress(mbr),
                                         site_masters.contains(mbr));
            m.addJoined(mi);
        }
        Message rsp=new ObjectMessage(dest, m).putHeader(this.id, new Relay2Header(TOPO_RSP));
        down(rsp);
    }

    protected PhysicalAddress getPhysicalAddress(Address mbr) {
        return mbr != null? (PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, mbr)) : null;
    }

    public void handleView(View view) {
        members=view.getMembers(); // First, save the members for routing received messages to local members

        int max_num_site_masters=max_site_masters;
        if(site_masters_ratio > 0)
            max_num_site_masters=(int)Math.max(max_site_masters, site_masters_ratio * view.size());

        List<Address> old_site_masters=site_masters;
        List<Address> new_site_masters=determineSiteMasters(view, max_num_site_masters);

        boolean become_site_master=new_site_masters.contains(local_addr)
          && (old_site_masters == null || !old_site_masters.contains(local_addr));
        boolean cease_site_master=old_site_masters != null
          && old_site_masters.contains(local_addr) && !new_site_masters.contains(local_addr);
        site_masters=new_site_masters;

        if(!site_masters.isEmpty() && site_masters.get(0).equals(local_addr))
            broadcast_route_notifications=true;

        if(become_site_master) {
            is_site_master=true;
            final String bridge_name="_" + NameCache.get(local_addr);
            if(relayer != null)
                relayer.stop();
            relayer=new Relayer(this, log);
            final Relayer tmp=relayer;
            if(async_relay_creation)
                timer.execute(() -> startRelayer(tmp, bridge_name));
            else
                startRelayer(relayer, bridge_name);
            notifySiteMasterListener(true);
        }
        else {
            if(cease_site_master) { // ceased being the site master: stop the relayer
                is_site_master=false;
                notifySiteMasterListener(false);
                log.trace(local_addr + ": ceased to be site master; closing bridges");
                if(relayer != null)
                    relayer.stop();
            }
        }
        suppress_log_no_route.removeExpired(suppress_time_no_route_errors);
        topo().adjust(this.site, view.getMembers());
    }

    public String toString() {
        return String.format("%s%s", RELAY2_CL, local_addr != null? String.format(" (%s)", local_addr) : "");
    }

    /** Called to handle a message received from a different site (via a bridge channel) */
    protected void handleRelayMessage(Relay2Header hdr, Message msg) {
        Message copy=copy(msg).dest(hdr.final_dest).src(hdr.original_sender).putHeader(id, hdr);
        // todo: check if copy is needed!
        process(true, copy);
    }

    /** Handles SITES_UP/SITES_DOWN/TOPO_REQ/TOPO_RSP messages */
    protected boolean handleAdminMessage(Relay2Header hdr, Message msg) {
        switch(hdr.type) {
            case SITES_UP:
            case SITES_DOWN:
                Set<String> tmp_sites=new HashSet<>();
                if(hdr.hasSites())
                    tmp_sites.addAll(hdr.getSites());
                tmp_sites.remove(this.site);
                if(tmp_sites != null && !tmp_sites.isEmpty()) {
                    Status status=hdr.type == SITES_UP? Status.up : Status.down;
                    Set<String> tmp=site_status.add(tmp_sites, status);
                    if(status == Status.down)
                        topo.removeAll(tmp_sites);
                    if(route_status_listener != null && !tmp.isEmpty()) {
                        String[] t=tmp.toArray(new String[]{});
                        if(hdr.type == SITES_UP)
                            route_status_listener.sitesUp(t);
                        else
                            route_status_listener.sitesDown(t);
                    }
                }
                return true;
            case TOPO_REQ:
                sendResponseFor(members, msg.src());
                return true;
            case TOPO_RSP:
                Members mbrs=msg.getObject();
                if(mbrs != null)
                    topo.handleResponse(mbrs);
                return true;
        }
        return false;
    }


    // todo: use CompletableFutures and possibly thenRunAsync() to parallelize (e.g.) routing and local delivery
    protected Object routeThen(Message msg, List<String> sites, Supplier<Object> action) {
        if(!msg.isFlagSet(Flag.NO_RELAY))
            route(msg, sites);
        return action != null? action.get() : null;
    }

    protected Object process(boolean down, Message msg) {
        Address dest=msg.dest();
        SiteAddress dst=null;
        Type type=dest == null? Type.ALL : (dst=(SiteAddress)dest).type();
        if(is_site_master) {
            switch(type) {
                case ALL:
                    if(down)
                        return routeThen(msg, null,() -> deliver(null, msg, true));
                    return routeThen(msg, null, () -> passUp(msg));
                case SM_ALL:
                    return routeThen(msg, null, () -> passUp(msg));
                case SM:
                    if(sameSite(dst))
                        return passUp(msg);
                    return route(msg, Arrays.asList(dst.getSite()));
                case UNICAST:
                    if(sameSite(dst)) {
                        if(down) {
                            // no passUp() if dst == local_addr: we want the transport to use a separate thread to do
                            // loopbacks
                            return deliver(dst, msg,false);
                        }
                        return passUp(msg);
                    }
                    return route(msg, Arrays.asList(dst.getSite()));
            }
        }
        else {
            switch(type) {
                case ALL:
                    if(down)
                        return deliver(null, msg, false);
                    return passUp(msg);
                case SM_ALL:
                case SM:
                    if(down)
                        return sendToLocalSiteMaster(local_addr, msg); // todo: local_addr or msg.src()?
                    throw new IllegalStateException(String.format("non site master received a msg with dst %s",dst));
                case UNICAST:
                    if(down) {
                        if(sameSite(dst)) // todo: if same address -> passUp()
                            return deliver(dst, msg, false);
                        return sendToLocalSiteMaster(local_addr, msg);
                    }
                    return passUp(msg);
            }
        }
        return null;
    }


    /**
     * Sends a message to the given sites, or all sites (excluding the local site)
     * @param msg The message to be sent
     * @param sites The sites to send the message to. If null, msg will be sent to all sites listed in the
     *              routing table, excepting the local site
     */
    protected Object route(Message msg, Collection<String> sites) {
        // boolean skip_null_routes=sites != null;
        final Relayer r=relayer;
        if(r == null) {
            log.warn("%s: not site master; dropping message", local_addr);
            return null;
        }
        if(sites == null)
            sites=new ArrayList<>(r.routes.keySet());
        sites.remove(this.site);
        if(sites.isEmpty()) // no sites in routing table
            return null;
        Relay2Header hdr=msg.getHeader(this.id);
        Address dest=msg.dest(), sender=hdr != null && hdr.original_sender != null?
          ((ExtendedUUID)hdr.getOriginalSender()).addContents((ExtendedUUID)local_addr) : local_addr;
        // msg.src(sender); // todo: check if necessary

        Set<String> visited_sites=null;
        if(dest == null || dest instanceof  SiteMaster && ((SiteMaster)dest).getSite() == null) {
            visited_sites=new HashSet<>(sites); // to be added to the header
            visited_sites.add(this.site);
            if(hdr != null && hdr.hasVisitedSites()) {
                visited_sites.addAll(hdr.getVisitedSites());
                sites.removeAll(hdr.getVisitedSites()); // avoid cycles (https://issues.redhat.com/browse/JGRP-1519)
            }
        }

        for(String s: sites) {
            Route route=r.getRoute(s, sender);
            if(route == null) {
                route=r.getForwardingRouteMatching(s, sender);
            }
            if(route == null) {
                suppress_log_no_route.log(SuppressLog.Level.error, s, suppress_time_no_route_errors, sender, s);
                sendSiteUnreachableTo(msg.getSrc(), s);
            }
            else
                route.send(dest, sender, msg, visited_sites);
        }
        return null;
    }

    /**
     * Sends the message to a local destination.
     * @param next_dest The destination. If null, the message will be delivered to all members of the local cluster. In
     *             this case, flag {@link Flag#NO_RELAY} will be set, so that the resulting
     *             multicast is not forwarded to other sites.
     * @param msg The message to deliver
     */
    protected Object deliver(Address next_dest, Message msg, boolean dont_relay) {
        checkLocalAddress(next_dest);
        Address final_dest=msg.dest(), original_sender=msg.src();
        if(log.isTraceEnabled())
            log.trace(local_addr + ": forwarding message to final destination " + final_dest + " to " +
                        next_dest);
        Relay2Header tmp=msg.getHeader(this.id);
        // todo: check if copy is needed here
        Relay2Header hdr=tmp != null? tmp.copy().setOriginalSender(original_sender).setFinalDestination(final_dest)
          : new Relay2Header(DATA, final_dest, original_sender);
        Message copy=copy(msg).setDest(next_dest).setSrc(null).putHeader(id, hdr);
        if(dont_relay)
            copy.setFlag(Flag.NO_RELAY);
        return down_prot.down(copy);
    }

    protected Object sendToLocalSiteMaster(Address sender, Message msg) {
        long start=stats? System.nanoTime() : 0;
        Address site_master=pickSiteMaster(sender);
        if(site_master == null)
            throw new IllegalStateException("site master is null");
        Object ret=deliver(site_master, msg, false);
        if(stats) {
            forward_sm_time.add(System.nanoTime() - start);
            forward_to_site_master.increment();
        }
        return ret;
    }

    /**
     * Sends a message up the stack. If there's a header, msg.dest is set to the header's final destination and
     * msg.src to te header's original sender
     * @param msg The message to be sent up
     */
    protected Object passUp(Message msg) {
        Relay2Header hdr=msg.getHeader(this.id);
        Message copy=copy(msg); // todo: check if copy is needed!
        if(hdr != null) {
            copy.dest(hdr.final_dest).src(hdr.original_sender); // no need to copy as msg won't get retransmitted
            if(handleAdminMessage(hdr, copy))
                return null;
        }
        return up_prot.up(copy);
    }

    protected Address checkLocalAddress(Address dest) {
        if(dest == null)
            return dest;
        SiteAddress s=(SiteAddress)dest;
        String dest_site=s.getSite();
        if(dest_site != null && !site.equals(dest_site))
            throw new IllegalArgumentException(String.format("destination %s it not the same as the local site %s",
                                                             dest_site, this.site));
        return dest;
    }

    protected boolean sameSite(SiteAddress addr) {
        if(addr == null)
            return true;
        String dest_site=addr.getSite();
        return dest_site == null || this.site.equals(dest_site);
    }

    /**
     * Sends a SITE-UNREACHABLE message to the sender of the message. Because the sender is always local (we're the
     * relayer), no routing needs to be done
     * @param src The node who is trying to send a message to the {@code target_site}
     * @param target_site The remote site's name.
     */
    protected void sendSiteUnreachableTo(Address src, String target_site) {
        if (src == null || src.equals(local_addr)) {
            //short circuit
            // if src == null, it means the message comes from the top protocol (i.e. the local node)
            triggerSiteUnreachableEvent(new SiteMaster(target_site));
            return;
        }
        // send message back to the src node.
        Message msg=new EmptyMessage(src).setFlag(Flag.OOB)
          .putHeader(id, new Relay2Header(SITE_UNREACHABLE).addToSites(target_site));
        down(msg);
    }

    protected void sitesChange(boolean down, Set<String> sites) {
        if(!broadcast_route_notifications || sites == null || sites.isEmpty())
            return;
        Relay2Header hdr=new Relay2Header(down? SITES_DOWN : SITES_UP, null, null)
          .addToSites(sites);
        down_prot.down(new EmptyMessage(null).putHeader(id, hdr));
    }

    /** Copies the message, but only the headers above the current protocol (RELAY) (or RpcDispatcher related headers) */
    protected Message copy(Message msg) {
        return Util.copy(msg, true, Global.BLOCKS_START_ID, this.prots_above);
    }


    protected void startRelayer(Relayer rel, String bridge_name) {
        try {
            log.trace(local_addr + ": became site master; starting bridges");
            rel.start(site_config, bridge_name, site);
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed starting relayer", t);
        }
    }


    protected void notifySiteMasterListener(boolean flag) {
        if(site_master_listener != null)
            site_master_listener.accept(flag);
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

    private void triggerSiteUnreachableEvent(SiteAddress remoteSite) {
        up_prot.up(new Event(Event.SITE_UNREACHABLE, remoteSite));
        if(route_status_listener != null)
            route_status_listener.sitesUnreachable(remoteSite.getSite());
    }

}

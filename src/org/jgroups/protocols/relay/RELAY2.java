package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.protocols.FORWARD_TO_COORD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;
import org.w3c.dom.Node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.jboss.org/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
@XmlInclude(schema="relay.xsd",type=XmlInclude.Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
@XmlElement(name="RelayConfiguration",type="relay:RelayConfigurationType")
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {
    // reserved flags
    public static final short site_master_flag            = 1 << 0;
    public static final short can_become_site_master_flag = 1 << 1;

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Name of the site (needs to be defined in the configuration)",writable=false)
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

    @Property(description="Whether or not we generate our own addresses in which we use can_become_site_master. " +
      "If this property is false, can_become_site_master is ignored")
    protected boolean                                  enable_address_tagging;

    @Property(description="Whether or not to relay multicast (dest=null) messages")
    protected boolean                                  relay_multicasts=true;

    @Property(description="If true, the creation of the relay channel (and the connect()) are done in the background. " +
      "Async relay creation is recommended, so the view callback won't be blocked")
    protected boolean                                  async_relay_creation=true;

    @Property(description="If true, logs a warning if the FORWARD_TO_COORD protocol is not found. This property might " +
      "get deprecated soon")
    protected boolean                                  warn_when_ftc_missing;

    @Property(description="Fully qualified name of a class implementing SiteMasterPicker")
    protected String                                   site_master_picker_impl;


    /* ---------------------------------------------    Fields    ------------------------------------------------ */

    /** A map containing site names (e.g. "LON") as keys and SiteConfigs as values */
    protected final Map<String,RelayConfig.SiteConfig> sites=new HashMap<>();

    protected RelayConfig.SiteConfig                   site_config;

    @ManagedAttribute(description="Whether this member is a site master")
    protected volatile boolean                         is_site_master;

    // A list of site masters in this (local) site
    protected volatile List<Address>                   site_masters;

    protected SiteMasterPicker                         site_master_picker;

    protected volatile Relayer                         relayer;

    protected TimeScheduler                            timer;

    protected volatile Address                         local_addr;

    protected volatile List<Address>                   members=new ArrayList<>(11);

    /** Whether or not FORWARD_TO_COORD is on the stack */
    @ManagedAttribute(description="FORWARD_TO_COORD protocol is present below the current protocol")
    protected boolean                                  forwarding_protocol_present;

    @Property(description="If true, a site master forwards messages received from other sites to randomly chosen " +
      "members of the local site for load balancing, reducing work for itself")
    protected boolean                                  can_forward_local_cluster;

    // protocol IDs above RELAY2
    protected short[]                                  prots_above;

    protected volatile RouteStatusListener             route_status_listener;

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

    /** Number of messages delivered locally, e.g. received and delivered to self */
    protected final LongAdder                          local_deliveries=new LongAdder();

    /** Total time (ms) for received messages that are delivered locally */
    protected final LongAdder                          local_delivery_time=new LongAdder();



    // Fluent configuration
    public RELAY2 site(String site_name)               {site=site_name;              return this;}
    public RELAY2 config(String cfg)                   {config=cfg;                  return this;}
    public RELAY2 canBecomeSiteMaster(boolean flag)    {can_become_site_master=flag; return this;}
    public RELAY2 enableAddressTagging(boolean flag)   {enable_address_tagging=flag; return this;}
    public RELAY2 relayMulticasts(boolean flag)        {relay_multicasts=flag;       return this;}
    public RELAY2 asyncRelayCreation(boolean flag)     {async_relay_creation=flag;   return this;}
    public RELAY2 siteMasterPicker(SiteMasterPicker s) {if(s != null) this.site_master_picker=s; return this;}

    public String  site()                              {return site;}
    public List<String> siteNames()                    {return getSites();}
    public String  config()                            {return config;}
    public boolean canBecomeSiteMaster()               {return can_become_site_master;}
    public boolean enableAddressTagging()              {return enable_address_tagging;}
    public boolean relayMulticasts()                   {return relay_multicasts;}
    public boolean asyncRelayCreation()                {return async_relay_creation;}
    public Address getLocalAddress()                   {return local_addr;}
    public TimeScheduler getTimer()                    {return timer;}
    public void incrementRelayed()                     {relayed.increment();}
    public void addToRelayedTime(long delta)           {relayed_time.add(delta);}


    public RouteStatusListener getRouteStatusListener()       {return route_status_listener;}
    public void setRouteStatusListener(RouteStatusListener l) {this.route_status_listener=l;}

    @ManagedAttribute(description="Number of messages forwarded to the local SiteMaster")
    public long getNumForwardedToSiteMaster() {return forward_to_site_master.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to the local SiteMaster")
    public long getTimeForwardingToSM() {return TimeUnit.MILLISECONDS.convert(forward_sm_time.sum(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to the local SiteMaster")
    public long getAvgMsgsForwardingToSM() {return getTimeForwardingToSM() > 0?
                                            (long)(getNumForwardedToSiteMaster() / (getTimeForwardingToSM()/1000.0)) : 0;}



    @ManagedAttribute(description="Number of messages sent by this SiteMaster to a remote SiteMaster")
    public long getNumRelayed() {return relayed.sum();}

    @ManagedAttribute(description="The total time (ms) spent relaying messages from this SiteMaster to remote SiteMasters")
    public long getTimeRelaying() {return TimeUnit.MILLISECONDS.convert(relayed_time.sum(), TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for relaying messages from this SiteMaster to remote SiteMasters")
    public long getAvgMsgsRelaying() {return getTimeRelaying() > 0? (long)(getNumRelayed() / (getTimeRelaying()/1000.0)) : 0;}



    @ManagedAttribute(description="Number of messages (received from a remote Sitemaster and) delivered " +
      "by this SiteMaster to a local node")
    public long getNumForwardedToLocalMbr() {return forward_to_local_mbr.sum();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to a member in the same site")
    public long getTimeForwardingToLocalMbr() {return TimeUnit.MILLISECONDS.convert(forward_to_local_mbr_time.sum(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to a member in the same site")
    public long getAvgMsgsForwardingToLocalMbr() {return getTimeForwardingToLocalMbr() > 0?
                                                  (long)(getNumForwardedToLocalMbr() / (getTimeForwardingToLocalMbr()/1000.0)) : 0;}




    @ManagedAttribute(description="Number of messages delivered locally, e.g. received and delivered to self")
    public long getNumLocalDeliveries() {return local_deliveries.sum();}

    @ManagedAttribute(description="The total time (ms) spent delivering received messages locally")
    public long getTimeDeliveringLocally() {return TimeUnit.MILLISECONDS.convert(local_delivery_time.sum(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for delivering received messages locally")
    public long getAvgMsgsDeliveringLocally() {return getTimeDeliveringLocally() > 0?
                                               (long)(getNumLocalDeliveries() / (getTimeDeliveringLocally()/1000.0)) : 0;}

    @ManagedAttribute(description="Whether or not this instance is a site master")
    public boolean isSiteMaster() {return relayer != null;}


    public void resetStats() {
        super.resetStats();
        forward_to_site_master.reset();
        forward_sm_time.reset();
        relayed.reset();
        relayed_time.reset();
        forward_to_local_mbr.reset();
        forward_to_local_mbr_time.reset();
        local_deliveries.reset();
        local_delivery_time.reset();
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
    }

    public void configure() throws Exception {
        timer=getTransport().getTimer();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        TP tp=getTransport();
        if(tp.getUseIpAddresses())
            throw new IllegalArgumentException(String.format("%s cannot be used if %s.use_ip_addrs is true",
                                                             RELAY2.class.getSimpleName(), tp.getClass().getSimpleName()));
        if(max_site_masters < 1) {
            log.warn("max_size_masters was " + max_site_masters + ", changed to 1");
            max_site_masters=1;
        }

        if(site_master_picker_impl != null) {
            Class<SiteMasterPicker> clazz=Util.loadClass(site_master_picker_impl, (Class)null);
            this.site_master_picker=clazz.newInstance();
        }

        if(config != null)
            parseSiteConfiguration(sites);

        site_config=sites.get(site);
        if(site_config == null)
            throw new Exception("site configuration for \"" + site + "\" not found in " + config);
        log.trace(local_addr + ": site configuration:\n" + site_config);

        if(!site_config.getForwards().isEmpty())
            log.warn(local_addr + ": forwarding routes are currently not supported and will be ignored. This will change " +
                       "with hierarchical routing (https://issues.jboss.org/browse/JGRP-1506)");

        List<Integer> available_down_services=getDownServices();
        forwarding_protocol_present=available_down_services != null && available_down_services.contains(Event.FORWARD_TO_COORD);
        if(!forwarding_protocol_present && warn_when_ftc_missing && log.isWarnEnabled())
            log.warn(local_addr + ": " + FORWARD_TO_COORD.class.getSimpleName() + " protocol not found below; " +
                       "unable to re-submit messages to the new coordinator if the current coordinator crashes");

        if(enable_address_tagging) {
            JChannel ch=getProtocolStack().getChannel();
            ch.addAddressGenerator(() -> {
                ExtendedUUID retval=ExtendedUUID.randomUUID();
                if(can_become_site_master)
                    retval.setFlag(can_become_site_master_flag);
                return retval;
            });
        }

        prots_above=getIdsAbove();
    }


    public void stop() {
        super.stop();
        is_site_master=false;
        log.trace(local_addr + ": ceased to be site master; closing bridges");
        if(relayer != null)
            relayer.stop();
    }

    /**
     * Parses the configuration by reading the config file.
     * @throws Exception
     */
    protected void parseSiteConfiguration(final Map<String,RelayConfig.SiteConfig> map) throws Exception {
        InputStream input=null;
        try {
            input=ConfiguratorFactory.getConfigStream(config);
            RelayConfig.parse(input, map);
        }
        finally {
            Util.close(input);
        }
    }

    public void parse(Node node) throws Exception {
        RelayConfig.parse(node, sites);
    }

    @ManagedOperation(description="Prints the contents of the routing table. " +
      "Only available if we're the current coordinator (site master)")
    public String printRoutes() {
        return relayer != null? relayer.printRoutes() : "n/a (not site master)";
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



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object down(Message msg) {
        Address dest=msg.getDest();
        if(dest == null || !(dest instanceof SiteAddress))
            return down_prot.down(msg);

        SiteAddress target=(SiteAddress)dest;
        Address src=msg.getSrc();
        SiteAddress sender=src instanceof SiteMaster? new SiteMaster(((SiteMaster)src).getSite())
          : new SiteUUID((UUID)local_addr, NameCache.get(local_addr), site);
        if(local_addr instanceof ExtendedUUID)
            ((ExtendedUUID)sender).addContents((ExtendedUUID)local_addr);

        // target is in the same site; we can deliver the message in our local cluster
        if(target.getSite().equals(site)) {
            if(local_addr.equals(target) || (target instanceof SiteMaster && is_site_master)) {
                // we cannot simply pass msg down, as the transport doesn't know how to send a message to a (e.g.) SiteMaster
                long start=stats? System.nanoTime() : 0;
                forwardTo(local_addr, target, sender, msg, false);
                if(stats) {
                    local_delivery_time.add(System.nanoTime() - start);
                    local_deliveries.increment();
                }
            }
            else
                deliverLocally(target, sender, msg);
            return null;
        }

        // forward to the site master unless we're the site master (then route the message directly)
        if(!is_site_master) {
            long start=stats? System.nanoTime() : 0;
            Address site_master=pickSiteMaster(sender);
            if(site_master == null)
                throw new IllegalStateException("site master is null");
            forwardTo(site_master, target, sender, msg, max_site_masters == 1);
            if(stats) {
                forward_sm_time.add(System.nanoTime() - start);
                forward_to_site_master.increment();
            }
        }
        else
            route(target, sender, msg);
        return null;
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        Relay2Header hdr=msg.getHeader(id);
        Address dest=msg.getDest();

        if(hdr == null) {
            // forward a multicast message to all bridges except myself, then pass up
            if(dest == null && is_site_master && relay_multicasts && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                Address src=msg.getSrc();
                Address sender=new SiteUUID((UUID)msg.getSrc(), NameCache.get(msg.getSrc()), site);
                if(src instanceof ExtendedUUID)
                    ((SiteUUID)sender).addContents((ExtendedUUID)src);
                sendToBridges(sender, msg, site);
            }
            return up_prot.up(msg); // pass up
        }
        else { // header is not null
            if(dest != null)
                handleMessage(hdr, msg);
            else
                deliver(null, hdr.original_sender, msg);
        }
        return null;
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            Relay2Header hdr=msg.getHeader(id);
            Address dest=msg.getDest();

            if(hdr == null) {
                // forward a multicast message to all bridges except myself, then pass up
                if(dest == null && is_site_master && relay_multicasts && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                    Address src=msg.getSrc();
                    Address sender=new SiteUUID((UUID)msg.getSrc(), NameCache.get(msg.getSrc()), site);
                    if(src instanceof ExtendedUUID)
                        ((SiteUUID)sender).addContents((ExtendedUUID)src);
                    sendToBridges(sender, msg, site);
                }
            }
            else { // header is not null
                batch.remove(msg); // message is consumed
                if(dest != null)
                    handleMessage(hdr, msg);
                else
                    deliver(null, hdr.original_sender, msg);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /** Called to handle a message received by the relayer */
    protected void handleRelayMessage(Relay2Header hdr, Message msg) {
        if(hdr.final_dest != null) {
            Message message=msg;
            Relay2Header header=hdr;

            if(header.type == Relay2Header.DATA && can_forward_local_cluster) {
                SiteUUID site_uuid=(SiteUUID)hdr.final_dest;

                //  If configured to do so, we want to load-balance these messages,
                UUID tmp=(UUID)Util.pickRandomElement(members);
                SiteAddress final_dest=new SiteUUID(tmp, site_uuid.getName(), site_uuid.getSite());

                // If we select a different address to handle this message, we handle it here.
                if(!final_dest.equals(hdr.final_dest)) {
                    message=copy(msg);
                    header=new Relay2Header(Relay2Header.DATA, final_dest, hdr.original_sender );
                    message.putHeader(id, header);
                }
            }
            handleMessage(header, message);
        }
        else {
            Message copy=copy(msg).dest(null).src(null).putHeader(id, hdr);
            down_prot.down(copy); // multicast locally

            // Don't forward: https://issues.jboss.org/browse/JGRP-1519
            // sendToBridges(msg.getSrc(), buf, from_site, site_id);  // forward to all bridges except self and from
        }
    }





    /** Called to handle a message received by the transport */
    protected void handleMessage(Relay2Header hdr, Message msg) {
        switch(hdr.type) {
            case Relay2Header.DATA:
                route((SiteAddress)hdr.final_dest, (SiteAddress)hdr.original_sender, msg);
                break;
            case Relay2Header.SITE_UNREACHABLE:
                up_prot.up(new Event(Event.SITE_UNREACHABLE, hdr.final_dest));
                break;
            case Relay2Header.HOST_UNREACHABLE:
                break;
            default:
                log.error("type " + hdr.type + " unknown");
                break;
        }
    }



    /**
     * Routes the message to the target destination, used by a site master (coordinator)
     * @param dest
     * @param sender the address of the sender
     * @param msg The message
     */
    protected void route(SiteAddress dest, SiteAddress sender, Message msg) {
        String target_site=dest.getSite();
        if(target_site.equals(site)) {
            if(local_addr.equals(dest) || ((dest instanceof SiteMaster) && is_site_master)) {
                deliver(dest, sender, msg);
            }
            else
                deliverLocally(dest, sender, msg); // send to member in same local site
            return;
        }
        Relayer tmp=relayer;
        if(tmp == null) {
            log.warn(local_addr + ": not site master; dropping message");
            return;
        }

        Route route=tmp.getRoute(target_site, sender);
        if(route == null) {
            log.error(local_addr + ": no route to " + target_site + ": dropping message");
            sendSiteUnreachableTo(sender, target_site);
        }
        else
            route.send(dest,sender,msg);
    }


    /** Sends the message via all bridges excluding the excluded_sites bridges */
    protected void sendToBridges(Address sender, final Message msg, String ... excluded_sites) {
        Relayer tmp=relayer;
        List<Route> routes=tmp != null? tmp.getRoutes(excluded_sites) : null;
        if(routes == null)
            return;
        for(Route route: routes) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": relaying multicast message from " + sender + " via route " + route);
            try {
                route.send(null, sender, msg);
            }
            catch(Exception ex) {
                log.error(local_addr + ": failed relaying message from " + sender + " via route " + route, ex);
            }
        }
    }

    /**
     * Sends a SITE-UNREACHABLE message to the sender of the message. Because the sender is always local (we're the
     * relayer), no routing needs to be done
     * @param dest
     * @param target_site
     */
    protected void sendSiteUnreachableTo(Address dest, String target_site) {
        Message msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .src(new SiteUUID((UUID)local_addr, NameCache.get(local_addr), site))
          .putHeader(id,new Relay2Header(Relay2Header.SITE_UNREACHABLE,new SiteMaster(target_site),null));
        down_prot.down(msg);
    }

    protected void forwardTo(Address next_dest, SiteAddress final_dest, Address original_sender, final Message msg,
                             boolean forward_to_current_coord) {
        if(log.isTraceEnabled())
            log.trace(local_addr + ": forwarding message to final destination " + final_dest + " to " +
                        (forward_to_current_coord? " the current coordinator" : next_dest));
        Message copy=copy(msg).dest(next_dest).src(null);
        Relay2Header hdr=new Relay2Header(Relay2Header.DATA, final_dest, original_sender);
        copy.putHeader(id,hdr);
        if(forward_to_current_coord && forwarding_protocol_present)
            down_prot.down(new Event(Event.FORWARD_TO_COORD, copy));
        else
            down_prot.down(copy);
    }


    protected void deliverLocally(SiteAddress dest, SiteAddress sender, Message msg) {
        Address local_dest;
        boolean send_to_coord=false;
        if(dest instanceof SiteUUID) {
            if(dest instanceof SiteMaster) {
                local_dest=pickSiteMaster(sender);
                if(local_dest == null)
                    throw new IllegalStateException("site master was null");
                send_to_coord=true;
            }
            else {
                SiteUUID tmp=(SiteUUID)dest;
                local_dest=new UUID(tmp.getMostSignificantBits(), tmp.getLeastSignificantBits());
            }
        }
        else
            local_dest=dest;

        if(log.isTraceEnabled())
            log.trace(local_addr + ": delivering message to " + dest + " in local cluster");
        long start=stats? System.nanoTime() : 0;
        forwardTo(local_dest, dest, sender, msg, send_to_coord);
        if(stats) {
            forward_to_local_mbr_time.add(System.nanoTime() - start);
            forward_to_local_mbr.increment();
        }
    }


    protected void deliver(Address dest, Address sender, final Message msg) {
        try {
            Message copy=copy(msg).dest(dest).src(sender);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": delivering message from " + sender);
            long start=stats? System.nanoTime() : 0;
            up_prot.up(copy);
            if(stats) {
                local_delivery_time.add(System.nanoTime() - start);
                local_deliveries.increment();
            }
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedDeliveringMessage"), e);
        }
    }

    /** Copies the message, but only the headers above the current protocol (RELAY) (or RpcDispatcher related headers) */
    protected Message copy(Message msg) {
        return msg.copy(true, Global.BLOCKS_START_ID, this.prots_above);
    }



    public void handleView(View view) {
        members=view.getMembers(); // First, save the members for routing received messages to local members

        List<Address> old_site_masters=site_masters;
        List<Address> new_site_masters=determineSiteMasters(view);

        boolean become_site_master=new_site_masters.contains(local_addr)
          && (old_site_masters == null || !old_site_masters.contains(local_addr));
        boolean cease_site_master=old_site_masters != null
          && old_site_masters.contains(local_addr) && !new_site_masters.contains(local_addr);
        site_masters=new_site_masters;

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
        }
        else {
            if(cease_site_master) { // ceased being the site master: stop the relayer
                is_site_master=false;
                log.trace(local_addr + ": ceased to be site master; closing bridges");
                if(relayer != null)
                    relayer.stop();
            }
        }
    }


    protected void startRelayer(Relayer rel, String bridge_name) {
        try {
            log.trace(local_addr + ": became site master; starting bridges");
            rel.start(site_config.getBridges(), bridge_name, site);
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed starting relayer", t);
        }
    }



    /**
     * Iterates over the list of members and adds every member if the member's rank is below max_site_masters. Skips
     * members which cannot become site masters (can_become_site_master == false). If no site master can be found,
     * the first member of the view will be returned (even if it has can_become_site_master == false)
     */
    protected List<Address> determineSiteMasters(View view) {
        List<Address> retval=new ArrayList<>(view.size());
        int selected=0;

        for(Address member: view) {
            if(member instanceof ExtendedUUID && !((ExtendedUUID)member).isFlagSet(can_become_site_master_flag))
                continue;

            if(selected++ < max_site_masters)
                retval.add(member);
        }

        if(retval.isEmpty()) {
            Address tmp=Util.getCoordinator(view);
            if(tmp != null)
                retval.add(Util.getCoordinator(view));
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



    public static class Relay2Header extends Header {
        public static final byte DATA             = 1;
        public static final byte SITE_UNREACHABLE = 2; // final_dest is a SiteMaster
        public static final byte HOST_UNREACHABLE = 3; // final_dest is a SiteUUID (not currently used)

        protected byte    type;
        protected Address final_dest;
        protected Address original_sender;


        public Relay2Header() {
        }

        public Relay2Header(byte type, Address final_dest, Address original_sender) {
            this.type=type;
            this.final_dest=final_dest;
            this.original_sender=original_sender;
        }
        public short getMagicId() {return 80;}
        public Supplier<? extends Header> create() {return Relay2Header::new;}

        public int serializedSize() {
            return Global.BYTE_SIZE + Util.size(final_dest) + Util.size(original_sender);
        }

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeAddress(final_dest, out);
            Util.writeAddress(original_sender, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            final_dest=Util.readAddress(in);
            original_sender=Util.readAddress(in);
        }

        public String toString() {
            return typeToString(type) + " [dest=" + final_dest + ", sender=" + original_sender + "]";
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case DATA:             return "DATA";
                case SITE_UNREACHABLE: return "SITE_UNREACHABLE";
                case HOST_UNREACHABLE: return "HOST_UNREACHABLE";
                default:               return "<unknown>";
            }
        }
    }
}

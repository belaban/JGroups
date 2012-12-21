package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.protocols.FORWARD_TO_COORD;
import org.jgroups.protocols.relay.config.RelayConfig;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TopologyUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.jboss.org/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
@MBean(description="RELAY2 protocol")
public class RELAY2 extends Protocol {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Name of the site (needs to be defined in the configuration)",writable=false)
    protected String                                   site;

    @Property(description="Name of the relay configuration",writable=false)
    protected String                                   config;

    @Property(description="Whether or not this node can become the site master. If false, " +
      "and we become the coordinator, we won't start the bridge(s)",writable=false)
    protected boolean                                  can_become_site_master=true;

    @Property(description="Whether or not we generate our own addresses in which we use can_become_site_master. " +
      "If this property is false, can_become_site_master is ignored")
    protected boolean                                  enable_address_tagging=false;

    @Property(description="Whether or not to relay multicast (dest=null) messages")
    protected boolean                                  relay_multicasts=true;

    @Property(description="The number of tries to forward a message to a remote site",
              deprecatedMessage="not used anymore, will be ignored")
    protected int                                      max_forward_attempts=5;

    @Deprecated
    @Property(description="The time (in milliseconds) to sleep between forward attempts",
              deprecatedMessage="not used anymore, will be ignored")
    protected long                                     forward_sleep=1000;

    @Property(description="Max number of messages in the foward queue. Messages are added to the forward queue " +
      "when the status of a route went from UP to UNKNOWN and the queue is flushed when the status goes to " +
      "UP (resending all queued messages) or DOWN (sending SITE-UNREACHABLE messages to the senders)")
    protected int                                      fwd_queue_max_size=2000;

    @Property(description="Number of millisconds to wait when the status for a site changed from UP to UNKNOWN " +
      "before that site is declared DOWN. A site that's DOWN triggers immediate sending of a SITE-UNREACHABLE message " +
      "back to the sender of a message to that site")
    protected long                                     site_down_timeout=8000;

    @Property(description="If true, the creation of the relay channel (and the connect()) are done in the background. " +
      "Async relay creation is recommended, so the view callback won't be blocked")
    protected boolean                                  async_relay_creation=true;

    @Property(description="If true, logs a warning if the FORWARD_TO_COORD protocol is not found. This property might " +
      "get deprecated soon")
    protected boolean                                  warn_when_ftc_missing=false;


    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    @ManagedAttribute(description="My site-ID")
    protected short                                    site_id=-1;

    /** A map containing site names (e.g. "LON") as keys and SiteConfigs as values */
    protected final Map<String,RelayConfig.SiteConfig> sites=new HashMap<String,RelayConfig.SiteConfig>();

    protected RelayConfig.SiteConfig                   site_config;

    @ManagedAttribute(description="Whether this member is the coordinator")
    protected volatile boolean                         is_coord=false;

    protected volatile Address                         coord;

    protected volatile Relayer                         relayer;

    protected TimeScheduler                            timer;

    protected volatile Address                         local_addr;

    /** Whether or not FORWARD_TO_COORD is on the stack */
    @ManagedAttribute(description="FORWARD_TO_COORD protocol is present below the current protocol")
    protected boolean                                  forwarding_protocol_present;

    // protocol IDs above RELAY2
    protected short[]                                  prots_above;

    /** Number of messages forwarded to the local SiteMaster */
    protected final AtomicLong                         forward_to_site_master=new AtomicLong(0);

    protected final AtomicLong                         forward_sm_time=new AtomicLong(0);

    /** Number of messages relayed by the local SiteMaster to a remote SiteMaster */
    protected final AtomicLong                         relayed=new AtomicLong(0);

    /** Total time spent relaying messages from the local SiteMaster to remote SiteMasters (in ns) */
    protected final AtomicLong                         relayed_time=new AtomicLong(0);

    /** Number of messages (received from a remote Sitemaster and) delivered by the local SiteMaster to a local node */
    protected final AtomicLong                         forward_to_local_mbr=new AtomicLong(0);

    protected final AtomicLong                         forward_to_local_mbr_time=new AtomicLong(0);

    /** Number of messages delivered locally, e.g. received and delivered to self */
    protected final AtomicLong                         local_deliveries=new AtomicLong(0);

    /** Total time (ms) for received messages that are delivered locally */
    protected final AtomicLong                         local_delivery_time=new AtomicLong(0);



    // Fluent configuration
    public RELAY2 site(String site_name)             {site=site_name;              return this;}
    public RELAY2 config(String cfg)                 {config=cfg;                  return this;}
    public RELAY2 canBecomeSiteMaster(boolean flag)  {can_become_site_master=flag; return this;}
    public RELAY2 enableAddressTagging(boolean flag) {enable_address_tagging=flag; return this;}
    public RELAY2 relayMulticasts(boolean flag)      {relay_multicasts=flag;       return this;}
    @Deprecated
    public RELAY2 maxForwardAttempts(int num)        {                             return this;}
    @Deprecated
    public RELAY2 forwardSleep(long time)            {                             return this;}
    public RELAY2 forwardQueueMaxSize(int size)      {fwd_queue_max_size=size;     return this;}
    public RELAY2 siteDownTimeout(long timeout)      {site_down_timeout=timeout;   return this;}
    public RELAY2 asyncRelayCreation(boolean flag)   {async_relay_creation=flag;   return this;}

    public String  site()                            {return site;}
    public String  config()                          {return config;}
    public boolean canBecomeSiteMaster()             {return can_become_site_master;}
    public boolean enableAddressTagging()            {return enable_address_tagging;}
    public boolean relayMulticasts()                 {return relay_multicasts;}
    public int     forwardQueueMaxSize()             {return fwd_queue_max_size;}
    public long    siteDownTimeout()                 {return site_down_timeout;}
    public boolean asyncRelayCreation()              {return async_relay_creation;}
    public Address       getLocalAddress()           {return local_addr;}
    public TimeScheduler getTimer()                  {return timer;}
    public void incrementRelayed()                   {relayed.incrementAndGet();}
    public void addToRelayedTime(long delta)         {relayed_time.addAndGet(delta);}

    @ManagedAttribute(description="Number of messages forwarded to the local SiteMaster")
    public long getNumForwardedToSiteMaster() {return forward_to_site_master.get();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to the local SiteMaster")
    public long getTimeForwardingToSM() {return TimeUnit.MILLISECONDS.convert(forward_sm_time.get(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to the local SiteMaster")
    public long getAvgMsgsForwardingToSM() {return getTimeForwardingToSM() > 0?
                                            (long)(getNumForwardedToSiteMaster() / (getTimeForwardingToSM()/1000.0)) : 0;}



    @ManagedAttribute(description="Number of messages sent by this SiteMaster to a remote SiteMaster")
    public long getNumRelayed() {return relayed.get();}

    @ManagedAttribute(description="The total time (ms) spent relaying messages from this SiteMaster to remote SiteMasters")
    public long getTimeRelaying() {return TimeUnit.MILLISECONDS.convert(relayed_time.get(), TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for relaying messages from this SiteMaster to remote SiteMasters")
    public long getAvgMsgsRelaying() {return getTimeRelaying() > 0? (long)(getNumRelayed() / (getTimeRelaying()/1000.0)) : 0;}



    @ManagedAttribute(description="Number of messages (received from a remote Sitemaster and) delivered " +
      "by this SiteMaster to a local node")
    public long getNumForwardedToLocalMbr() {return forward_to_local_mbr.get();}

    @ManagedAttribute(description="The total time (in ms) spent forwarding messages to a member in the same site")
    public long getTimeForwardingToLocalMbr() {return TimeUnit.MILLISECONDS.convert(forward_to_local_mbr_time.get(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for forwarding messages to a member in the same site")
    public long getAvgMsgsForwardingToLocalMbr() {return getTimeForwardingToLocalMbr() > 0?
                                                  (long)(getNumForwardedToLocalMbr() / (getTimeForwardingToLocalMbr()/1000.0)) : 0;}




    @ManagedAttribute(description="Number of messages delivered locally, e.g. received and delivered to self")
    public long getNumLocalDeliveries() {return local_deliveries.get();}

    @ManagedAttribute(description="The total time (ms) spent delivering received messages locally")
    public long getTimeDeliveringLocally() {return TimeUnit.MILLISECONDS.convert(local_delivery_time.get(),TimeUnit.NANOSECONDS);}

    @ManagedAttribute(description="The average number of messages / s for delivering received messages locally")
    public long getAvgMsgsDeliveringLocally() {return getTimeDeliveringLocally() > 0?
                                               (long)(getNumLocalDeliveries() / (getTimeDeliveringLocally()/1000.0)) : 0;}


    public void resetStats() {
        super.resetStats();
        forward_to_site_master.set(0);
        forward_sm_time.set(0);
        relayed.set(0);
        relayed_time.set(0);
        forward_to_local_mbr.set(0);
        forward_to_local_mbr_time.set(0);
        local_deliveries.set(0);
        local_delivery_time.set(0);
    }

    public View getBridgeView(String cluster_name) {
        Relayer tmp=relayer;
        return tmp != null? tmp.getBridgeView(cluster_name) : null;
    }


    public RELAY2 addSite(String site_name, RelayConfig.SiteConfig cfg) {
        sites.put(site_name, cfg);
        return this;
    }


    
    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        if(site == null)
            throw new IllegalArgumentException("site cannot be null");
        if(config != null)
            parseSiteConfiguration(sites);

        for(RelayConfig.SiteConfig cfg: sites.values())
            SiteUUID.addToCache(cfg.getId(),cfg.getName());
        site_config=sites.get(site);
        if(site_config == null)
            throw new Exception("site configuration for \"" + site + "\" not found in " + config);
        if(log.isTraceEnabled())
            log.trace(local_addr + ": site configuration:\n" + site_config);


        // Sanity check
        Collection<Short> site_ids=new TreeSet<Short>();
        for(RelayConfig.SiteConfig cfg: sites.values()) {
            site_ids.add(cfg.getId());
            if(site.equals(cfg.getName()))
                site_id=cfg.getId();
        }

        int index=0;
        for(short tmp_id: site_ids) {
            if(tmp_id != index)
                throw new Exception("site IDs need to start with 0 and are required to increase monotonically; " +
                                      "site IDs=" + site_ids);
            index++;
        }

        if(site_id < 0)
            throw new IllegalArgumentException("site_id could not be determined from site \"" + site + "\"");

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
            final AddressGenerator old_generator=ch.getAddressGenerator();
            ch.setAddressGenerator(new AddressGenerator() {
                public Address generateAddress() {
                    if(old_generator != null) {
                        Address addr=old_generator.generateAddress();
                        if(addr instanceof TopologyUUID)
                            return new CanBeSiteMasterTopology((TopologyUUID)addr, can_become_site_master);
                        else if(addr instanceof UUID)
                            return new CanBeSiteMaster((UUID)addr, can_become_site_master);
                        log.warn(local_addr + ": address generator is already set (" + old_generator +
                                   "); will replace it with own generator");
                    }
                    return CanBeSiteMaster.randomUUID(can_become_site_master);
                }
            });
        }

        prots_above=getIdsAbove();
    }


    public void stop() {
        super.stop();
        is_coord=false;
        if(log.isTraceEnabled())
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
        Relayer.Route route=tmp != null? tmp.getRoute(SiteUUID.getSite(site_name)): null;
        return route != null? route.bridge() : null;
    }

    /**
     * Returns the route to a given site
     * @param site_name The site name, e.g. "SFO"
     * @return The route to the given site, or null if no route was found or we're not the coordinator
     */
    public Relayer.Route getRoute(String site_name) {
        Relayer tmp=relayer;
        return tmp != null? tmp.getRoute(SiteUUID.getSite(site_name)): null;
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest == null || !(dest instanceof SiteAddress))
                    break;

                SiteAddress target=(SiteAddress)dest;
                Address src=msg.getSrc();
                SiteAddress sender=src instanceof SiteMaster? new SiteMaster(((SiteMaster)src).getSite())
                  : new SiteUUID((UUID)local_addr, UUID.get(local_addr), site_id);

                // target is in the same site; we can deliver the message in our local cluster
                if(target.getSite() == site_id ) {
                    if(local_addr.equals(target) || (target instanceof SiteMaster && is_coord)) {
                        // we cannot simply pass msg down, as the transport doesn't know how to send a message to a (e.g.) SiteMaster
                        long start=stats? System.nanoTime() : 0;
                        forwardTo(local_addr, target, sender, msg, false);
                        if(stats) {
                            local_delivery_time.addAndGet(System.nanoTime() - start);
                            local_deliveries.incrementAndGet();
                        }
                    }
                    else
                        deliverLocally(target, sender, msg);
                    return null;
                }

                // forward to the coordinator unless we're the coord (then route the message directly)
                if(!is_coord) {
                    long start=stats? System.nanoTime() : 0;
                    forwardTo(coord,target,sender,msg,true);
                    if(stats) {
                        forward_sm_time.addAndGet(System.nanoTime() - start);
                        forward_to_site_master.incrementAndGet();
                    }
                }
                else
                    route(target, sender, msg);
                return null;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Relay2Header hdr=(Relay2Header)msg.getHeader(id);
                Address dest=msg.getDest();

                if(hdr == null) {
                    // forward a multicast message to all bridges except myself, then pass up
                    if(dest == null && is_coord && relay_multicasts && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                        Address sender=new SiteUUID((UUID)msg.getSrc(), UUID.get(msg.getSrc()), site_id);
                        sendToBridges(sender, msg, site_id);
                    }
                    break; // pass up
                }
                else { // header is not null
                    if(dest != null)
                        handleMessage(hdr, msg);
                    else
                        deliver(null, hdr.original_sender, msg);
                }
                return null;

            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }


    /** Called to handle a message received by the relayer */
    protected void handleRelayMessage(Relay2Header hdr, Message msg) {
        Address final_dest=hdr.final_dest;
        if(final_dest != null)
            handleMessage(hdr, msg);
        else {
            // byte[] buf=msg.getBuffer();
            Message copy=msg.copy(true, false);
            copy.setDest(null); // final_dest is null !
            copy.setSrc(null);  // we'll use our own address
            copy.putHeader(id, hdr);
            down_prot.down(new Event(Event.MSG, copy));            // multicast locally

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
        short target_site=dest.getSite();
        if(target_site == site_id) {
            if(local_addr.equals(dest) || ((dest instanceof SiteMaster) && is_coord)) {
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

        Relayer.Route route=tmp.getRoute(target_site);
        if(route == null)
            log.error(local_addr + ": no route to " + SiteUUID.getSiteName(target_site) + ": dropping message");
        else {
            route.send(target_site, dest, sender, msg);
        }
    }


    /** Sends the message via all bridges excluding the excluded_sites bridges */
    protected void sendToBridges(Address sender, final Message msg, short ... excluded_sites) {
        Relayer tmp=relayer;
        List<Relayer.Route> routes=tmp != null? tmp.getRoutes(excluded_sites) : null;
        if(routes == null)
            return;
        for(Relayer.Route route: routes) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": relaying multicast message from " + sender + " via route " + route);
            try {
                route.send(((SiteAddress)route.siteMaster()).getSite(), null, sender, msg);
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
    protected void sendSiteUnreachableTo(Address dest, short target_site) {
        Message msg=new Message(dest).setFlag(Message.Flag.OOB);
        msg.setSrc(new SiteUUID((UUID)local_addr, UUID.get(local_addr), site_id));
        Relay2Header hdr=new Relay2Header(Relay2Header.SITE_UNREACHABLE, new SiteMaster(target_site), null);
        msg.putHeader(id, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected void forwardTo(Address next_dest, SiteAddress final_dest, Address original_sender, final Message msg,
                             boolean forward_to_current_coord) {
        if(log.isTraceEnabled())
            log.trace(local_addr + ": forwarding message to final destination " + final_dest + " to " +
                        (forward_to_current_coord? " the current coordinator" : next_dest));
        Message copy=copy(msg).dest(next_dest).src(null);
        Relay2Header hdr=new Relay2Header(Relay2Header.DATA, final_dest, original_sender);
        copy.putHeader(id, hdr);
        if(forward_to_current_coord && forwarding_protocol_present)
            down_prot.down(new Event(Event.FORWARD_TO_COORD, copy));
        else
            down_prot.down(new Event(Event.MSG, copy));
    }


    protected void deliverLocally(SiteAddress dest, SiteAddress sender, Message msg) {
        Address local_dest;
        boolean send_to_coord=false;
        if(dest instanceof SiteUUID) {
            if(dest instanceof SiteMaster) {
                local_dest=coord;
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
            forward_to_local_mbr_time.addAndGet(System.nanoTime() - start);
            forward_to_local_mbr.incrementAndGet();
        }
    }


    protected void deliver(Address dest, Address sender, final Message msg) {
        try {
            Message copy=copy(msg).dest(dest).src(sender);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": delivering message from " + sender);
            long start=stats? System.nanoTime() : 0;
            up_prot.up(new Event(Event.MSG, copy));
            if(stats) {
                local_delivery_time.addAndGet(System.nanoTime() - start);
                local_deliveries.incrementAndGet();
            }
        }
        catch(Exception e) {
            log.error("failed unmarshalling message", e);
        }
    }

    /** Copies the message, but only the headers above the current protocol (RELAY) (or RpcDispatcher related headers) */
    protected Message copy(Message msg) {
        return msg.copy(true, Global.BLOCKS_START_ID, this.prots_above);
    }



    protected void handleView(View view) {
        Address old_coord=coord, new_coord=determineSiteMaster(view);
        boolean become_coord=new_coord.equals(local_addr) && (old_coord == null || !old_coord.equals(local_addr));
        boolean cease_coord=old_coord != null && old_coord.equals(local_addr) && !new_coord.equals(local_addr);
        coord=new_coord;

        if(become_coord) {
            is_coord=true;
            final String bridge_name="_" + UUID.get(local_addr);
            if(relayer != null)
                relayer.stop();
            relayer=new Relayer(this, log, sites.size());
            final Relayer tmp=relayer;
            if(async_relay_creation) {
                timer.execute(new Runnable() {
                    public void run() {
                        startRelayer(tmp, bridge_name);
                    }
                });
            }
            else
                startRelayer(relayer, bridge_name);
        }
        else {
            if(cease_coord) { // ceased being the coordinator (site master): stop the Relayer
                is_coord=false;
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": ceased to be site master; closing bridges");
                if(relayer != null)
                    relayer.stop();
            }
        }
    }


    protected void startRelayer(Relayer rel, String bridge_name) {
        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": became site master; starting bridges");
            rel.start(site_config.getBridges(), bridge_name, site_id);
        }
        catch(Throwable t) {
            log.error(local_addr + ": failed starting relayer", t);
        }
    }


    /**
     * Gets the site master from view. Iterates through the members and skips members which are {@link CanBeSiteMaster}
     * or {@link CanBeSiteMasterTopology} and its can_become_site_master field is false. If no valid member is found
     * (e.g. all members have can_become_site_master set to false, then the first member will be returned
     */
    protected static Address determineSiteMaster(View view) {
        List<Address> members=view.getMembers();
        for(Address member: members) {
            if((member instanceof CanBeSiteMasterTopology && ((CanBeSiteMasterTopology)member).canBecomeSiteMaster())
              || ((member instanceof CanBeSiteMaster) && ((CanBeSiteMaster)member).canBecomeSiteMaster()))
                return member;
        }
        return Util.getCoordinator(view);
    }



    public static enum RouteStatus {
        UP,      // The route is up and messages can be sent
        UNKNOWN, // Initial status, plus when the view excludes the route. Queues all messages in this state
        DOWN     // The route is down; send SITE-UNREACHABLE messages back to the senders
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

        public int size() {
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

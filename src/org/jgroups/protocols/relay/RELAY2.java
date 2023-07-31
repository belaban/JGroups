package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.util.*;
import java.util.function.Supplier;

/**
 *
 * Design: ./doc/design/RELAY2.txt and at https://github.com/belaban/JGroups/blob/master/doc/design/RELAY2.txt.<p/>
 * JIRA: https://issues.redhat.com/browse/JGRP-1433
 * @author Bela Ban
 * @since 3.2
 */
//@XmlInclude(schema="relay.xsd",type=XmlInclude.Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
//@XmlElement(name="RelayConfiguration",type="relay:RelayConfigurationType")
@MBean(description="RELAY2 protocol")
public class RELAY2 extends RELAY {
    // reserved flags
    // public static final short site_master_flag            = 1 << 0;

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Whether or not we generate our own addresses in which we use can_become_site_master. " +
      "If this property is false, can_become_site_master is ignored")
    protected boolean                                  enable_address_tagging;

    @Property(description="Whether or not to relay multicast (dest=null) messages")
    protected boolean                                  relay_multicasts=true;

    /* ---------------------------------------------    Fields    ------------------------------------------------ */
    protected volatile Relayer2                        relayer;

    @Property(description="If true, a site master forwards messages received from other sites to randomly chosen " +
      "members of the local site for load balancing, reducing work for itself")
    protected boolean                                  can_forward_local_cluster;

    @Property(description="Number of millis to wait for topology detection",type=AttributeType.TIME)
    protected long                                     topo_wait_time=2000;

    protected final ResponseCollector<String>          topo_collector=new ResponseCollector<>();

    // Fluent configuration
    public RELAY2 enableAddressTagging(boolean flag)   {enable_address_tagging=flag; return this;}
    public RELAY2 relayMulticasts(boolean flag)        {relay_multicasts=flag;       return this;}
    public boolean enableAddressTagging()              {return enable_address_tagging;}
    public boolean relayMulticasts()                   {return relay_multicasts;}
    public boolean canForwardLocalCluster()            {return can_forward_local_cluster;}
    public RELAY2  canForwardLocalCluster(boolean c)   {this.can_forward_local_cluster=c; return this;}
    public long    getTopoWaitTime()                   {return topo_wait_time;}
    public RELAY2  setTopoWaitTime(long t)             {this.topo_wait_time=t; return this;}

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
        clearNoRouteCache();
    }

    public View getBridgeView(String cluster_name) {
        Relayer2 tmp=relayer;
        return tmp != null? tmp.getBridgeView(cluster_name) : null;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        if(!site_config.getForwards().isEmpty())
            log.warn(local_addr + ": forwarding routes are currently not supported and will be ignored. This will change " +
                       "with hierarchical routing (https://issues.redhat.com/browse/JGRP-1506)");

        if(enable_address_tagging) {
            JChannel ch=getProtocolStack().getChannel();
            ch.addAddressGenerator(() -> {
                ExtendedUUID retval=ExtendedUUID.randomUUID();
                if(can_become_site_master)
                    retval.setFlag(can_become_site_master_flag);
                return retval;
            });
        }
    }


    public void stop() {
        super.stop();
        is_site_master=false;
        log.trace(local_addr + ": ceased to be site master; closing bridges");
        if(relayer != null)
            relayer.stop();
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
        if(relayer != null) // I'm a site-master
            return _printTopology(relayer);
        Address site_master=site_masters != null && !site_masters.isEmpty()? site_masters.get(0) : null;
        return site_master == null? "not site-master" : fetchTopoFromSiteMaster(site_master);
    }

    @ManagedOperation(description="Prints the topology (site masters and local members) of this site")
    public String printLocalTopology() {
        StringBuilder sb=new StringBuilder(site).append("\n");
        for(Address mbr: members) {
            sb.append("  ").append(mbr);
            PhysicalAddress pa=getPhysicalAddress(mbr);
            if(pa != null)
                sb.append(String.format(" (%s)", pa));
            if(Objects.equals(mbr, local_addr))
                sb.append(" (me)");
            if(site_masters.contains(mbr))
                sb.append(" // site master");
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Returns the bridge channel to a given site
     * @param site_name The site name, e.g. "SFO"
     * @return The JChannel to the given site, or null if no route was found or we're not the coordinator
     */
    public JChannel getBridge(String site_name) {
        Relayer2 tmp=relayer;
        Route route=tmp != null? tmp.getRoute(site_name): null;
        return route != null? route.bridge() : null;
    }

    /**
     * Returns the route to a given site
     * @param site_name The site name, e.g. "SFO"
     * @return The route to the given site, or null if no route was found or we're not the coordinator
     */
    public Route getRoute(String site_name) {
        Relayer2 tmp=relayer;
        return tmp != null? tmp.getRoute(site_name): null;
    }

    /**
     * @return A {@link List} of sites name that are currently up or {@code null} if this node is not a Site Master (i.e.
     * {@link #isSiteMaster()} returns false).
     */
    public List<String> getCurrentSites() {
        Relayer2 rel = relayer;
        return rel == null ? null : rel.getSiteNames();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView(evt.getArg());
                break;
        }
        return down_prot.down(evt);
    }


    public Object down(Message msg) {
        Address dest=msg.getDest();
        if(!(dest instanceof SiteAddress))
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
                forwardTo(local_addr, target, sender, msg, false);
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
        RelayHeader hdr=msg.getHeader(id);
        Address dest=msg.getDest();

        if(hdr == null) {
            // forward a multicast message to all bridges except myself, then pass up
            if(dest == null && is_site_master && relay_multicasts && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                Address src=msg.getSrc();
                SiteUUID sender=new SiteUUID((UUID)msg.getSrc(), NameCache.get(msg.getSrc()), site);
                if(src instanceof ExtendedUUID)
                    sender.addContents((ExtendedUUID)src);
                sendToBridges(sender, msg, site);
            }
            return up_prot.up(msg); // pass up
        }
        if(handleAdminMessage(hdr, msg.src()))
            return null;
        if(dest != null)
            handleMessage(hdr, msg);
        else
            deliver(null, hdr.original_sender, msg);
        return null;
    }

    public void up(MessageBatch batch) {
        List<SiteAddress> unreachable_sites=null;
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            RelayHeader hdr=msg.getHeader(id);
            Address dest=msg.getDest();

            if(hdr == null) {
                // forward a multicast message to all bridges except myself, then pass up
                if(dest == null && is_site_master && relay_multicasts && !msg.isFlagSet(Message.Flag.NO_RELAY)) {
                    Address src=msg.getSrc();
                    SiteUUID sender=new SiteUUID((UUID)msg.getSrc(), NameCache.get(msg.getSrc()), site);
                    if(src instanceof ExtendedUUID)
                        sender.addContents((ExtendedUUID)src);
                    sendToBridges(sender, msg, site);
                }
            }
            else { // header is not null
                if(handleAdminMessage(hdr, batch.sender())) {
                    it.remove();
                    continue;
                }
                it.remove(); // message is consumed
                if(dest != null) {
                    if(hdr.getType() == RelayHeader.SITE_UNREACHABLE) {
                        SiteAddress site_addr=(SiteAddress)hdr.final_dest;
                        String site_name=site_addr.getSite();
                        if(unreachable_sites == null)
                            unreachable_sites=new ArrayList<>();
                        boolean contains=unreachable_sites.stream().anyMatch(sa -> sa.getSite().equals(site_name));
                        if(!contains)
                            unreachable_sites.add(site_addr);
                    }
                    else
                        handleMessage(hdr, msg);
                }
                else
                    deliver(null, hdr.original_sender, msg);
            }
        }
        if(unreachable_sites != null) {
            for(SiteAddress sa: unreachable_sites)
                triggerSiteUnreachableEvent(sa); // https://issues.redhat.com/browse/JGRP-2586
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
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
            relayer=new Relayer2(this, log);
            final Relayer2 tmp=relayer;
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
        if(suppress_log_no_route != null)
            suppress_log_no_route.removeExpired(suppress_time_no_route_errors);
    }


    /** Called to handle a message received by the relayer */
    protected void handleRelayMessage(Message msg) {
        RelayHeader hdr=msg.getHeader(id);
        if(hdr == null) {
            log.warn("%s: received a message without a relay header; discarding it", local_addr);
            return;
        }
        if(hdr.final_dest != null) {
            Message message=msg;
            RelayHeader header=hdr;

            if(header.type == RelayHeader.DATA && can_forward_local_cluster) {
                SiteUUID site_uuid=(SiteUUID)hdr.final_dest;

                //  If configured to do so, we want to load-balance these messages,
                UUID tmp=(UUID)Util.pickRandomElement(members);
                SiteAddress final_dest=new SiteUUID(tmp, site_uuid.getName(), site_uuid.getSite());

                // If we select a different address to handle this message, we handle it here.
                if(!final_dest.equals(hdr.final_dest)) {
                    message=copy(msg);
                    header=new RelayHeader(RelayHeader.DATA, final_dest, hdr.original_sender );
                    message.putHeader(id, header);
                }
            }
            handleMessage(header, message);
        }
        else {
            Message copy=copy(msg).setDest(null).setSrc(null).putHeader(id, hdr);
            down_prot.down(copy); // multicast locally

            // Don't forward: https://issues.redhat.com/browse/JGRP-1519
            // sendToBridges(msg.getSrc(), buf, from_site, site_id);  // forward to all bridges except self and from
        }
    }

    /** Handles SITES_UP/SITES_DOWN/TOPO_REQ/TOPO_RSP messages */
    protected boolean handleAdminMessage(RelayHeader hdr, Address sender) {
        switch(hdr.type) {
            case RelayHeader.SITES_UP:
            case RelayHeader.SITES_DOWN:
                if(route_status_listener != null) {
                    Set<String> tmp_sites=new HashSet<>();
                    if(hdr.hasSites())
                        tmp_sites.addAll(hdr.getSites());
                    if(!tmp_sites.isEmpty()) {
                        String[] t=tmp_sites.toArray(new String[]{});
                        if(hdr.type == RelayHeader.SITES_UP)
                            route_status_listener.sitesUp(t);
                        else
                            route_status_listener.sitesDown(t);
                    }
                }
                return true;
            case RelayHeader.TOPO_REQ:
                Message topo_rsp=new EmptyMessage(sender)
                  .putHeader(id, new RelayHeader(RelayHeader.TOPO_RSP).addToSites(_printTopology(relayer)));
                down_prot.down(topo_rsp);
                return true;
            case RelayHeader.TOPO_RSP:
                String rsp=hdr.sites != null && !hdr.sites.isEmpty()? hdr.sites.iterator().next() : null;
                topo_collector.add(sender, rsp);
                return true;
        }
        return false;
    }


    /** Called to handle a message received by the transport */
    protected void handleMessage(RelayHeader hdr, Message msg) {
        switch(hdr.type) {
            case RelayHeader.DATA:
                route((SiteAddress)hdr.final_dest, (SiteAddress)hdr.original_sender, msg);
                break;
            case RelayHeader.SITE_UNREACHABLE:
                triggerSiteUnreachableEvent((SiteAddress)hdr.final_dest);
                break;
        }
    }


    /**
     * Routes the message to the target destination, used by a site master (coordinator)
     *
     * @param dest   the destination site address
     * @param sender the address of the sender
     * @param msg    The message
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
        Relayer2 tmp=relayer;
        if(tmp == null) {
            log.warn(local_addr + ": not site master; dropping message");
            return;
        }

        Route route=tmp.getRoute(target_site, sender);
        if(route == null) {
            if(suppress_log_no_route != null)
                suppress_log_no_route.log(SuppressLog.Level.error, target_site, suppress_time_no_route_errors, sender, target_site);
            else
                log.error(Util.getMessage("RelayNoRouteToSite"), local_addr, target_site);
            sendSiteUnreachableTo(msg.getSrc(), target_site);
        }
        else
            route.send(dest,sender,msg);
    }


    /** Sends the message via all bridges excluding the excluded_sites bridges */
    protected void sendToBridges(Address sender, final Message msg, String ... excluded_sites) {
        Relayer2 tmp=relayer;
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
        Message msg=new EmptyMessage(src).setFlag(Message.Flag.OOB)
          .putHeader(id,new RelayHeader(RelayHeader.SITE_UNREACHABLE,new SiteMaster(target_site),null));
        down_prot.down(msg);
    }

    protected void forwardTo(Address next_dest, SiteAddress final_dest, Address original_sender, final Message msg,
                             boolean forward_to_current_coord) {
        if(log.isTraceEnabled())
            log.trace(local_addr + ": forwarding message to final destination " + final_dest + " to " +
                        (forward_to_current_coord? " the current coordinator" : next_dest));
        Message copy=copy(msg).setDest(next_dest).setSrc(null);
        RelayHeader hdr=new RelayHeader(RelayHeader.DATA, final_dest, original_sender);
        copy.putHeader(id,hdr);
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
            Message copy=copy(msg).setDest(dest).setSrc(sender);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": delivering message from " + sender);
            up_prot.up(copy);
        }
        catch(Exception e) {
            log.error(Util.getMessage("FailedDeliveringMessage"), e);
        }
    }


    protected void startRelayer(Relayer2 rel, String bridge_name) {
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
    protected List<Address> determineSiteMasters(View view, int max_num_site_masters) {
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


    protected String _printTopology(Relayer2 rel) {
        Map<Address,String> local_sitemasters=new HashMap<>();
        List<String> all_sites=rel.getSiteNames();
        List<Supplier<Boolean>> topo_reqs=new ArrayList<>();
        for(String site_name: all_sites) {
            Route r=rel.getRoute(site_name);
            JChannel bridge=r.bridge();
            Address site_master=r.site_master;
            if(Objects.equals(site_master, r.bridge().getAddress()))
                local_sitemasters.put(site_master, printLocalTopology());
            else {
                local_sitemasters.put(site_master, null);
                topo_reqs.add(() -> sendTopoReq(bridge, site_master));
            }
        }
        topo_collector.reset(local_sitemasters.keySet());
        local_sitemasters.entrySet().stream().filter(e -> e.getValue() != null)
          .forEach(e -> topo_collector.add(e.getKey(), e.getValue()));
        topo_reqs.forEach(Supplier::get);

        topo_collector.waitForAllResponses(topo_wait_time);
        Map<Address,String> rsps=topo_collector.getResults();
        if(rsps == null || rsps.isEmpty())
            return "n/a";
        return String.join("\n", rsps.values());
    }

    protected boolean sendTopoReq(JChannel bridge, Address dest) {
        Message topo_req=new EmptyMessage(dest).putHeader(id, new RelayHeader(RelayHeader.TOPO_REQ));
        try {
            bridge.send(topo_req);
            return true;
        }
        catch(Exception e) {
            log.warn("%s: failed sending TOPO-REQ message to %s: %s", bridge.getAddress(), dest, e);
            return false;
        }
    }

    protected String fetchTopoFromSiteMaster(Address sm) {
        topo_collector.reset(sm);
        Message topo_req=new EmptyMessage(sm).putHeader(id, new RelayHeader(RelayHeader.TOPO_REQ));
        down_prot.down(topo_req);
        topo_collector.waitForAllResponses(topo_wait_time);
        Map<Address,String> rsps=topo_collector.getResults();
        return rsps != null? rsps.get(sm) : null;
    }

    private void triggerSiteUnreachableEvent(SiteAddress remoteSite) {
        up_prot.up(new Event(Event.SITE_UNREACHABLE, remoteSite));
    }

/*    public static class Relay2Header extends Header {
        public static final byte DATA             = 1;
        public static final byte SITE_UNREACHABLE = 2; // final_dest is a SiteMaster
        public static final byte HOST_UNREACHABLE = 3; // final_dest is a SiteUUID (not currently used)
        public static final byte SITES_UP         = 4;
        public static final byte SITES_DOWN       = 5;
        public static final byte TOPO_REQ         = 6;
        public static final byte TOPO_RSP         = 7;

        protected byte     type;
        protected Address  final_dest;
        protected Address  original_sender;
        protected String[] sites; // used with SITES_UP/SITES_DOWN/TOPO_RSP


        public Relay2Header() {
        }

        public Relay2Header(byte type) {
            this.type=type;
        }

        public Relay2Header(byte type, Address final_dest, Address original_sender) {
            this(type);
            this.final_dest=final_dest;
            this.original_sender=original_sender;
        }
        public short getMagicId() {return 80;}
        public Supplier<? extends Header> create() {return Relay2Header::new;}
        public byte    getType()           {return type;}
        public Address getFinalDest()      {return final_dest;}
        public Address getOriginalSender() {return original_sender;}

        public Relay2Header setSites(String ... s) {
            sites=s;
            return this;
        }

        public String[] getSites() {
            return sites;
        }

        @Override
        public int serializedSize() {
            return Global.BYTE_SIZE + Util.size(final_dest) + Util.size(original_sender) + sizeOf(sites);
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeByte(type);
            Util.writeAddress(final_dest, out);
            Util.writeAddress(original_sender, out);
            out.writeInt(sites == null? 0 : sites.length);
            if(sites != null) {
                for(String s: sites)
                    Bits.writeString(s, out);
            }
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            final_dest=Util.readAddress(in);
            original_sender=Util.readAddress(in);
            int num_elements=in.readInt();
            if(num_elements == 0)
                return;
            sites=new String[num_elements];
            for(int i=0; i < sites.length; i++)
                sites[i]=Bits.readString(in);
        }

        public String toString() {
            return typeToString(type) + " [dest=" + final_dest + ", sender=" + original_sender +
              (type == TOPO_RSP? ", topos=" : ", sites=") + Arrays.toString(sites) + "]";
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case DATA:             return "DATA";
                case SITE_UNREACHABLE: return "SITE_UNREACHABLE";
                case HOST_UNREACHABLE: return "HOST_UNREACHABLE";
                case SITES_UP:         return "SITES_UP";
                case SITES_DOWN:       return "SITES_DOWN";
                case TOPO_REQ:         return "TOPO_REQ";
                case TOPO_RSP:         return "TOPO_RSP";
                default:               return "<unknown>";
            }
        }

        protected static int sizeOf(String[] arr) {
            int retval=Global.INT_SIZE; // number of elements
            if(arr != null) {
                for(String s: arr)
                    retval+=Bits.sizeUTF(s) + 1; // presence bytes
            }
            return retval;
        }
    }*/
}

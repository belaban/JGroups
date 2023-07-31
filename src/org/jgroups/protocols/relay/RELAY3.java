package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.*;
import org.jgroups.protocols.relay.SiteAddress.Type;
import org.jgroups.protocols.relay.SiteStatus.Status;
import org.jgroups.protocols.relay.Topology.MemberInfo;
import org.jgroups.protocols.relay.Topology.Members;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.util.*;
import java.util.function.Supplier;

import static org.jgroups.protocols.relay.RelayHeader.*;

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
//@XmlInclude(schema="relay.xsd",type=XmlInclude.Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
//@XmlElement(name="RelayConfiguration",type="relay:RelayConfigurationType")
@MBean(description="RELAY3 protocol")
public class RELAY3 extends RELAY {
    protected volatile Relayer3                        relayer;

    // to prevent duplicate sitesUp()/sitesDown() notifications; this is needed in every member: routes are only
    // maintained by site masters (relayer != null)
    // todo: replace with topo once JGRP-2706 is in place
    @ManagedAttribute(description="A cache maintaining a list of sites that are up")
    protected final SiteStatus                         site_status=new SiteStatus();

    public SiteStatus siteStatus()                     {return site_status;}

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
        Relayer3 tmp=relayer;
        return tmp != null? tmp.getBridgeView(cluster_name) : null;
    }

    @Override
    public void configure() throws Exception {
        super.configure();
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
    }


    public void stop() {
        super.stop();
        is_site_master=false;
        log.trace("%s: ceased to be site master; closing bridges", local_addr);
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
        Relayer3 tmp=relayer;
        return tmp != null? tmp.getRoute(site_name): null;
    }

    /**
     * @return A {@link List} of sites name that are currently up or {@code null} if this node is not a Site Master (i.e.
     * {@link #isSiteMaster()} returns false).
     */
    public List<String> getCurrentSites() {
        Relayer3 rel=relayer;
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
        RelayHeader hdr=msg.getHeader(id);
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
            RelayHeader hdr=msg.getHeader(id);
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
        Message rsp=new ObjectMessage(dest, m).putHeader(this.id, new RelayHeader(TOPO_RSP));
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
            relayer=new Relayer3(this, log);
            final Relayer3 tmp=relayer;
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
        return String.format("%s%s", getClass().getSimpleName(), local_addr != null? String.format(" (%s)", local_addr) : "");
    }

    /** Called to handle a message received from a different site (via a bridge channel) */
    protected void handleRelayMessage(Message msg) {
        RelayHeader hdr=msg.getHeader(id);
        if(hdr == null) {
            log.warn("%s: received a message without a relay header; discarding it", local_addr);
            return;
        }
        Message copy=copy(msg).dest(hdr.final_dest).src(hdr.original_sender).putHeader(id, hdr);
        // todo: check if copy is needed!
        process(true, copy);
    }

    /** Handles SITES_UP/SITES_DOWN/TOPO_REQ/TOPO_RSP messages */
    protected boolean handleAdminMessage(RelayHeader hdr, Message msg) {
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
        final Relayer3 r=relayer;
        if(r == null) {
            log.warn("%s: not site master; dropping message", local_addr);
            return null;
        }
        if(sites == null)
            sites=new ArrayList<>(r.routes.keySet());
        sites.remove(this.site);
        if(sites.isEmpty()) // no sites in routing table
            return null;
        RelayHeader hdr=msg.getHeader(this.id);
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
        RelayHeader tmp=msg.getHeader(this.id);
        // todo: check if copy is needed here
        RelayHeader hdr=tmp != null? tmp.copy().setOriginalSender(original_sender).setFinalDestination(final_dest)
          : new RelayHeader(DATA, final_dest, original_sender);
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
        RelayHeader hdr=msg.getHeader(this.id);
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
          .putHeader(id, new RelayHeader(SITE_UNREACHABLE).addToSites(target_site));
        down(msg);
    }


    protected void startRelayer(Relayer3 rel, String bridge_name) {
        try {
            log.trace(local_addr + ": became site master; starting bridges");
            rel.start(site_config, bridge_name, site);
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

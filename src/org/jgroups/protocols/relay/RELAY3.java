package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.Message.Flag;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.relay.SiteAddress.Type;
import org.jgroups.protocols.relay.SiteStatus.Status;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.util.UUID;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
@MBean(description="RELAY3 protocol")
public class RELAY3 extends RELAY {
    // to prevent duplicate sitesUp()/sitesDown() notifications; this is needed in every member: routes are only
    // maintained by site masters (relayer != null)
    // todo: replace with topo once JGRP-2706 is in place
    @ManagedAttribute(description="A cache maintaining a list of sites that are up")
    protected final SiteStatus                         site_status=new SiteStatus();

    public SiteStatus siteStatus()                     {return site_status;}

    @Override public void configure() throws Exception {
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


    public Object down(Message msg) {
        //if(msg.isFlagSet(Flag.NO_RELAY))
          //  return down_prot.down(msg);
        msg.src(local_addr);
        return process(true, msg);
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

    /**
     * Returns information about all members of all sites, or only the local members of a given site
     * @param all_sites When true, return information about all sites (1 msg/site), otherwise only about this site
     * @param dest The address to which to send the response
     */
    protected void sendResponseTo(Address dest, boolean all_sites) {
        if(all_sites) {
            sendResponsesForAllSites(dest);
            return;
        }
        if(log.isDebugEnabled())
            log.debug("%s: sending topo response to %s: %s", local_addr, dest, view);
        RelayHeader hdr=new RelayHeader(TOPO_RSP, dest, local_addr).addToSites(this.site);
        Message rsp=new ObjectMessage(dest, this.view).putHeader(this.id, hdr);
        down(rsp);
    }

    protected void sendResponsesForAllSites(Address dest) {
        for(Map.Entry<String,View> e: topo.cache().entrySet()) {
            String site_name=e.getKey();
            View v=e.getValue();
            if(log.isDebugEnabled())
                log.debug("%s: sending topo response to %s: %s", local_addr, dest, view);
            RelayHeader hdr=new RelayHeader(TOPO_RSP, dest, local_addr).addToSites(site_name);
            Message rsp=new ObjectMessage(dest, v).putHeader(this.id, hdr);
            down(rsp);
        }
    }

    public void handleView(View view) {
        View old_view=this.view;
        this.view=view;
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
            final Relayer3 tmp=(Relayer3)relayer;
            final long start=System.currentTimeMillis();
            if(async_relay_creation)
                timer.execute(() -> startRelayer(tmp, bridge_name)
                  .handleAsync((r,t) -> handleRelayerStarted(relayer, start, r, t)));
            else
                startRelayer(tmp, bridge_name)
                  .handleAsync((r,t) -> handleRelayerStarted(relayer, start, r, t));
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
        topo.put(site, view);

        if(!topo.globalViews())
            return;

        if(is_site_master) {
            if(!become_site_master) {
                // send my own information as a TOPO_RSP to all site masters, who then forward it locally with NO_RELAY
                sendResponseTo(new SiteMaster(null), false);
            }
        }
        else { // not site master
            // on the first view and as non site master -> fetch the cache from the site master
            if(old_view == null && !cease_site_master) {
                topo.refresh(this.site, true);
                // CompletableFuture.runAsync(() -> topo.refresh(this.site, true));
            }
        }
    }

    protected <T> Object handleRelayerStarted(Relayer r, long start, T ignored, Throwable t) {
        if(t != null)
            log.error(local_addr + ": failed starting relayer", t);
        else {
            log.info("%s: relayer was started in %d ms: %s", local_addr, System.currentTimeMillis() - start, r);
            if(topo.globalViews()) {
                // get the caches from all site masters
                topo.refresh(null);
                // send my own information as a TOPO_RSP to all site masters, who then forward it locally with NO_RELAY
                sendResponseTo(new SiteMaster(null), false);
            }
        }
        return null;
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
                    else {
                        if(topo.globalViews()) {
                            for(String s : tmp_sites)
                                topo.refresh(s, true);
                        }
                    }
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
                sendResponseTo(msg.src(), hdr.returnEntireCache());
                return true;
            case TOPO_RSP:
                View v=msg.getObject();
                String site_name=Objects.requireNonNull(hdr.getSite());
                if(v != null) {
                    topo.put(site_name, v);
                    if(!topo.globalViews())
                        return true;
                    Address dest=msg.getDest();
                    if(dest != null && ((SiteAddress)dest).type() == Type.SM_ALL) {
                        Message local_mcast=new ObjectMessage(null, v).putHeader(id, hdr);
                        deliver(null, local_mcast, true, true);
                    }
                }
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

    /** This method has all of the routing logic, for both site masters and regular members */
    protected Object process(boolean down, Message msg) {
        Address dest=msg.dest();
        SiteAddress dst=null;
        Type type=dest == null? Type.ALL : (dst=(SiteAddress)dest).type();
        if(is_site_master) {
            switch(type) {
                case ALL:
                    if(down)
                        return routeThen(msg, null,() -> deliver(null, msg, true));
                    return dontRoute(msg)? passUp(msg) : routeThen(msg, null, () -> passUp(msg));
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
     * Determines if a message should be routed. If NO_RELAY is set, then the message won't be routed. If we have
     * multiple site masters, and this site master is picked to route the message, then return true, else return false.
     * JIRA: https://issues.redhat.com/browse/JGRP-2696
     */
    protected boolean dontRoute(Message msg) {
        if(msg.isFlagSet(Flag.NO_RELAY))
            return true; // don't route
        final List<Address> sms=site_masters;
        if(sms == null || sms.size() < 2)
            return false; // do route
        Address first_sm=sms.get(0);
        return local_addr.equals(first_sm);
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
        return deliver(next_dest, msg, dont_relay, false);
    }

    protected Object deliver(Address next_dest, Message msg, boolean dont_relay, boolean dont_loopback) {
        checkLocalAddress(next_dest);
        Address final_dest=msg.dest(), original_sender=msg.src();
        if(log.isTraceEnabled())
            log.trace("%s: forwarding message to final destination %s to %s", local_addr, final_dest, next_dest);
        RelayHeader tmp=msg.getHeader(this.id);
        // todo: check if copy is needed here
        RelayHeader hdr=tmp != null? tmp.copy().setOriginalSender(original_sender).setFinalDestination(final_dest)
          : new RelayHeader(DATA, final_dest, original_sender);
        Message copy=copy(msg).setDest(next_dest).setSrc(null).putHeader(id, hdr);
        if(dont_relay)
            copy.setFlag(Flag.NO_RELAY);
        if(dont_loopback)
            copy.setFlag(Message.TransientFlag.DONT_LOOPBACK);
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


    protected CompletableFuture<Relayer> startRelayer(Relayer3 rel, String bridge_name) {
        try {
            log.trace(local_addr + ": became site master; starting bridges");
            return rel.start(site_config, bridge_name, site);
        }
        catch(Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

}

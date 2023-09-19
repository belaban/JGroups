package org.jgroups.protocols.relay;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
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
@MBean(description="RELAY2 protocol")
public class RELAY2 extends RELAY {

    /* ------------------------------------------    Properties     ---------------------------------------------- */
    @Property(description="Whether or not we generate our own addresses in which we use can_become_site_master. " +
      "If this property is false, can_become_site_master is ignored")
    protected boolean                                  enable_address_tagging;

    @Property(description="Whether or not to relay multicast (dest=null) messages")
    protected boolean                                  relay_multicasts=true;

    /* ---------------------------------------------    Fields    ------------------------------------------------ */
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
            final Relayer2 tmp=(Relayer2)relayer;
            if(async_relay_creation)
                timer.execute(() -> startRelayer(tmp, bridge_name));
            else
                startRelayer(tmp, bridge_name);
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
        try {
            msg.clearHeaders(); // remove all headers added by the bridge cluster
            msg.putHeader(id, hdr);
            ((BaseMessage)msg).readHeaders(hdr.originalHeaders());
        }
        catch(Exception ex) {
            log.error("%s: failed handling message relayed from %s: %s", local_addr, msg.src(), ex);
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
        Relayer tmp=relayer;
        if(tmp == null) {
            log.warn(local_addr + ": not site master; dropping message");
            return;
        }

        Route route=tmp.getRoute(target_site, sender);
        if(route == null) {
            if(suppress_log_no_route != null)
                suppress_log_no_route.log(SuppressLog.Level.error, target_site, suppress_time_no_route_errors,
                                          local_addr, sender, dest);
            else
                log.error(Util.getMessage("RelayNoRouteToSite"), local_addr, target_site);
            sendSiteUnreachableTo(msg.getSrc(), target_site);
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

    protected String _printTopology(Relayer rel) {
        Map<Address,String> local_sitemasters=new HashMap<>();
        Collection<String> all_sites=rel.getSiteNames();
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

}

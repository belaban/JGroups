
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;
import org.jgroups.util.Tuple;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


/**
 * The TCPPING protocol defines a static cluster membership. The cluster members are retrieved by
 * directly contacting the members listed in initial_hosts, sending point-to-point discovery requests.
 * <p/>
 * The TCPPING protocol defines a static configuration, which requires that you to know in advance where to find all
 * of the members of your cluster.
 *
 * @author Bela Ban
 */
public class TCPPING extends Discovery {

    /* -----------------------------------------    Properties     --------------------------------------- */

    @Property(description="Number of additional ports to be probed for membership. A port_range of 0 does not " +
      "probe additional ports. Example: initial_hosts=A[7800] port_range=0 probes A:7800, port_range=1 probes " +
      "A:7800 and A:7801")
    protected int port_range=1;

    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership. " +
      "Ideally, all members should be listed. If this is not possible, send_cache_on_join and / or return_entire_cache " +
      "can be set to true",
      converter=PropertyConverters.InitialHosts.class, dependsUpon="port_range",
      systemProperty=Global.TCPPING_INITIAL_HOSTS)
    protected List<PhysicalAddress> initial_hosts=Collections.emptyList();

    @Property(description="max number of hosts to keep beyond the ones in initial_hosts")
    protected int max_dynamic_hosts=2000;
    /* --------------------------------------------- Fields ------------------------------------------------------ */


    /** https://jira.jboss.org/jira/browse/JGRP-989 */
    protected BoundedList<PhysicalAddress> dynamic_hosts;



    public TCPPING() {
    }

    public boolean isDynamic() {
        return false;
    }

    /**
     * Returns the list of initial hosts as configured by the user via XML. Note that the returned list is mutable, so
     * careful with changes !
     * @return List<Address> list of initial hosts. This variable is only set after the channel has been created and
     * set Properties() has been called
     */
    public List<PhysicalAddress> getInitialHosts() {
        return initial_hosts;
    }


    /** @deprecated Use {@link #setInitialHosts(Collection)} instead (will later get renamed to setInitialHosts()) */
    @Deprecated public void setInitialHosts(List<PhysicalAddress> initial_hosts) {
        this.initial_hosts=initial_hosts;
    }

    public void setInitialHosts(Collection<InetSocketAddress> hosts) {
        if(hosts == null || hosts.isEmpty())
            return;
        initial_hosts=hosts.stream().map(h -> new IpAddress(h.getAddress(), h.getPort())).collect(Collectors.toList());
    }

    public int getPortRange() {
        return port_range;
    }

    public void setPortRange(int port_range) {
        this.port_range=port_range;
    }

    @ManagedAttribute
    public String getDynamicHostList() {
        return dynamic_hosts.toString();
    }

    @ManagedOperation
    public void clearDynamicHostList() {
        dynamic_hosts.clear();
    }

    public void init() throws Exception {
        super.init();
        dynamic_hosts=new BoundedList<>(max_dynamic_hosts);
    }

    public Object down(Event evt) {
        Object retval=super.down(evt);
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                for(Address logical_addr: view.getMembersRaw()) {
                    PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, logical_addr));
                    if(physical_addr != null && !initial_hosts.contains(physical_addr)) {
                        dynamic_hosts.addIfAbsent(physical_addr);
                    }
                }
                break;
            case Event.ADD_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=evt.getArg();
                PhysicalAddress physical_addr=tuple.getVal2();
                if(physical_addr != null && !initial_hosts.contains(physical_addr))
                    dynamic_hosts.addIfAbsent(physical_addr);
                break;
        }
        return retval;
    }

    public void discoveryRequestReceived(Address sender, String logical_name, PhysicalAddress physical_addr) {
        super.discoveryRequestReceived(sender, logical_name, physical_addr);
        if(physical_addr != null && !initial_hosts.contains(physical_addr))
            dynamic_hosts.addIfAbsent(physical_addr);
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        PingData        data=null;
        PhysicalAddress physical_addr=null;

        if(!use_ip_addrs || !initial_discovery) {
            physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

            // https://issues.jboss.org/browse/JGRP-1670
            data=new PingData(local_addr, false, NameCache.get(local_addr), physical_addr);
            if(members != null && members.size() <= max_members_in_discovery_request)
                data.mbrs(members);
        }

        List<PhysicalAddress> cluster_members=new ArrayList<>(initial_hosts.size() + (dynamic_hosts != null? dynamic_hosts.size() : 0) + 5);
        initial_hosts.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);

        if(dynamic_hosts != null)
            dynamic_hosts.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);

        if(use_disk_cache) {
            // this only makes sense if we have PDC below us
            Collection<PhysicalAddress> list=(Collection<PhysicalAddress>)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESSES));
            if(list != null)
                list.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);
        }

        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name).initialDiscovery(initial_discovery);
        for(final PhysicalAddress addr: cluster_members) {
            if(physical_addr != null && addr.equals(physical_addr)) // no need to send the request to myself
                continue;

            // the message needs to be DONT_BUNDLE, see explanation above
            final Message msg=new Message(addr).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE, Message.Flag.OOB)
              .putHeader(this.id,hdr);
            if(data != null)
                msg.setBuffer(marshal(data));

            if(async_discovery_use_separate_thread_per_request)
                timer.execute(() -> sendDiscoveryRequest(msg), sends_can_block);
            else
                sendDiscoveryRequest(msg);
        }
    }

    protected void sendDiscoveryRequest(Message req) {
        try {
            log.trace("%s: sending discovery request to %s", local_addr, req.getDest());
            down_prot.down(req);
        }
        catch(Throwable t) {
            log.trace("sending discovery request to %s failed: %s", req.dest(), t);
        }
    }
}


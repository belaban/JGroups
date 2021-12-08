
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.jgroups.Message.Flag.*;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;


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

    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership. " +
      "Ideally, all members should be listed. If this is not possible, send_cache_on_join and / or return_entire_cache " +
      "can be set to true",dependsUpon="port_range",systemProperty=Global.TCPPING_INITIAL_HOSTS)
    protected String                       initial_hosts_str;

    @Property(description="Number of additional ports to be probed for membership. A port_range of 0 does not " +
      "probe additional ports. Example: initial_hosts=A[7800] port_range=0 probes A:7800, port_range=1 probes " +
      "A:7800 and A:7801")
    protected int                          port_range=1;

    @ManagedAttribute(description="True if initial hosts were set programmatically (via setInitialHosts())")
    protected boolean                      initial_hosts_set_programmatically;

    @ManagedAttribute(description="A list of unresolved hosts of initial_hosts")
    protected Collection<String>           unresolved_hosts=new HashSet<>();

    @Property(description="max number of hosts to keep beyond the ones in initial_hosts")
    protected int                          max_dynamic_hosts=2000;
    /* --------------------------------------------- Fields ------------------------------------------------------ */


    protected Collection<PhysicalAddress>  initial_hosts=new HashSet<>();

    /** https://jira.jboss.org/jira/browse/JGRP-989 */
    protected BoundedList<PhysicalAddress> dynamic_hosts;

    protected StackType stack_type=StackType.Dual;



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
    public Collection<PhysicalAddress> getInitialHosts() {
        return initial_hosts;
    }

    @ManagedAttribute(description="The list of resolved hosts:ports")
    public Collection<PhysicalAddress> getResolvedHosts() {
        return initial_hosts;
    }

    public <T extends TCPPING> T setInitialHosts(Collection<InetSocketAddress> hosts) {
        if(hosts == null || hosts.isEmpty())
            return (T)this;
        initial_hosts=hosts.stream().map(h -> new IpAddress(h.getAddress(), h.getPort())).collect(Collectors.toList());
        initial_hosts_str=hostsToStr(initial_hosts);
        initial_hosts_set_programmatically=true;
        return (T)this;
    }

    public <T extends TCPPING> T setInitialHosts2(Collection<PhysicalAddress> hosts) {
        if(hosts == null || hosts.isEmpty())
            return (T)this;
        initial_hosts=hosts;
        initial_hosts_str=hostsToStr(initial_hosts);
        initial_hosts_set_programmatically=true;
        return (T)this;
    }

    public <T extends TCPPING> T initialHosts(Collection<InetSocketAddress> h) {setInitialHosts(h); return (T)this;}

    public int getPortRange() {
        return port_range;
    }

    public <T extends TCPPING> T setPortRange(int port_range) {
        this.port_range=port_range; return (T)this;
    }

    public <T extends TCPPING> T portRange(int r) {this.port_range=r; return (T)this;}

    @ManagedAttribute
    public String getDynamicHostList() {
        return dynamic_hosts.toString();
    }

    @ManagedOperation
    public <T extends TCPPING> T clearDynamicHostList() {
        dynamic_hosts.clear(); return (T)this;
    }

    public void init() throws Exception {
        super.init();

        InetAddress bind_addr=transport.getBindAddr();
        if(bind_addr != null)
            stack_type=bind_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;

        dynamic_hosts=new BoundedList<>(max_dynamic_hosts);
        if(!initial_hosts_set_programmatically) {
            boolean all_resolved=Util.parseCommaDelimitedHostsInto(initial_hosts, unresolved_hosts, initial_hosts_str,
                                                                   port_range, stack_type);
            if(!all_resolved)
                log.warn("unable to resolve the following hostnames: %s", unresolved_hosts);
        }
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
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

        // https://issues.jboss.org/browse/JGRP-1670
        PingData data=new PingData(local_addr, false, NameCache.get(local_addr), physical_addr);
        if(members != null && members.size() <= max_members_in_discovery_request)
            data.mbrs(members);

        List<PhysicalAddress> cluster_members=new ArrayList<>(initial_hosts.size() + (dynamic_hosts != null? dynamic_hosts.size() : 0) + 5);
        initial_hosts.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);

        if(dynamic_hosts != null)
            dynamic_hosts.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);

        // check for unresolved hosts (https://issues.redhat.com/browse/JGRP-2535)
        if(!initial_hosts_set_programmatically) {
            if(!unresolved_hosts.isEmpty()) {
                unresolved_hosts.clear();
                if(Util.parseCommaDelimitedHostsInto(initial_hosts, unresolved_hosts, initial_hosts_str, port_range, stack_type))
                    log.debug("finally resolved all hosts: %s", initial_hosts);
            }
        }

        if(use_disk_cache) {
            // this only makes sense if we have PDC below us
            Collection<PhysicalAddress> list=(Collection<PhysicalAddress>)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESSES));
            if(list != null)
                list.stream().filter(phys_addr -> !cluster_members.contains(phys_addr)).forEach(cluster_members::add);
        }

        ByteArray data_buf=data != null? marshal(data) : null;
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name).initialDiscovery(initial_discovery);
        for(final PhysicalAddress addr: cluster_members) {
            if(addr.equals(physical_addr)) // no need to send the request to myself
                continue;

            // the message needs to be DONT_BUNDLE, see explanation above
            final Message msg=new BytesMessage(addr).setFlag(DONT_BUNDLE, OOB).setFlag(DONT_LOOPBACK)
              .putHeader(this.id,hdr);
            if(data_buf != null)
                msg.setArray(data_buf);

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
            log.trace("sending discovery request to %s failed: %s", req.getDest(), t);
        }
    }

    protected static String hostsToStr(Collection<PhysicalAddress> hosts) {
        if(hosts == null || hosts.isEmpty())
            return "";
        return hosts.stream().map(a -> ((IpAddress)a).printIpAddress2()).collect(Collectors.joining(","));
    }
}


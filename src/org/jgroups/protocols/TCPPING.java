
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Responses;
import org.jgroups.util.Tuple;

import java.util.*;


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
    private int port_range=1;

    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership",
        converter=PropertyConverters.InitialHosts.class, dependsUpon="port_range",
            systemProperty=Global.TCPPING_INITIAL_HOSTS)
    private List<PhysicalAddress> initial_hosts=Collections.emptyList();

    @Property(description="max number of hosts to keep beyond the ones in initial_hosts")
    protected int max_dynamic_hosts=2000;
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    /**
     * List of PhysicalAddresses
     */

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

    public void setInitialHosts(List<PhysicalAddress> initial_hosts) {
        this.initial_hosts=initial_hosts;
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

    @ManagedAttribute
    public String getInitialHostsList() {
        return initial_hosts.toString();
    }

    public void init() throws Exception {
        super.init();
        dynamic_hosts=new BoundedList<>(max_dynamic_hosts);
    }

    public Object down(Event evt) {
        Object retval=super.down(evt);
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                for(Address logical_addr: members) {
                    PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, logical_addr));
                    if(physical_addr != null && !initial_hosts.contains(physical_addr)) {
                        dynamic_hosts.addIfAbsent(physical_addr);
                    }
                }
                break;
            case Event.SET_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> tuple=(Tuple<Address,PhysicalAddress>)evt.getArg();
                PhysicalAddress physical_addr=tuple.getVal2();
                if(physical_addr != null && !initial_hosts.contains(physical_addr))
                    dynamic_hosts.addIfAbsent(physical_addr);
                break;
        }
        return retval;
    }

    public void discoveryRequestReceived(Address sender, String logical_name, PhysicalAddress physical_addr) {
        super.discoveryRequestReceived(sender, logical_name, physical_addr);
        if(physical_addr != null) {
            if(!initial_hosts.contains(physical_addr))
                dynamic_hosts.addIfAbsent(physical_addr);
        }
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));

        // https://issues.jboss.org/browse/JGRP-1670
        PingData data=new PingData(local_addr, false, org.jgroups.util.UUID.get(local_addr), physical_addr);
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name);

        List<PhysicalAddress> cluster_members=new ArrayList<>(initial_hosts.size() + (dynamic_hosts != null? dynamic_hosts.size() : 0) + 5);
        for(PhysicalAddress phys_addr: initial_hosts)
            if(!cluster_members.contains(phys_addr))
                cluster_members.add(phys_addr);
        if(dynamic_hosts != null) {
            for(PhysicalAddress phys_addr : dynamic_hosts)
                if(!cluster_members.contains(phys_addr))
                    cluster_members.add(phys_addr);
        }

        if(use_disk_cache) {
            // this only makes sense if we have PDC below us
            Collection<PhysicalAddress> list=(Collection<PhysicalAddress>)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESSES));
            if(list != null)
                for(PhysicalAddress phys_addr: list)
                    if(!cluster_members.contains(phys_addr))
                        cluster_members.add(phys_addr);
        }

        for(final PhysicalAddress addr: cluster_members) {
            if(physical_addr != null && addr.equals(physical_addr)) // no need to send the request to myself
                continue;

            // the message needs to be DONT_BUNDLE, see explanation above
            final Message msg=new Message(addr).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE, Message.Flag.OOB)
              .putHeader(this.id,hdr).setBuffer(marshal(data));

            if(async_discovery_use_separate_thread_per_request) {
                timer.execute(new Runnable() {
                    public void run() {
                        log.trace("%s: sending discovery request to %s", local_addr, msg.getDest());
                        down_prot.down(new Event(Event.MSG, msg));
                    }
                });
            }
            else {
                log.trace("%s: sending discovery request to %s", local_addr, msg.getDest());
                down_prot.down(new Event(Event.MSG, msg));
            }
        }
    }
}


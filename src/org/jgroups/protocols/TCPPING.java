
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;

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
    private List<IpAddress> initial_hosts=null;

    @Property(description="max number of hosts to keep beyond the ones in initial_hosts")
    protected int max_dynamic_hosts=100;
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    /**
     * List of PhysicalAddresses
     */

    /** https://jira.jboss.org/jira/browse/JGRP-989 */
    protected final BoundedList<PhysicalAddress> dynamic_hosts=new BoundedList<PhysicalAddress>(max_dynamic_hosts);


    
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
    public List<IpAddress> getInitialHosts() {
        return initial_hosts;
    }      
    
    public void setInitialHosts(List<IpAddress> initial_hosts) {
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
       

    public Collection<PhysicalAddress> fetchClusterMembers(String cluster_name) {
        Set<PhysicalAddress> combined_target_members=new HashSet<PhysicalAddress>(initial_hosts);
        combined_target_members.addAll(dynamic_hosts);
        return combined_target_members;
    }

    public boolean sendDiscoveryRequestsInParallel() {
        return true;
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
        }
        return retval;
    }

    public void discoveryRequestReceived(Address sender, String logical_name, Collection<PhysicalAddress> physical_addrs) {
        super.discoveryRequestReceived(sender, logical_name, physical_addrs);
        if(physical_addrs != null) {
            for(PhysicalAddress addr: physical_addrs) {
                if(!initial_hosts.contains(addr))
                    dynamic_hosts.addIfAbsent(addr);
            }
        }
    }
}


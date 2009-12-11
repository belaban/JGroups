
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Promise;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * The TCPPING protocol layer retrieves the initial membership in answer to the
 * GMS's FIND_INITIAL_MBRS event. The initial membership is retrieved by
 * directly contacting other group members, sending point-to-point mebership
 * requests. The responses should allow us to determine the coordinator whom we
 * have to contact in case we want to join the group. When we are a server
 * (after having received the BECOME_SERVER event), we'll respond to TCPPING
 * requests with a TCPPING response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * <p>
 * The TCPPING protocol requires a static conifiguration, which assumes that you
 * to know in advance where to find other members of your group. For dynamic
 * discovery, use the PING protocol, which uses multicast discovery, or the
 * TCPGOSSIP protocol, which contacts a Gossip Router to acquire the initial
 * membership.
 * 
 * @author Bela Ban
 * @version $Id: TCPPING.java,v 1.58 2009/12/11 13:10:09 belaban Exp $
 */
public class TCPPING extends Discovery {
    
    /* -----------------------------------------    Properties     --------------------------------------- */
    
    @Property(description="Number of ports to be probed for initial membership. Default is 1")
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
        return_entire_cache=true;
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

    @ManagedAttribute
    public String getInitialHostsList() {
        return initial_hosts.toString();
    }
       
    public void init() throws Exception {        
        super.init();
    }

    public void start() throws Exception {        
        super.start();
    }
    
    
    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception{
        PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), Arrays.asList(physical_addr));
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
        hdr.return_view_only=return_views_only;

        Set<PhysicalAddress> combined_target_members=new HashSet<PhysicalAddress>(initial_hosts);
        combined_target_members.addAll(dynamic_hosts);

        for(final Address addr: combined_target_members) {
            if(addr.equals(physical_addr))
                continue;
            final Message msg=new Message(addr, null, null);
            msg.setFlag(Message.OOB);
            msg.putHeader(getName(), hdr);
            if(log.isTraceEnabled())
                log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());                      
            timer.execute(new Runnable() {
                public void run() {
                    try {
                        down_prot.down(new Event(Event.MSG, msg));
                    }
                    catch(Exception ex){
                        if(log.isErrorEnabled())
                            log.error("failed sending discovery request to " + addr + ": " +  ex);
                    }
                }
            });
        }
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
}


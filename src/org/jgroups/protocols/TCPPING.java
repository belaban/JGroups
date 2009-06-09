
package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.UUID;
import org.jgroups.util.Promise;

import java.util.*;


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
 * @version $Id: TCPPING.java,v 1.47 2009/06/09 08:42:04 belaban Exp $
 */
public class TCPPING extends Discovery {
    
    private final static String NAME="TCPPING";
    
    /* -----------------------------------------    Properties     --------------------------------------- */
    
    @Property(description="Number of ports to be probed for initial membership. Default is 1")
    private int port_range=1; 
    
    @Property(name="initial_hosts", description="Comma delimeted list of hosts to be contacted for initial membership")
    private String hosts;
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    /**
     * List of PhysicalAddresses
     */
    private List<IpAddress> initial_hosts;


    public TCPPING() {
        return_entire_cache=true;
    }

    public String getName() {
        return NAME;
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
    
    public void setInitialHosts(String hosts) {
        this.hosts = hosts;
    }

    public int getPortRange() {
        return port_range;
    }

    public void setPortRange(int port_range) {
        this.port_range=port_range;
    }
       
    public void start() throws Exception {        
        super.start();
        initial_hosts = Util.parseCommaDelimitedHosts(hosts, port_range);       
    }
    
    public void sendGetMembersRequest(String cluster_name, Promise promise) throws Exception{
        PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), Arrays.asList(physical_addr));
        PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
        for(final Address addr: initial_hosts) {
            if(addr.equals(physical_addr))
                continue;
            final Message msg = new Message(addr, null, null);
            msg.setFlag(Message.OOB);
            msg.putHeader(NAME, hdr);
            if(log.isTraceEnabled())
                log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());                      
            timer.submit(new Runnable() {
                public void run() {
                    try {
                        down_prot.down(new Event(Event.MSG, msg));
                    }
                    catch(Exception ex){
                        if(log.isErrorEnabled())
                            log.error("failed sending discovery request to " + addr, ex);
                    }
                }
            });
        }
    }
}


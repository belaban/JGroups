// $Id: TCPPING.java,v 1.38 2008/05/30 15:42:28 vlada Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.util.Util;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;

import java.util.*;
import java.net.UnknownHostException;


/**
 * The TCPPING protocol layer retrieves the initial membership in answer to the GMS's
 * FIND_INITIAL_MBRS event. The initial membership is retrieved by directly contacting other group
 * members, sending point-to-point mebership requests. The responses should allow us to determine
 * the coordinator whom we have to contact in case we want to join the group. When we are a server
 * (after having received the BECOME_SERVER event), we'll respond to TCPPING requests with a TCPPING
 * response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a FIND_INITIAL_MBRS_OK event up
 * the stack.
 * <p>
 * The TCPPING protocol requires a static conifiguration, which assumes that you to know in advance
 * where to find other members of your group. For dynamic discovery, use the PING protocol, which
 * uses multicast discovery, or the TCPGOSSIP protocol, which contacts a Gossip Router to acquire
 * the initial membership.
 *
 * @author Bela Ban
 */
public class TCPPING extends Discovery {
    @Property
    int                 port_range=1;        // number of ports to be probed for initial membership

    /** List<IpAddress> */
    List<IpAddress>       initial_hosts=null;  // hosts to be contacted for the initial membership
    final static String name="TCPPING";



    public String getName() {
        return name;
    }

    /**
     * Returns the list of initial hosts as configured by the user via XML. Note that the returned list is mutable, so
     * careful with changes !
     * @return List<Address> list of initial hosts. This variable is only set after the channel has been created and
     * set Properties() has been called
     */
    @Property
    public List<IpAddress> getInitialHosts() {
        return initial_hosts;
    }

    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=Util.parseCommaDelimetedHosts(hosts, port_range);
    }


    public int getPortRange() {
        return port_range;
    }

    public void setPortRange(int port_range) {
        this.port_range=port_range;
    }

    public void init() throws Exception {
        super.init();
    }

    public void localAddressSet(Address addr) {
        // Add own address to initial_hosts if not present: we must always be able to ping ourself !
        if(initial_hosts != null && addr != null) {
            if(initial_hosts.contains(addr)) {
                List<IpAddress> tmp=new ArrayList<IpAddress>(initial_hosts);
                tmp.remove(addr);
                initial_hosts=Collections.unmodifiableList(tmp); // we cannot modify initial_hosts, so copy and modify it
                if(log.isDebugEnabled()) log.debug("[SET_LOCAL_ADDRESS]: removing my own address (" + addr +
                                                   ") from initial_hosts; initial_hosts=" + initial_hosts);
            }
        }
    }   
    
    public void sendGetMembersRequest(String cluster_name) {

        for(Iterator<IpAddress> it = initial_hosts.iterator();it.hasNext();){
            final Address addr = it.next();
            final Message msg = new Message(addr, null, null);
            msg.setFlag(Message.OOB);
            msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name));

            if(log.isTraceEnabled())
                log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());                      
            timer.submit(new Runnable() {
                public void run() {
                    try{                        
                        down_prot.down(new Event(Event.MSG, msg));
                    }catch(Exception ex){
                        if(log.isErrorEnabled())
                            log.error("failed sending discovery request to " + addr, ex);
                    }
                }
            });
        }
    }
}


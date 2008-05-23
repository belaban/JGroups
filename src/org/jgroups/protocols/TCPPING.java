// $Id: TCPPING.java,v 1.36 2008/05/23 10:45:38 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Global;
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
    List<Address>       initial_hosts=null;  // hosts to be contacted for the initial membership
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
    public List<Address> getInitialHosts() {
        return initial_hosts;
    }

    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=createInitialHosts(hosts);
    }


    public int getPortRange() {
        return port_range;
    }

    public void setPortRange(int port_range) {
        this.port_range=port_range;
    }

    public void localAddressSet(Address addr) {
        // Add own address to initial_hosts if not present: we must always be able to ping ourself !
        if(initial_hosts != null && addr != null) {
            if(initial_hosts.contains(addr)) {
                initial_hosts.remove(addr);
                if(log.isDebugEnabled()) log.debug("[SET_LOCAL_ADDRESS]: removing my own address (" + addr +
                                                   ") from initial_hosts; initial_hosts=" + initial_hosts);
            }
        }
    }   
    
    public void sendGetMembersRequest(String cluster_name) {

        for(Iterator<Address> it = initial_hosts.iterator();it.hasNext();){
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



    /* -------------------------- Private methods ---------------------------- */

    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private List<Address> createInitialHosts(String l) throws UnknownHostException {
        StringTokenizer tok=new StringTokenizer(l, ",");
        String          t;
        IpAddress       addr;
        List<Address>   retval=new ArrayList<Address>();

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken().trim();
                String host=t.substring(0, t.indexOf('['));
                host=host.trim();
                int port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                for(int i=port; i < port + port_range; i++) {
                    addr=new IpAddress(host, i);
                    retval.add(addr);
                }
            }
            catch(NumberFormatException e) {
                if(log.isErrorEnabled()) log.error("exeption is " + e);
            }
        }

        return retval;
    }

}


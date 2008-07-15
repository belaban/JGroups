
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;


/**
 * The PING protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack). We do this by mcasting PING
 * requests to an IP MCAST address (or, if gossiping is enabled, by contacting the GossipRouter).
 * The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to PING requests with a PING
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * The following properties are available
 * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipRouter, default is null
 * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipRouter, default is null
 * @author Bela Ban
 * @version $Id: PING.java,v 1.45 2008/07/15 20:39:48 vlada Exp $
 */
public class PING extends Discovery {
    
    private static final String name="PING";
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */

    
    @Property(description="Gossip host")
    private String gossip_host=null;
    
    @Property(description="Gossip port")
    private int gossip_port=0;
    
    @Property(description="Time in msecs after which the entry in GossipRouter will be refreshed. Default is 20000 msec")
    private long gossip_refresh=20000;
    
    @Property(description="Number of ports to be probed for initial membership. Default is 1")
    private int port_range=1;
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    private GossipClient client;
    
    private List<IpAddress> gossip_hosts=null;
        
    private List<IpAddress> initial_hosts=null; // hosts to be contacted for the initial membership


    public String getName() {
        return name;
    }
    
    public void init() throws Exception {
        super.init();
        if(gossip_hosts != null) {
            client=new GossipClient(gossip_hosts, gossip_refresh, 1000, timer);
        }
        else if(gossip_host != null && gossip_port != 0) {
            try {
                client=new GossipClient(new IpAddress(InetAddress.getByName(gossip_host),
                                                      gossip_port), gossip_refresh, 1000, timer);
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                    log.error("creation of GossipClient failed, exception=" + e);
                throw e;
            }
        }
    }
    
    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=Util.parseCommaDelimetedHosts(hosts, port_range);
    }

    @Property
    public void setGossipHosts(String hosts) throws UnknownHostException {
        gossip_hosts=Util.parseCommaDelimetedHosts(hosts, port_range);
    }


    public void stop() {
        super.stop();
        if(client != null) {
            client.stop();
        }
    }


    public void localAddressSet(Address addr) {
        // Add own address to initial_hosts if not present: we must always be able to ping ourself !
//        if(initial_hosts != null && addr != null) {
//            if(initial_hosts.contains(addr)) {
//                initial_hosts.remove(addr);
//                if(log.isDebugEnabled()) log.debug("[SET_LOCAL_ADDRESS]: removing my own address (" + addr +
//                        ") from initial_hosts; initial_hosts=" + initial_hosts);
//            }
//        }
    }



    public void handleConnect() {
        if(client != null)
            client.register(group_addr, local_addr);
    }

    public void handleDisconnect() {
        if(client != null)
            client.stop();
    }



    public void sendGetMembersRequest(String cluster_name) {
        Message       msg;
        PingHeader    hdr;
        List<Address> gossip_rsps;

        if(client != null) {
            gossip_rsps=client.getMembers(group_addr);
            if(gossip_rsps != null && !gossip_rsps.isEmpty()) {
                // Set a temporary membership in the UDP layer, so that the following multicast
                // will be sent to all of them
                Event view_event=new Event(Event.TMP_VIEW, makeView(new Vector<Address>(gossip_rsps)));
                down_prot.down(view_event); // needed e.g. by failure detector or UDP
            }
            else {
                //do nothing
                return;
            }

            if(!gossip_rsps.isEmpty()) {
                for(Iterator<Address> it=gossip_rsps.iterator(); it.hasNext();) {
                    Address dest=it.next();
                    msg=new Message(dest, null, null);  // unicast msg
                    msg.setFlag(Message.OOB);
                    msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name));
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }

            Util.sleep(500);
        }
        else {
            if(initial_hosts != null && !initial_hosts.isEmpty()) {
                for(Address addr: initial_hosts) {
                    if(addr.equals(local_addr))
                        continue;
                    // if(tmpMbrs.contains(addr)) {
                    // ; // continue; // changed as suggested by Mark Kopec
                    // }
                    msg=new Message(addr, null, null);
                    msg.setFlag(Message.OOB);
                    msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name));

                    if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }
            else {
                // 1. Mcast GET_MBRS_REQ message
                hdr=new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name);
                msg=new Message(null);  // mcast msg
                msg.setFlag(Message.OOB);
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                sendMcastDiscoveryRequest(msg);
            }
        }
    }

    void sendMcastDiscoveryRequest(Message discovery_request) {
        down_prot.down(new Event(Event.MSG, discovery_request));
    }
}


package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;


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
 * @version $Id: PING.java,v 1.37 2007/11/29 11:27:08 belaban Exp $
 */
public class PING extends Discovery {
    String       gossip_host=null;
    int          gossip_port=0;
    long         gossip_refresh=20000; // time in msecs after which the entry in GossipRouter will be refreshed
    GossipClient client;
    int          port_range=1;        // number of ports to be probed for initial membership
    private List<Address> initial_hosts=null;  // hosts to be contacted for the initial membership
    public static final String name="PING";


    public String getName() {
        return name;
    }



    /**
     * sets the properties of the PING protocol.
     * The following properties are available
     * property: timeout - the timeout (ms) to wait for the initial members, default is 3000=3 secs
     * property: num_initial_members - the minimum number of initial members for a FIND_INITAL_MBRS, default is 2
     * property: gossip_host - if you are using GOSSIP then this defines the host of the GossipRouter, default is null
     * property: gossip_port - if you are using GOSSIP then this defines the port of the GossipRouter, default is null
     *
     * @param props - a property set containing only PING properties
     * @return returns true if all properties were parsed properly
     *         returns false if there are unrecnogized properties in the property set
     */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("gossip_host");
        if(str != null) {
            gossip_host=str;
            props.remove("gossip_host");
        }

        str=props.getProperty("gossip_port");
        if(str != null) {
            gossip_port=Integer.parseInt(str);
            props.remove("gossip_port");
        }

        str=props.getProperty("gossip_refresh");
        if(str != null) {
            gossip_refresh=Long.parseLong(str);
            props.remove("gossip_refresh");
        }

        if(gossip_host != null && gossip_port != 0) {
            try {
                client=new GossipClient(new IpAddress(InetAddress.getByName(gossip_host), gossip_port), gossip_refresh);
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error("creation of GossipClient failed, exception=" + e);
                return false; // will cause stack creation to abort
            }
        }

        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
            port_range=Integer.parseInt(str);
            if(port_range < 1) {
                port_range=1;
            }
            props.remove("port_range");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            try {
                initial_hosts=createInitialHosts(str);
            }
            catch(UnknownHostException e) {
                if(log.isErrorEnabled())
                    log.error("failed constructing initial list of hosts", e);
                return false;
            }
        }

        return super.setProperties(props);
    }


    public void stop() {
        super.stop();
        if(client != null) {
            client.stop();
        }
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
                for(Iterator it=gossip_rsps.iterator(); it.hasNext();) {
                    Address dest=(Address)it.next();
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

    /* -------------------------- Private methods ---------------------------- */



    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private List<Address> createInitialHosts(String l) throws UnknownHostException {
        StringTokenizer tok=new StringTokenizer(l, ",");
        String          t;
        IpAddress       addr;
        java.util.List<Address> retval=new ArrayList<Address>();

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

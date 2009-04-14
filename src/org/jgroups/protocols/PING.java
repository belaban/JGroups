
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.Promise;

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
 * @version $Id: PING.java,v 1.36.2.6 2009/04/14 14:16:58 vlada Exp $
 */
public class PING extends Discovery {
    String       gossip_host=null;
    Vector<IpAddress>  gossip_hosts=null;
    int          gossip_port=0;
    long         gossip_refresh=20000; // time in msecs after which the entry in GossipRouter will be refreshed
    GossipClient client;
    int          port_range=1;        // number of ports to be probed for initial membership
    private List<Address> initial_hosts=null;  // hosts to be contacted for the initial membership
    int                   sock_conn_timeout=1000;     // max time in millis for a socket creation
    int                   sock_read_timeout=3000;     // max time in millis for a socket read
    protected final Promise<Boolean> discovery_reception=new Promise<Boolean>();
    /** Time (in ms) to wait for our own discovery message to be received. 0 means don't wait. If the
            discovery message is not received within discovery_timeout ms, a warning will be logged */
    private long discovery_timeout=0L;
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

        str=props.getProperty("gossip_hosts");
        if(str != null) {
            try {
                gossip_hosts=createInitialHosts(str);
            }
            catch(UnknownHostException e) {
                throw new IllegalArgumentException(e);
            }
            props.remove("gossip_hosts");
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

        str=props.getProperty("sock_conn_timeout");  // wait for at most n members
        if(str != null) {
            sock_conn_timeout=Integer.parseInt(str);
            props.remove("sock_conn_timeout");
        }

        str=props.getProperty("sock_read_timeout");  // wait for at most n members
        if(str != null) {
            sock_read_timeout=Integer.parseInt(str);
            props.remove("sock_read_timeout");
        }

        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
            port_range=Integer.parseInt(str);
            if(port_range < 1) {
                port_range=1;
            }
            props.remove("port_range");
        }

        str=props.getProperty("discovery_timeout");  // wait for at most n members
        if(str != null) {
            discovery_timeout=Integer.parseInt(str);
            props.remove("discovery_timeout");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            try {
                initial_hosts=new ArrayList<Address>();
                List<IpAddress> tmp=createInitialHosts(str);
                initial_hosts.addAll(tmp);
            }
            catch(UnknownHostException e) {
                if(log.isErrorEnabled())
                    log.error("failed constructing initial list of hosts", e);
                return false;
            }
        }

        return super.setProperties(props);
    }

    public int getGossipPort() {
        return gossip_port;
    }

    public void setGossipPort(int gossip_port) {
        this.gossip_port=gossip_port;
    }

    public long getGossipRefresh() {
        return gossip_refresh;
    }

    public void setGossipRefresh(long gossip_refresh) {
        this.gossip_refresh=gossip_refresh;
    }

    public void init() throws Exception {
        super.init();
        if(gossip_hosts != null) {
            client=new GossipClient(gossip_hosts, gossip_refresh);
            client.setSocketConnectionTimeout(sock_conn_timeout);
            client.setSocketReadTimeout(sock_read_timeout);
        }
        else if(gossip_host != null && gossip_port != 0) {
            client=new GossipClient(new IpAddress(InetAddress.getByName(gossip_host), gossip_port), gossip_refresh);
            client.setSocketConnectionTimeout(sock_conn_timeout);
            client.setSocketReadTimeout(sock_read_timeout);
        }
    }

    public void stop() {
        super.stop();
        if(client != null) {
            client.stop();
        }
        discovery_reception.reset();
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
        if(client != null) {
            if(group_addr != null && local_addr != null)
                client.unregister(group_addr, local_addr);
            client.stop();
        }
    }



    public void sendGetMembersRequest() throws Exception {
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
                    msg.putHeader(getName(), new PingHeader(PingHeader.GET_MBRS_REQ, null));
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
                    msg.putHeader(name, new PingHeader(PingHeader.GET_MBRS_REQ, null));

                    if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                    down_prot.down(new Event(Event.MSG, msg));
                }
            }
            else {
                // 1. Mcast GET_MBRS_REQ message
                hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
                msg=new Message(null);  // mcast msg
                msg.setFlag(Message.OOB);
                msg.putHeader(getName(), hdr); // needs to be getName(), so we might get "MPING" !
                sendMcastDiscoveryRequest(msg);
            }
        }
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            PingHeader hdr=(PingHeader)msg.getHeader(getName());
            if(hdr != null && hdr.type == PingHeader.GET_MBRS_REQ && msg.getSrc().equals(local_addr)) {
                discovery_reception.setResult(true);
            }
        }

        return super.up(evt);
    }

    void sendMcastDiscoveryRequest(Message discovery_request) {
        discovery_reception.reset();
        down_prot.down(new Event(Event.MSG, discovery_request));
        waitForDiscoveryRequestReception();
    }

    /* -------------------------- Private methods ---------------------------- */


    protected void waitForDiscoveryRequestReception() {
        if(discovery_timeout > 0) {
            try {
                discovery_reception.getResultWithTimeout(discovery_timeout);
            }
            catch(TimeoutException e) {
                if(log.isWarnEnabled())
                    log.warn("didn't receive my own discovery request - multicast socket might not be configured correctly");
            }
        }
    }


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return List of IpAddresses
     */
    private Vector<IpAddress> createInitialHosts(String l) throws UnknownHostException {
        StringTokenizer tok=new StringTokenizer(l, ",");
        String          t;
        IpAddress       addr;
        Vector<IpAddress> retval=new Vector<IpAddress>();

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

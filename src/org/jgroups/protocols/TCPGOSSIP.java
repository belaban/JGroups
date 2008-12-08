
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.util.*;
import java.net.UnknownHostException;


/**
 * The TCPGOSSIP protocol layer retrieves the initial membership (used by the
 * GMS when started by sending event FIND_INITIAL_MBRS down the stack). We do
 * this by contacting one or more GossipRouters, which must be running at
 * well-known addresses:ports. The responses should allow us to determine the
 * coordinator whom we have to contact, e.g. in case we want to join the group.
 * When we are a server (after having received the BECOME_SERVER event), we'll
 * respond to TCPGOSSIP requests with a TCPGOSSIP response.
 * <p>
 * The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 * 
 * @author Bela Ban
 * @version $Id: TCPGOSSIP.java,v 1.34 2008/12/08 13:18:58 belaban Exp $
 */
public class TCPGOSSIP extends Discovery {
    
    private final static String name="TCPGOSSIP";    

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    
    @Property(description="Rate of continious refresh registering of underlying gossip client with gossip server. Default is 20000 msec")
    long gossip_refresh_rate=20000;
    @Property(description="Max time for socket creation. Default is 1000 msec")
    int sock_conn_timeout=1000;
    @Property(description="Max time in milliseconds to block on a read. 0 blocks forever")
    int sock_read_timeout=3000;
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    List<IpAddress> initial_hosts=null; // (list of IpAddresses) hosts to be contacted for the initial membership
    GossipClient gossip_client=null; // accesses the GossipRouter(s) to find initial mbrship    

    public String getName() {
        return name;
    }
    
    public void init() throws Exception {
        super.init();
        if(initial_hosts == null || initial_hosts.isEmpty()) {
            throw new IllegalArgumentException("initial_hosts must contain the address of at least one GossipRouter");
        }

        if(timeout <= sock_conn_timeout) {
            throw new IllegalArgumentException("timeout (" + timeout
                                               + ") must be greater than sock_conn_timeout ("
                                               + sock_conn_timeout
                                               + ")");
        }
    }
    
    @Property
    public void setInitialHosts(String hosts) throws UnknownHostException {
        initial_hosts=Util.parseCommaDelimetedHosts(hosts,1);       
    }

    public void start() throws Exception {
        super.start();
        if(gossip_client == null) {
            gossip_client=new GossipClient(initial_hosts, gossip_refresh_rate, sock_conn_timeout, timer);
            gossip_client.setSocketReadTimeout(sock_read_timeout);
        }
    }

    public void stop() {
        super.stop();
        if(gossip_client != null) {
            gossip_client.stop();
            gossip_client=null;
        }
    }

    public void destroy() {
        if(gossip_client != null) {
            gossip_client.destroy();
            gossip_client=null;
        }
    }


    public void handleConnect() {
        if(group_addr == null || local_addr == null) {
            if(log.isErrorEnabled())
                log.error("group_addr or local_addr is null, cannot register with GossipRouter(s)");
        }
        else {
            if(log.isTraceEnabled())
                log.trace("registering " + local_addr + " under " + group_addr + " with GossipRouter");
            gossip_client.register(group_addr, local_addr);
        }
    }

    public void handleDisconnect() {
        if(group_addr != null && local_addr != null) {
            gossip_client.unregister(group_addr, local_addr);
        }
    }

    public void sendGetMembersRequest(String cluster_name) {
        Message msg, copy;
        PingHeader hdr;
        List<Address> tmp_mbrs;
        Address mbr_addr;

        if(group_addr == null) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: group_addr is null, cannot get mbrship");            
            return;
        }
        if(log.isTraceEnabled()) log.trace("fetching members from GossipRouter(s)");
        tmp_mbrs=gossip_client.getMembers(group_addr,
                                          (long)(timeout * .50)); // needs to be below timeout
        if(tmp_mbrs == null || tmp_mbrs.isEmpty()) {
            if(log.isErrorEnabled()) log.error("[FIND_INITIAL_MBRS]: gossip client found no members");           
            return;
        }
        if(log.isTraceEnabled()) log.trace("consolidated mbrs from GossipRouter(s) are " + tmp_mbrs);

        // 1. 'Mcast' GET_MBRS_REQ message
        hdr=new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name);
        msg=new Message(null);
        msg.setFlag(Message.OOB);
        msg.putHeader(name, hdr);

        for(Iterator<Address> it=tmp_mbrs.iterator(); it.hasNext();) {
            mbr_addr=it.next();
            copy=msg.copy();
            copy.setDest(mbr_addr);
            if(log.isTraceEnabled()) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + copy.getDest());
            down_prot.down(new Event(Event.MSG, copy));
        }
    }
}


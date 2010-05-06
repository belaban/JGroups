
package org.jgroups.protocols;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.DeprecatedProperty;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Promise;
import org.jgroups.util.Tuple;


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
 * @version $Id: TCPGOSSIP.java,v 1.57 2010/05/06 14:52:44 belaban Exp $
 */
@DeprecatedProperty(names={"gossip_refresh_rate"})
public class TCPGOSSIP extends Discovery {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Max time for socket creation. Default is 1000 msec")
    int sock_conn_timeout=1000;

    @Property(description="Max time in milliseconds to block on a read. 0 blocks forever")
    int sock_read_timeout=3000;

    @Property(description="Interval (ms) by which a disconnected stub attempts to reconnect to the GossipRouter")
    long reconnect_interval=10000L;
    
    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership", 
    		converter=PropertyConverters.InitialHosts2.class)
    public void setInitialHosts(List<InetSocketAddress> initial_hosts) {
        if(initial_hosts == null || initial_hosts.isEmpty())
            throw new IllegalArgumentException("initial_hosts must contain the address of at least one GossipRouter");

    	this.initial_hosts = initial_hosts ;
    }

    public List<InetSocketAddress> getInitialHosts() {
        return initial_hosts;
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    List<InetSocketAddress> initial_hosts=null; // (list of IpAddresses) hosts to be contacted for the initial membership
    
    private volatile RouterStubManager stubManager;

    public void init() throws Exception {
        super.init();
        stubManager = RouterStubManager.emptyGossipClientStubManager(this);
        if(timeout <= sock_conn_timeout)
            throw new IllegalArgumentException("timeout (" + timeout + ") must be greater than sock_conn_timeout ("
                    + sock_conn_timeout + ")");

        // we cannot use TCPGOSSIP together with TUNNEL (https://jira.jboss.org/jira/browse/JGRP-1101)
        TP transport=getTransport();
        if(transport instanceof TUNNEL)
            throw new IllegalStateException("TCPGOSSIP cannot be used with TUNNEL; use either TUNNEL:PING or " +
                    "TCP:TCPGOSSIP as valid configurations");
    }

    public void start() throws Exception {
        super.start();
    }

    public void stop() {
        super.stop();       
        stubManager.disconnectStubs();
    }

    public void destroy() {
        stubManager.destroyStubs();
        super.destroy();
    }

    public void handleConnect() {
        if (group_addr == null || local_addr == null) {
            if (log.isErrorEnabled())
                log.error("group_addr or local_addr is null, cannot register with GossipRouter(s)");
        } else {
            if (log.isTraceEnabled())
                log.trace("registering " + local_addr + " under " + group_addr + " with GossipRouter");
            
            stubManager.destroyStubs();
            stubManager = new RouterStubManager(this, group_addr, local_addr, reconnect_interval);
            for (InetSocketAddress host : initial_hosts) {
                stubManager.createStub(host.getHostName(), host.getPort(), null);                                
            }
            connect(group_addr, local_addr);            
        }
    }


    public void handleDisconnect() {
        stubManager.disconnectStubs();
    }

    @SuppressWarnings("unchecked")
    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception {
        if (group_addr == null) {
            if (log.isErrorEnabled())
                log.error("cluster_name is null, cannot get membership");
            return;
        }

        if (log.isTraceEnabled())
            log.trace("fetching members from GossipRouter(s)");

        final List<PingData> responses = new LinkedList<PingData>();
        for (RouterStub stub : stubManager.getStubs()) {
            try {
                List<PingData> rsps = stub.getMembers(group_addr);
                responses.addAll(rsps);
            }
            catch(Throwable t) {
                log.warn("failed fetching members from " + stub.getGossipRouterAddress() + ": " +  t);
            }
        }

        final Set<Address> initial_mbrs = new HashSet<Address>();
        for (PingData rsp : responses) {
            Address logical_addr = rsp.getAddress();
            initial_mbrs.add(logical_addr);

            // 1. Set physical addresses
            Collection<PhysicalAddress> physical_addrs = rsp.getPhysicalAddrs();
            if (physical_addrs != null) {
                for (PhysicalAddress physical_addr : physical_addrs)
                    down(new Event(Event.SET_PHYSICAL_ADDRESS, new Tuple<Address, PhysicalAddress>(
                                    logical_addr, physical_addr)));
            }

            // 2. Set logical name
            String logical_name = rsp.getLogicalName();
            if (logical_name != null && logical_addr instanceof org.jgroups.util.UUID)
                org.jgroups.util.UUID.add((org.jgroups.util.UUID) logical_addr, logical_name);
        }

        if (initial_mbrs.isEmpty()) {
            if (log.isErrorEnabled())
                log.error("[FIND_INITIAL_MBRS]: found no members");
            return;
        }
        if (log.isTraceEnabled())
            log.trace("consolidated mbrs from GossipRouter(s) are " + initial_mbrs);

        for (Address mbr_addr : initial_mbrs) {
            Message msg = new Message(mbr_addr);
            msg.setFlag(Message.OOB);
            PingHeader hdr = new PingHeader(PingHeader.GET_MBRS_REQ, cluster_name);
            hdr.return_view_only = return_views_only;
            msg.putHeader(this.id, hdr);
            if (log.isTraceEnabled())
                log.trace("[FIND_INITIAL_MBRS] sending GET_MBRS_REQ request to " + mbr_addr);            
            down_prot.down(new Event(Event.MSG, msg));
        }
    }


    protected void connect(String group, Address logical_addr) {
        String logical_name=org.jgroups.util.UUID.get(logical_addr);
        PhysicalAddress physical_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=physical_addr != null? new ArrayList<PhysicalAddress>() : null;
        if(physical_addr != null)
            physical_addrs.add(physical_addr);

      
        for (RouterStub stub : stubManager.getStubs()) {
            try {
                if(log.isTraceEnabled())
                    log.trace("trying to connect to " + stub.getGossipRouterAddress());
                stub.connect(group, logical_addr, logical_name, physical_addrs);
            }
            catch(Exception e) {
                if(log.isErrorEnabled())
                    log.error("failed connecting to " + stub.getGossipRouterAddress() + ": " + e); 
                stubManager.startReconnecting(stub);
            }
        }     
    }
}


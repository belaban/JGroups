
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RouterStub;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.util.Responses;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;


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
 */
public class TCPGOSSIP extends Discovery implements RouterStub.MembersNotification {
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Max time for socket creation. Default is 1000 msec")
    int sock_conn_timeout=1000;

    @Property(description="Max time in milliseconds to block on a read. 0 blocks forever")
    @Deprecated
    int sock_read_timeout=3000;

    @Property(description="Interval (ms) by which a disconnected stub attempts to reconnect to the GossipRouter")
    long reconnect_interval=10000L;
    
    @Property(name="initial_hosts", description="Comma delimited list of hosts to be contacted for initial membership", 
              converter=PropertyConverters.InitialHosts2.class)
    public void setInitialHosts(List<InetSocketAddress> hosts) {
        if(hosts == null || hosts.isEmpty())
            throw new IllegalArgumentException("initial_hosts must contain the address of at least one GossipRouter");

        initial_hosts.addAll(hosts) ;
    }

    public List<InetSocketAddress> getInitialHosts() {
        return initial_hosts;
    }

    @Property(description="Whether to use blocking (false) or non-blocking (true) connections. If GossipRouter is used, " +
      "this needs to be false; if GossipRouterNio is used, it needs to be true")
    protected boolean use_nio;

    public boolean isDynamic() {
        return true;
    }

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    
    // (list of IpAddresses) hosts to be contacted for the initial membership
    private final List<InetSocketAddress> initial_hosts = new CopyOnWriteArrayList<>();
    
    protected volatile RouterStubManager stubManager;

    public RouterStubManager getStubManager() {return stubManager;}

    public void init() throws Exception {
        super.init();
        stubManager = RouterStubManager.emptyGossipClientStubManager(this).useNio(this.use_nio);
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
        if (cluster_name == null || local_addr == null)
            log.error(Util.getMessage("GroupaddrOrLocaladdrIsNullCannotRegisterWithGossipRouterS"));
        else {
            InetAddress bind_addr=getTransport().getBindAddress();
            log.trace("registering " + local_addr + " under " + cluster_name + " with GossipRouter");
            stubManager.destroyStubs();
            PhysicalAddress physical_addr = (PhysicalAddress) down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            stubManager = new RouterStubManager(this,cluster_name, local_addr, UUID.get(local_addr), physical_addr, reconnect_interval).useNio(this.use_nio);
            for (InetSocketAddress host : initial_hosts) {
                RouterStub stub=stubManager.createAndRegisterStub(new IpAddress(bind_addr, 0), new IpAddress(host.getAddress(), host.getPort()));
                stub.socketConnectionTimeout(sock_conn_timeout);
            }
            stubManager.connectStubs();
        }
    }

    @ManagedOperation(description="Prints all stubs and the reconnect list")
    public String print() {
        RouterStubManager mgr=stubManager;
        return mgr != null? mgr.print() : "n/a";
    }

    @ManagedOperation(description="Prints all currently connected stubs")
    public String printStubs() {
        RouterStubManager mgr=stubManager;
        return mgr != null? mgr.printStubs() : "n/a";
    }

    @ManagedOperation(description="Prints the reconnect list")
    public String printReconnectList() {
        RouterStubManager mgr=stubManager;
        return mgr != null? mgr.printReconnectList() : "n/a";
    }

    public void handleDisconnect() {
        stubManager.disconnectStubs();
    }

    @Override
    public void findMembers(List<Address> members, boolean initial_discovery, Responses responses) {
        if(this.cluster_name == null) {
            log.error(Util.getMessage("ClusternameIsNullCannotGetMembership"));
            return;
        }

        log.trace("fetching members from GossipRouter(s)");
        stubManager.forEach(new RouterStubManager.Consumer() { // replace with lambda in Java 8
            @Override
            public void accept(RouterStub stub) {
                 try {
                     stub.getMembers(TCPGOSSIP.this.cluster_name, TCPGOSSIP.this);
                 }
                 catch(Throwable t) {
                     log.warn("failed fetching members from %s: %s, cause: %s", stub.gossipRouterAddress(), t, t.getCause());
                 }
            }
        });
    }

    @Override
    public void members(List<PingData> mbrs) {
        Set<PhysicalAddress> physical_addrs=new HashSet<>();
        PhysicalAddress      own_physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        PingData             data=new PingData(local_addr, false, org.jgroups.util.UUID.get(local_addr), own_physical_addr);
        PingHeader           hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName(cluster_name);

        for(PingData ping_data: mbrs)
            if(ping_data != null && ping_data.getPhysicalAddr() != null)
                physical_addrs.add(ping_data.getPhysicalAddr());

        for(PhysicalAddress physical_addr: physical_addrs) {
            if(physical_addr != null && own_physical_addr.equals(physical_addr)) // no need to send the request to myself
                continue;
            // the message needs to be DONT_BUNDLE, see explanation above
            final Message msg=new Message(physical_addr).setFlag(Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE, Message.Flag.OOB)
              .putHeader(this.id, hdr).setBuffer(marshal(data));
            log.trace("%s: sending discovery request to %s", local_addr, msg.getDest());
            down_prot.down(new Event(Event.MSG, msg));
        }
    }

    @ManagedOperation
    public void addInitialHost(String hostname, int port) {
        //if there is such a stub already, remove and destroy it
        removeInitialHost(hostname, port);
        
        //now re-add it
        InetSocketAddress isa = new InetSocketAddress(hostname, port);
        initial_hosts.add(isa);

        stubManager.createAndRegisterStub(null, new IpAddress(isa.getAddress(), isa.getPort()));
        stubManager.connectStubs(); // tries to connect all unconnected stubs
    }

    @ManagedOperation
    public boolean removeInitialHost(String hostname, int port) {
        InetSocketAddress isa = new InetSocketAddress(hostname, port);
        stubManager.unregisterStub(new IpAddress(isa.getAddress(), isa.getPort()));
        return initial_hosts.remove(isa);
    }

}


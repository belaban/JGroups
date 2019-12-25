package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RouterStub;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.util.Util;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is opened to a Router
 * (using the RouterStub client-side stub), the IP address/port of which was given using channel
 * properties {@code router_host} and {@code router_port}. All outgoing traffic is sent
 * via this TCP socket to the Router which distributes it to all connected TUNNELs in this group.
 * Incoming traffic received from Router will simply be passed up the stack.
 * 
 * <p>
 * A TUNNEL layer can be used to penetrate a firewall, most firewalls allow creating TCP connections
 * to the outside world, however, they do not permit outside hosts to initiate a TCP connection to a
 * host inside the firewall. Therefore, the connection created by the inside host is reused by
 * Router to send traffic from an outside host to a host inside the firewall.
 * 
 * @author Bela Ban
 * @author Vladimir Blagojevic
 */
@Experimental
public class TUNNEL extends TP implements RouterStub.StubReceiver {

    public interface TUNNELPolicy {
        void sendToAllMembers(String group, Address sender, byte[] data, int offset, int length) throws Exception;
        void sendToSingleMember(String group, Address dest, Address sender, byte[] data, int offset, int length) throws Exception;
    }

    /* ----------------------------------------- Properties -------------------------------------------------- */

    @Property(description = "Interval in msec to attempt connecting back to router in case of torn connection. Default is 5000 msec")
    protected long    reconnect_interval=5000;

    @Property(description="Should TCP no delay flag be turned on")
    protected boolean tcp_nodelay;

    @Property(description="Whether to use blocking (false) or non-blocking (true) connections. If GossipRouter is used, " +
      "this needs to be false; if GossipRouterNio is used, it needs to be true")
    protected boolean use_nio;

    @Property(description="A comma-separated list of GossipRouter hosts, e.g. HostA[12001],HostB[12001]")
    protected String  gossip_router_hosts;

    /* ------------------------------------------ Fields ----------------------------------------------------- */

    protected final List<InetSocketAddress> gossip_routers=new ArrayList<>();
    protected TUNNELPolicy                  tunnel_policy=new DefaultTUNNELPolicy();
    protected DatagramSocket                sock; // used to get a unique client address
    protected volatile RouterStubManager    stubManager;



    public TUNNEL() {
    }

    /** We can simply send a message with dest == null and the GossipRouter will take care of routing it to all
     * members in the cluster */
    public boolean supportsMulticasting() {
        return true;
    }


    public void setGossipRouterHosts(String hosts) throws UnknownHostException {
        gossip_routers.clear();
        // if we get passed value of List<SocketAddress>#toString() we have to strip []
        if(hosts.startsWith("[") && hosts.endsWith("]"))
            hosts=hosts.substring(1, hosts.length() - 1);
        gossip_router_hosts=hosts; //.addAll(Util.parseCommaDelimitedHosts2(hosts, port_range));
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

    public RouterStubManager getStubManager() {return stubManager;}

    public String toString() {
        return "TUNNEL";
    }

    public long getReconnectInterval() {
        return reconnect_interval;
    }

    public void setReconnectInterval(long reconnect_interval) {
        this.reconnect_interval = reconnect_interval;
    }

    /*------------------------------ Protocol interface ------------------------------ */

    public synchronized void setTUNNELPolicy(TUNNELPolicy policy) {
        if (policy == null)
            throw new IllegalArgumentException("Tunnel policy has to be non null");
        tunnel_policy = policy;
    }

    public void init() throws Exception {
        super.init();
        if(timer == null)
            throw new Exception("timer cannot be retrieved from protocol stack");
        gossip_routers.clear();
        gossip_routers.addAll(Util.parseCommaDelimitedHosts2(gossip_router_hosts, port_range));
        if(gossip_routers.isEmpty())
            throw new IllegalStateException("gossip_router_hosts needs to contain at least one address of a GossipRouter");
        log.debug("gossip routers are %s", gossip_routers);
        stubManager=RouterStubManager.emptyGossipClientStubManager(this).useNio(this.use_nio);
        sock=getSocketFactory().createDatagramSocket("jgroups.tunnel.ucast_sock", bind_port, bind_addr);
    }
    
    public void destroy() {        
        stubManager.destroyStubs();
        Util.close(sock);
        super.destroy();
    }

    private void disconnectStub() {
        stubManager.disconnectStubs();
    }

    @Override
    public Object down(Event evt) {
        Object retEvent = super.down(evt);
        switch (evt.getType()) {
            case Event.CONNECT:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
                String group=evt.getArg();
                Address local=local_addr;
                if(stubManager != null)
                    stubManager.destroyStubs();
                PhysicalAddress physical_addr=getPhysicalAddressFromCache(local);
                String logical_name=org.jgroups.util.NameCache.get(local);
                stubManager = new RouterStubManager(this,group,local, logical_name, physical_addr, getReconnectInterval()).useNio(this.use_nio);
                for(InetSocketAddress gr : gossip_routers) {
                    stubManager.createAndRegisterStub(new IpAddress(bind_addr, bind_port), new IpAddress(gr.getAddress(), gr.getPort()))
                      .receiver(this).set("tcp_nodelay", tcp_nodelay);
                }
                stubManager.connectStubs();
                break;

            case Event.DISCONNECT:
                disconnectStub();
                break;
        }
        return retEvent;
    }

    @Override
    public void receive(GossipData data) {
        switch (data.getType()) {
            case MESSAGE:
                if(Objects.equals(local_addr, data.getSender()))
                    return;
                byte[] msg=data.getBuffer();
                receive(data.getSender(), msg, 0, msg.length);
                break;
            case SUSPECT:
                Address suspect=data.getAddress();
                if(suspect != null) {
                    log.debug("%s: firing suspect event for %s", local_addr, suspect);
                    up(new Event(Event.SUSPECT, Collections.singletonList(suspect)));
                }
                break;
        }
    }



    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        String group=cluster_name != null? cluster_name.toString() : null;
        tunnel_policy.sendToAllMembers(group, local_addr, data, offset, length);
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        String group=cluster_name != null? cluster_name.toString() : null;
        tunnel_policy.sendToSingleMember(group, dest, local_addr, data, offset, length);
    }

    protected void sendToSingleMember(final Address dest, byte[] buf, int offset, int length) throws Exception {
        if(dest instanceof PhysicalAddress)
            throw new IllegalArgumentException(String.format("destination %s cannot be a physical address", dest));
        sendUnicast(dest, buf, offset, length);
    }

    protected void sendUnicast(Address dest, byte[] data, int offset, int length) throws Exception {
        String group=cluster_name != null? cluster_name.toString() : null;
        tunnel_policy.sendToSingleMember(group, dest, local_addr, data, offset, length);
    }

    public String getInfo() {
        return stubManager.printStubs();
    }

    protected PhysicalAddress getPhysicalAddress() {
        return sock != null ? new IpAddress(bind_addr, sock.getLocalPort()) : null;
    }


    private class DefaultTUNNELPolicy implements TUNNELPolicy {

        public void sendToAllMembers(final String group, Address sender,
                                     final byte[] data, final int offset, final int length) throws Exception {
            stubManager.forAny( stub -> {
                try {
                    if(log.isTraceEnabled())
                        log.trace("%s: sending a message to all members, GR used %s", local_addr, stub.gossipRouterAddress());
                    stub.sendToAllMembers(group, sender, data, offset, length);
                }
                catch(Exception ex) {
                    log.warn("%s: failed sending a message to all members, router used %s: %s",
                             local_addr, stub.gossipRouterAddress(), ex);
                }
            });
        }

        public void sendToSingleMember(final String group, final Address dest, Address sender,
                                       final byte[] data, final int offset, final int length) throws Exception {
            stubManager.forAny( stub -> {
                try {
                    if(log.isTraceEnabled())
                        log.trace("%s: sending a message to %s (router used %s)", local_addr, dest, stub.gossipRouterAddress());
                    stub.sendToMember(group, dest, sender, data, offset, length);
                }
                catch(Exception ex) {
                    log.warn("%s: failed sending a message to %s (router used %s): %s", local_addr, dest,
                             stub.gossipRouterAddress(), ex);
                }
            });
        }

    }
}

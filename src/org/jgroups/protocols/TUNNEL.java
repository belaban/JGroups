package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.RouterStub;
import org.jgroups.stack.RouterStubManager;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TLS;
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
public class TUNNEL extends TP implements RouterStub.StubReceiver {

    public interface TUNNELPolicy {
        void sendToAllMembers(String group, Address sender, byte[] data, int offset, int length) throws Exception;
        void sendToSingleMember(String group, Address dest, Address sender, byte[] data, int offset, int length) throws Exception;
    }

    /* ----------------------------------------- Properties -------------------------------------------------- */

    @Property(description = "Interval in msec to attempt connecting back to router in case of torn connection",
      type=AttributeType.TIME)
    protected long    reconnect_interval=5000;

    @Property(description="Should TCP no delay flag be turned on")
    protected boolean tcp_nodelay;

    @Property(description="Whether to use blocking (false) or non-blocking (true) connections. If GossipRouter is used, " +
      "this needs to be false; if GossipRouterNio is used, it needs to be true")
    protected boolean use_nio;

    @Property(description="A comma-separated list of GossipRouter hosts, e.g. HostA[12001],HostB[12001]")
    protected String  gossip_router_hosts;

    @Property(description="Sends a heartbeat to the GossipRouter every heartbeat_interval ms (0 disables this)",
      type=AttributeType.TIME)
    protected long    heartbeat_interval;

    @Property(description="Max time (ms) with no received message or heartbeat after which the connection to a " +
      "GossipRouter is closed. Ignored when heartbeat_interval is 0.", type=AttributeType.TIME)
    protected long    heartbeat_timeout;

    @Property(description="SO_LINGER in seconds. Default of -1 disables it")
    protected int     linger=-1; // SO_LINGER (number of seconds, -1 disables it)

    /* ------------------------------------------ Fields ----------------------------------------------------- */

    protected final List<InetSocketAddress> gossip_routers=new ArrayList<>();
    protected TUNNELPolicy                  tunnel_policy=new DefaultTUNNELPolicy();
    protected DatagramSocket                sock; // used to get a unique client address
    protected volatile RouterStubManager    stubManager;
    @Component(name="tls",description="Contains the attributes for TLS (SSL sockets) when enabled=true")
    protected TLS                           tls=new TLS();



    public TUNNEL() {
    }


    public long    getReconnectInterval()       {return reconnect_interval;}
    public TUNNEL  setReconnectInterval(long r) {this.reconnect_interval=r; return this;}
    public boolean isTcpNodelay()               {return tcp_nodelay;}
    public TUNNEL  setTcpNodelay(boolean nd)    {this.tcp_nodelay=nd;return this;}
    public boolean useNio()                     {return use_nio;}
    public TUNNEL  useNio(boolean use_nio)      {this.use_nio=use_nio; return this;}
    public TLS     tls()                        {return tls;}
    public TUNNEL  tls(TLS t)                   {this.tls=t; return this;}
    public int     getLinger()                  {return linger;}
    public TUNNEL  setLinger(int l)             {this.linger=l; return this;}

    /** We can simply send a message with dest == null and the GossipRouter will take care of routing it to all
     * members in the cluster */
    public boolean supportsMulticasting() {
        return true;
    }


    public TUNNEL setGossipRouterHosts(String hosts) throws UnknownHostException {
        gossip_routers.clear();
        // if we get passed value of List<SocketAddress>#toString() we have to strip []
        if(hosts.startsWith("[") && hosts.endsWith("]"))
            hosts=hosts.substring(1, hosts.length() - 1);
        gossip_router_hosts=hosts; //.addAll(Util.parseCommaDelimitedHosts2(hosts, port_range));
        return this;
    }

    @ManagedAttribute(description="Is the reconnector task running?")
    public boolean isReconnectorTaskRunning() {
        return stubManager != null && stubManager.reconnectorRunning();
    }

    @ManagedAttribute(description="Is the heartbeat task running?")
    public boolean isHeartbeatTaskRunning() {
        return stubManager != null && stubManager.heartbeaterRunning();
    }

    @ManagedAttribute(description="Is the timeout check task running?")
    public boolean isTimeoutCheckTaskRunning() {
        return stubManager != null && stubManager.timeouterRunning();
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



    /*------------------------------ Protocol interface ------------------------------ */

    public synchronized TUNNEL setTUNNELPolicy(TUNNELPolicy policy) {
        if (policy == null)
            throw new IllegalArgumentException("Tunnel policy has to be non null");
        tunnel_policy = policy;
        return this;
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
        stubManager=RouterStubManager.emptyGossipClientStubManager(log, timer).useNio(this.use_nio);
        sock=getSocketFactory().createDatagramSocket("jgroups.tunnel.ucast_sock", bind_port, bind_addr);
    }

    @Override
    public void start() throws Exception {
        super.start();
        if(tls.enabled()) {
            SocketFactory factory=tls.createSocketFactory();
            setSocketFactory(factory);
        }
    }

    public void destroy() {
        if(stubManager != null)
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
                stubManager=new RouterStubManager(log,timer,group,local, logical_name, physical_addr, reconnect_interval)
                  .useNio(this.use_nio).socketFactory(getSocketFactory()).heartbeat(heartbeat_interval, heartbeat_timeout);
                for(InetSocketAddress gr: gossip_routers) {
                    try {
                        InetSocketAddress target=gr.isUnresolved()? new InetSocketAddress(gr.getHostString(), gr.getPort())
                          : new InetSocketAddress(gr.getAddress(), gr.getPort());
                        stubManager.createAndRegisterStub(new InetSocketAddress(bind_addr, bind_port), target, linger)
                          .receiver(this).tcpNoDelay(tcp_nodelay);
                    }
                    catch(Throwable t) {
                        log.error("%s: failed creating stub to %s: %s", local, bind_addr + ":" + bind_port, t);
                    }
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


    @Override
    public void sendToAll(byte[] data, int offset, int length) throws Exception {
        String group=cluster_name != null? cluster_name.toString() : null;
        tunnel_policy.sendToAllMembers(group, local_addr, data, offset, length);
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        String group=cluster_name != null? cluster_name.toString() : null;
        tunnel_policy.sendToSingleMember(group, dest, local_addr, data, offset, length);
    }

    protected void sendTo(final Address dest, byte[] buf, int offset, int length) throws Exception {
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

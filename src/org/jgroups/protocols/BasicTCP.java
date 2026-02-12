package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.ConnectionListener;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.pbcast.GMS;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * Shared base class for TCP protocols
 * @author Scott Marlow
 * @author Bela Ban
 */
public abstract class BasicTCP extends TP implements Receiver, ConnectionListener {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Reaper interval in msec. Default is 0 (no reaping)",type=AttributeType.TIME)
    protected long        reaper_interval; // time in msecs between connection reaps

    @Property(description="Max time connection can be idle before being reaped (in ms)",type=AttributeType.TIME)
    protected long        conn_expire_time; // max time a conn can be idle before being reaped

    @Property(description="Receiver buffer size in bytes",type=AttributeType.BYTES)
    protected int         recv_buf_size;

    @Property(description="Send buffer size in bytes",type=AttributeType.BYTES)
    protected int         send_buf_size;

    @Property(description="Max time allowed for a socket creation in connection table",type=AttributeType.TIME)
    protected int         sock_conn_timeout=2000; // max time in millis for a socket creation in connection table
    
    @Property(description="Max time to block on reading of peer address",type=AttributeType.TIME)
    protected int         peer_addr_read_timeout=1000; // max time to block on reading of peer address

    @Property(description="The max number of bytes a message can have. If greater, an exception will be " +
      "thrown. 0 disables this", type=AttributeType.BYTES)
    protected int         max_length;
    
    @Property(description="Should TCP no delay flag be turned on. True: nagling is OFF, false: nagling is ON")
    protected boolean     tcp_nodelay=false; // https://issues.redhat.com/browse/JGRP-2781

    @Property(description="SO_LINGER in seconds. Default of -1 disables it")
    protected int         linger=-1; // SO_LINGER (number of seconds, -1 disables it)

    @Property(description="Wait for an ack from the server when a connection is established " +
      "(https://issues.redhat.com/browse/JGRP-2684)",deprecatedMessage="will be ignored (JGRP-2866)")
    @Deprecated(since="5.4.4",forRemoval=true)
    protected boolean     use_acks;

    /**
     * Indicates whether the transport is connected to the cluster.
     * Used by TCP-based transports to skip SUSPECT events during graceful disconnect when connections are closed.
     */
    @ManagedAttribute(description="Indicates whether the transport is connected to the cluster")
    protected volatile boolean connected;

    @LocalAddress
    @Property(name="client_bind_addr",
              description="The address of a local network interface which should be used by client sockets to bind to. " +
                "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.TCP_CLIENT_BIND_ADDR},writable=false)
    protected InetAddress client_bind_addr;

    @Property(description="The local port a client socket should bind to. If 0, an ephemeral port will be picked.")
    protected int         client_bind_port;

    @Property(description="If true, client sockets will not explicitly bind to bind_addr but will defer to the native socket")
    protected boolean     defer_client_bind_addr;

    @Property(description="Log a stack trace when a connection is closed")
    protected boolean     log_details=true;

    @Property(description="When true, a SUSPECT(P) event is passed up when a connection is closed by the peer P. " +
      "This is not recommended when connection reaping is enabled. https://issues.redhat.com/browse/JGRP-2869")
    protected boolean     enable_suspect_events;

    @ManagedAttribute(description="Number of suspect events sent up the stack due to peers closing connections")
    protected final LongAdder num_suspect_events=new LongAdder();

    protected final Predicate<PhysicalAddress> is_member=pa -> {
        Address addr=logical_addr_cache.getByValue(pa);
        return addr != null && members.contains(addr);
    };

    /* --------------------------------------------- Fields ------------------------------------------------------ */

    protected BasicTCP() {
        super();        
    }

    public boolean     supportsMulticasting()           {return false;}

    public long        getReaperInterval()              {return reaper_interval;}
    public BasicTCP    setReaperInterval(long interval) {this.reaper_interval=interval; return this;}
    public BasicTCP    reaperInterval(long interval)    {this.reaper_interval=interval; return this;}

    public long        getConnExpireTime()              {return conn_expire_time;}
    public BasicTCP    setConnExpireTime(long time)     {this.conn_expire_time=time; return this;}

    public int         getRecvBufSize()                 {return recv_buf_size;}
    public BasicTCP    setRecvBufSize(int r)            {this.recv_buf_size=r; return this;}

    public int         getSendBufSize()                 {return send_buf_size;}
    public BasicTCP    setSendBufSize(int s)            {this.send_buf_size=s; return this;}

    public int         getSockConnTimeout()             {return sock_conn_timeout;}
    public BasicTCP    setSockConnTimeout(int s)        {this.sock_conn_timeout=s; return this;}

    public int         getMaxLength()                   {return max_length;}
    public BasicTCP    setMaxLength(int len)            {max_length=len; return this;}

    public int         getPeerAddrReadTimeout()         {return peer_addr_read_timeout;}
    public BasicTCP    setPeerAddrReadTimeout(int p)    {this.peer_addr_read_timeout=p; return this;}

    public boolean     tcpNodelay()                     {return tcp_nodelay;}
    public BasicTCP    tcpNodelay(boolean t)            {this.tcp_nodelay=t; return this;}

    public int         getLinger()                      {return linger;}
    public BasicTCP    setLinger(int l)                 {this.linger=l; return this;}

    public static boolean useAcks()                     {return false;}
    public BasicTCP    useAcks(boolean ignored)         {return this;}

    public InetAddress getClientBindAddr()              {return client_bind_addr;}
    public BasicTCP    setClientBindAddr(InetAddress c) {this.client_bind_addr=c; return this;}

    public int         getClientBindPort()              {return client_bind_port;}
    public BasicTCP    setClientBindPort(int c)         {this.client_bind_port=c; return this;}

    public boolean     deferClientBindAddr()            {return defer_client_bind_addr;}
    public BasicTCP    deferClientBindAddr(boolean d)   {this.defer_client_bind_addr=d; return this;}

    public boolean     logDetails()                     {return log_details;}
    public BasicTCP    logDetails(boolean l)            {log_details=l; return this;}

    public boolean     enableSuspectEvents()            {return enable_suspect_events;}
    public BasicTCP    enableSuspectEvents(boolean b)   {enable_suspect_events=b; return this;}

    public long        numSuspectEvents()               {return num_suspect_events.sum();}



    public void init() throws Exception {
        super.init();
        if(bind_port <= 0) {
            Discovery discovery_prot=stack.findProtocol(Discovery.class);
            if(discovery_prot != null && !discovery_prot.isDynamic())
                throw new IllegalArgumentException("bind_port cannot be set to " + bind_port +
                                                     ", as no dynamic discovery protocol (e.g. MPING or TCPGOSSIP) has been detected.");
        }
        if(reaper_interval > 0 || conn_expire_time > 0) {
            if(enable_suspect_events)
                log.warn("reaping is enabled, but also suspect events; this is not recommended");
            if(conn_expire_time == 0 && reaper_interval > 0) {
                log.warn("reaper interval (%d) set, but not conn_expire_time, disabling reaping", reaper_interval);
                reaper_interval=0;
            }
            else if(conn_expire_time > 0 && reaper_interval == 0) {
                reaper_interval=conn_expire_time / 2;
                log.warn("conn_expire_time (%d) is set but reaper_interval is 0; setting it to %d", conn_expire_time, reaper_interval);
            }
        }
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        send(dest, data, offset, length);
    }

    public String getInfo() {
        return String.format("connections: %s\n", printConnections());
    }

    public abstract String printConnections();

    public abstract void send(Address dest, byte[] data, int offset, int length) throws Exception;

    public abstract void retainAll(Collection<Address> members);

    @Override
    public void resetStats() {
        super.resetStats();
        num_suspect_events.reset();
    }

    @Override
    public Object down(Event evt) {
        Object ret=super.down(evt);
        if(evt.getType() == Event.VIEW_CHANGE) {
            Set<Address> physical_mbrs=new HashSet<>();
            for(Address addr: members) {
                PhysicalAddress physical_addr=getPhysicalAddressFromCache(addr);
                if(physical_addr != null)
                    physical_mbrs.add(physical_addr);
            }
            retainAll(physical_mbrs); // remove all connections which are not members
        }
        return ret;
    }

    @Override
    public void connectionClosed(Connection conn) {
        Address peer_ip=conn.peerAddress();
        Address peer=peer_ip != null? logical_addr_cache.getByValue((PhysicalAddress)peer_ip) : null;

        if (conn.isCloseGracefuly() && peer != null && members.contains(peer) && connected) {
            // we notify the situation so GMS can react in case the LEAVE message was not being sent
            up_prot.up(new Event(Event.DISCONNECT, peer));
            return;
        }

        if(!enable_suspect_events)
            return;

        if(peer != null && members.contains(peer) && connected &&
          Optional.ofNullable(stack.<GMS>findProtocol(GMS.class)).filter(Predicate.not(GMS::isLeaving)).isPresent()) {
            if(log.isDebugEnabled())
                log.debug("%s: connection closed by peer %s (IP=%s), sending up a suspect event",
                          local_addr, peer, peer_ip);
            Event suspect=new Event(Event.SUSPECT, List.of(peer));
            Runnable r=() -> up_prot.up(suspect);
            boolean rc=thread_pool.execute(r);
            if(!rc)
                getThreadFactory().newThread(r).start();
            num_suspect_events.increment();
        }
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }
}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Shared base class for TCP protocols
 * @author Scott Marlow
 * @author Bela Ban
 */
public abstract class BasicTCP extends TP implements Receiver {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Reaper interval in msec. Default is 0 (no reaping)")
    protected long        reaper_interval=0; // time in msecs between connection reaps

    @Property(description="Max time connection can be idle before being reaped (in ms)")
    protected long        conn_expire_time=0; // max time a conn can be idle before being reaped

    @Property(description="Receiver buffer size in bytes")
    protected int         recv_buf_size=150000;
    
    @Property(description="Send buffer size in bytes")
    protected int         send_buf_size=150000;
    
    @Property(description="Max time allowed for a socket creation in connection table")
    protected int         sock_conn_timeout=2000; // max time in millis for a socket creation in connection table
    
    @Property(description="Max time to block on reading of peer address")
    protected int         peer_addr_read_timeout=1000; // max time to block on reading of peer address
    
    @Property(description="Should TCP no delay flag be turned on")
    protected boolean     tcp_nodelay=true; // should be true by default as message bundling makes delaying packets moot
    
    @Property(description="SO_LINGER in msec. Default of -1 disables it")
    protected int         linger=-1; // SO_LINGER (number of ms, -1 disables it)

    @LocalAddress
    @Property(name="client_bind_addr",
              description="The address of a local network interface which should be used by client sockets to bind to. " +
                "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.TCP_CLIENT_BIND_ADDR},writable=false)
    protected InetAddress client_bind_addr=null;

    @Property(description="The local port a client socket should bind to. If 0, an ephemeral port will be picked.")
    protected int         client_bind_port=0;

    @Property(description="If true, client sockets will not explicitly bind to bind_addr but will defer to the native socket")
    protected boolean     defer_client_bind_addr=false;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    

    protected BasicTCP() {
        super();        
    }

    public boolean supportsMulticasting()           {return false;}
    public long    getReaperInterval()              {return reaper_interval;}
    public void    setReaperInterval(long interval) {this.reaper_interval=interval;}
    public long    getConnExpireTime()              {return conn_expire_time;}
    public void    setConnExpireTime(long time)     {this.conn_expire_time=time;}


    public void init() throws Exception {
        super.init();
        if(bind_port <= 0) {
            Discovery discovery_prot=(Discovery)stack.findProtocol(Discovery.class);
            if(discovery_prot != null && !discovery_prot.isDynamic())
                throw new IllegalArgumentException("bind_port cannot be set to " + bind_port +
                                                     ", as no dynamic discovery protocol (e.g. MPING or TCPGOSSIP) has been detected.");
        }
        if(reaper_interval > 0 || conn_expire_time > 0) {
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


    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        sendToMembers(members, data, offset, length);
    }

    public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {
        if(log.isTraceEnabled()) log.trace("dest=" + dest + " (" + length + " bytes)");
        send(dest, data, offset, length);
    }

    public String getInfo() {
        StringBuilder sb=new StringBuilder();
        sb.append("connections: ").append(printConnections()).append("\n");
        return sb.toString();
    }


    public abstract String printConnections();

    public abstract void send(Address dest, byte[] data, int offset, int length) throws Exception;

    public abstract void retainAll(Collection<Address> members);

    /** BaseServer.Receiver interface */
    public void receive(Address sender, byte[] data, int offset, int length) {
        super.receive(sender, data, offset, length);
    }

    public void receive(Address sender, ByteBuffer buf) {
        Util.bufferToArray(sender, buf, this);
    }

    protected Object handleDownEvent(Event evt) {
        Object ret=super.handleDownEvent(evt);
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
}

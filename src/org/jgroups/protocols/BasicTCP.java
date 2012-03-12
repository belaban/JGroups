package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Shared base class for TCP protocols
 * @author Scott Marlow
 */
public abstract class BasicTCP extends TP {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    @Property(description="Reaper interval in msec. Default is 0 (no reaping)")
    protected long reaper_interval=0; // time in msecs between connection reaps

    @Property(description="Max time connection can be idle before being reaped (in ms)")
    protected long conn_expire_time=0; // max time a conn can be idle before being reaped

    @Property(description="Should separate send queues be used for each connection")
    boolean use_send_queues=true;
    
    @Property(description="Max number of messages in a send queue")
    int send_queue_size=10000;
    
    @Property(description="Receiver buffer size in bytes")
    int recv_buf_size=150000;
    
    @Property(description="Send buffer size in bytes")
    int send_buf_size=150000;
    
    @Property(description="Max time allowed for a socket creation in connection table")
    int sock_conn_timeout=2000; // max time in millis for a socket creation in connection table
    
    @Property(description="Max time to block on reading of peer address")
    int peer_addr_read_timeout=1000; // max time to block on reading of peer address
    
    @Property(description="Should TCP no delay flag be turned on")
    boolean tcp_nodelay=true;
    
    @Property(description="SO_LINGER in msec. Default of -1 disables it")
    int linger=-1; // SO_LINGER (number of ms, -1 disables it)


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    

    protected BasicTCP() {
        super();        
    }

    public boolean supportsMulticasting() {
        return false;
    }

    public long getReaperInterval() {return reaper_interval;}
    public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
    public long getConnExpireTime() {return conn_expire_time;}
    public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}


    public void init() throws Exception {
        super.init();

        if(!isSingleton() && bind_port <= 0) {
            Discovery discovery_prot=(Discovery)stack.findProtocol(Discovery.class);
            if(discovery_prot != null && !discovery_prot.isDynamic())
                throw new IllegalArgumentException("start_port cannot be set to " + bind_port +
                                                     ", as no dynamic discovery protocol (e.g. MPING or TCPGOSSIP) has been detected.");
        }
    }


    public void sendMulticast(byte[] data, int offset, int length) throws Exception {
        sendToAllPhysicalAddresses(data, offset, length);
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

    protected Object handleDownEvent(Event evt) {
        Object ret=super.handleDownEvent(evt);
        if(evt.getType() == Event.VIEW_CHANGE) {
            Set<Address> physical_mbrs=new HashSet<Address>();
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

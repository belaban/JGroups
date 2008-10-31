package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Set;

/**
 * Shared base class for tcpip protocols
 * @author Scott Marlow
 */
public abstract class BasicTCP extends TP {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    
    @Property(description="Should unicast messages to suspected members be dropped. Default is false")
    boolean skip_suspected_members=true;

    @Property(description="If cannot send a message to P (on an exception), should SUSPECT message be raised. Default is false")
    boolean suspect_on_send_failure=false;

    @ManagedAttribute(description="Reaper interval", writable=true)
    @Property(description="Reaper interval in msec. Default is 0 (no reaping)")
    protected long reaper_interval=0; // time in msecs between connection reaps

    @ManagedAttribute(description="Connection expiration time", writable=true)
    @Property(description="Max time connection can be idle before being reaped")
    protected long conn_expire_time=0; // max time a conn can be idle before being reaped

    @Property(description="Should separate send queues be used for each connection. Default is true")
    boolean use_send_queues=true;
    
    @Property(description="Max number of messages in a send queue. Default is 10000 messages")
    int send_queue_size=10000;
    
    @Property(description="Receiver buffer size in bytes. Default is 150000 bytes")
    int recv_buf_size=150000;
    
    @Property(description="Send buffer size in bytes. Default is 150000 bytes")
    int send_buf_size=150000;
    
    @Property(description="Max time allowed for a socket creation in ConnectionTable. Default is 2000 msec")
    int sock_conn_timeout=2000; // max time in millis for a socket creation in ConnectionTable
    
    @Property(description="Max time to block on reading of peer address. Default is 1000 msec")
    int peer_addr_read_timeout=1000; // max time to block on reading of peer address
    
    @Property(description="Should TCP no delay flag be turned on. Default is false")
    boolean tcp_nodelay=false;
    
    @Property(description="SO_LINGER in msec. Default of -1 disables it")
    int linger=-1; // SO_LINGER (number of ms, -1 disables it)

    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    

    /**
     * List the maintains the currently suspected members. This is used so we
     * don't send too many SUSPECT events up the stack (one per message !)
     */
    final BoundedList<Address>  suspected_mbrs=new BoundedList<Address>(20);
    
    protected InetAddress  external_addr=null; // the IP address which is broadcast to other group members
           
  
    protected BasicTCP() {
        super();        
    }
    
    public long getReaperInterval() {return reaper_interval;}
    public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
    public long getConnExpireTime() {return conn_expire_time;}
    public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}

    @Property(name="external_addr")
    public void setExternalAddress(String addr) throws UnknownHostException {
        external_addr=InetAddress.getByName(addr);
    }


    public void init() throws Exception {
        super.init();

        linger=linger > 0 ? Math.min(1, linger / 1000) : linger; // convert from ms to secs

        Util.checkBufferSize(getName() + ".recv_buf_size", recv_buf_size);
        Util.checkBufferSize(getName() + ".send_buf_size", send_buf_size);
       
        if(!isSingleton() && bind_port <= 0) {
            Protocol dynamic_discovery_prot=stack.findProtocol("MPING");
            if(dynamic_discovery_prot == null)
                dynamic_discovery_prot=stack.findProtocol("TCPGOSSIP");

            if(dynamic_discovery_prot != null) {
                if(log.isDebugEnabled())
                    log.debug("dynamic discovery is present (" + dynamic_discovery_prot + "), so start_port=" + bind_port + " is okay");
            }
            else {
                throw new IllegalArgumentException("start_port cannot be set to " + bind_port +
                        ", as no dynamic discovery protocol (e.g. MPING or TCPGOSSIP) has been detected.");
            }
        }
    }



    public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {
        Set<Address> mbrs;

        synchronized(members) {
            mbrs=(Set<Address>)members.clone();
        }
        for(Address dest: mbrs) {
            sendToSingleMember(dest, data, offset, length);
        }
    }

    public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {
        if(log.isTraceEnabled()) log.trace("dest=" + dest + " (" + length + " bytes)");
        if(skip_suspected_members) {
            if(suspected_mbrs.contains(dest)) {
                if(log.isTraceEnabled())
                    log.trace("will not send unicast message to " + dest + " as it is currently suspected");
                return;
            }
        }

        try {
            send(dest, data, offset, length);
        }
        catch(Exception e) {
            if(log.isTraceEnabled())
                log.trace("failure sending message to " + dest, e);
            if(suspect_on_send_failure && members.contains(dest)) {
                if(!suspected_mbrs.contains(dest)) {
                    suspected_mbrs.add(dest);
                    up_prot.up(new Event(Event.SUSPECT, dest));
                }
            }
        }
    }

    public String getInfo() {
        StringBuilder sb=new StringBuilder();
        sb.append("connections: ").append(printConnections()).append("\n");
        return sb.toString();
    }

    public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {
        if(multicast)
            msg.setDest(null);
        else
            msg.setDest(dest);
    }

    public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {
        postUnmarshalling(msg, dest, null, multicast);
    }

    public abstract String printConnections();

    public abstract void send(Address dest, byte[] data, int offset, int length) throws Exception;

    public abstract void retainAll(Collection<Address> members);

    /** ConnectionTable.Receiver interface */
    public void receive(Address sender, byte[] data, int offset, int length) {
        receive(local_addr, sender, data, offset, length);
    }

    protected Object handleDownEvent(Event evt) {
        Object ret=super.handleDownEvent(evt);
        if(evt.getType() == Event.VIEW_CHANGE) {
            suspected_mbrs.clear();
            retainAll(members); // remove all connections from the ConnectionTable which are not members
        }
        else if(evt.getType() == Event.UNSUSPECT) {
            Address suspected_mbr=(Address)evt.getArg();
            suspected_mbrs.remove(suspected_mbr);
        }
        return ret;
    }
}

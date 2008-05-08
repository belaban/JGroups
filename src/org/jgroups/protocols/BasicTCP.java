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
import java.util.Properties;
import java.util.Set;

/**
 * Shared base class for tcpip protocols
 * @author Scott Marlow
 */
public abstract class BasicTCP extends TP {

    /** Should we drop unicast messages to suspected members or not */
    @Property
    boolean               skip_suspected_members=true;

    /** When we cannot send a message to P (on an exception), then we send a SUSPECT message up the stack */
    @Property
    boolean               suspect_on_send_failure=false;


    /** List the maintains the currently suspected members. This is used so we don't send too many SUSPECT
     * events up the stack (one per message !)
     */
    final BoundedList<Address>  suspected_mbrs=new BoundedList<Address>(20);
    protected InetAddress  external_addr=null; // the IP address which is broadcast to other group members
        
    @ManagedAttribute(description="Reaper interval",writable=true)
    @Property
    protected long         reaper_interval=0;  // time in msecs between connection reaps
    
    @ManagedAttribute(description="Connection expiration time",writable=true)
    @Property
    protected long         conn_expire_time=0; // max time a conn can be idle before being reaped
    /** Use separate send queues for each connection */
    @Property
    boolean                use_send_queues=true;
    @Property
    int                    send_queue_size=10000; // max number of messages in a send queue
    @Property
    int                    recv_buf_size=150000;
    @Property
    int                    send_buf_size=150000;
    @Property
    int                    sock_conn_timeout=2000; // max time in millis for a socket creation in ConnectionTable
    @Property
    int                    peer_addr_read_timeout=1000; // max time to block on reading of peer address
    @Property
    boolean                tcp_nodelay=false;
    @Property
    int                    linger=-1; // SO_LINGER (number of ms, -1 disables it)
  
    public long getReaperInterval() {return reaper_interval;}
    public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
    public long getConnExpireTime() {return conn_expire_time;}
    public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}

    public boolean setProperties(Properties props) {       
        super.setProperties(props);       

        String str=props.getProperty("external_addr");
        if(str != null) {
            try {
                external_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(external_addr): host " + str + " not known");
                return false;
            }
            props.remove("external_addr");
        }     

        Util.checkBufferSize(getName() + ".recv_buf_size", recv_buf_size);
        Util.checkBufferSize(getName() + ".send_buf_size", send_buf_size);

        return true;
    }

    public void init() throws Exception {
        super.init();
        if(bind_port <= 0) {
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

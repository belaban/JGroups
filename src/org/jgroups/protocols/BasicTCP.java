package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
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
    boolean               skip_suspected_members=true;

    /** When we cannot send a message to P (on an exception), then we send a SUSPECT message up the stack */
    boolean               suspect_on_send_failure=false;


    /** List the maintains the currently suspected members. This is used so we don't send too many SUSPECT
     * events up the stack (one per message !)
     */
    final BoundedList<Address>  suspected_mbrs=new BoundedList<Address>(20);
    protected InetAddress  external_addr=null; // the IP address which is broadcast to other group members
        
    @ManagedAttribute(description="Reaper interval",writable=true)
    protected long         reaper_interval=0;  // time in msecs between connection reaps
    @ManagedAttribute(description="Connection expiration time",writable=true)
    protected long         conn_expire_time=0; // max time a conn can be idle before being reaped
    /** Use separate send queues for each connection */
    boolean                use_send_queues=true;
    int                    send_queue_size=10000; // max number of messages in a send queue
    int                    recv_buf_size=150000;
    int                    send_buf_size=150000;
    int                    sock_conn_timeout=2000; // max time in millis for a socket creation in ConnectionTable
    int                    peer_addr_read_timeout=1000; // max time to block on reading of peer address
    boolean                tcp_nodelay=false;
    int                    linger=-1; // SO_LINGER (number of ms, -1 disables it)
  
    public long getReaperInterval() {return reaper_interval;}
    public void setReaperInterval(long reaper_interval) {this.reaper_interval=reaper_interval;}
    public long getConnExpireTime() {return conn_expire_time;}
    public void setConnExpireTime(long conn_expire_time) {this.conn_expire_time=conn_expire_time;}

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);

        str=props.getProperty("start_port");
        if(str != null) {
            bind_port=Integer.parseInt(str);
            props.remove("start_port");
            if(log.isWarnEnabled()) log.warn("\"start_port\" is deprecated; use \"bind_port\" instead");
        }

        str=props.getProperty("end_port");
        if(str != null) {
            port_range=Integer.parseInt(str) - bind_port;
            props.remove("end_port");
            if(log.isWarnEnabled()) log.warn("\"end_port\" is deprecated; use \"port_range\" instead");
        }

        str=props.getProperty("external_addr");
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

        str=props.getProperty("reaper_interval");
        if(str != null) {
            reaper_interval=Long.parseLong(str);
            props.remove("reaper_interval");
        }

        str=props.getProperty("conn_expire_time");
        if(str != null) {
            conn_expire_time=Long.parseLong(str);
            props.remove("conn_expire_time");
        }

        str=props.getProperty("sock_conn_timeout");
        if(str != null) {
            sock_conn_timeout=Integer.parseInt(str);
            props.remove("sock_conn_timeout");
        }

        str=props.getProperty("peer_addr_read_timeout");
        if(str != null) {
            peer_addr_read_timeout=Integer.parseInt(str);
            props.remove("peer_addr_read_timeout");
        }

        str=props.getProperty("recv_buf_size");
        if(str != null) {
            recv_buf_size=Integer.parseInt(str);
            props.remove("recv_buf_size");
        }

        str=props.getProperty("send_buf_size");
        if(str != null) {
            send_buf_size=Integer.parseInt(str);
            props.remove("send_buf_size");
        }

        str=props.getProperty("skip_suspected_members");
        if(str != null) {
            skip_suspected_members=Boolean.valueOf(str).booleanValue();
            props.remove("skip_suspected_members");
        }

        str=props.getProperty("suspect_on_send_failure");
        if(str != null) {
            suspect_on_send_failure=Boolean.valueOf(str).booleanValue();
            props.remove("suspect_on_send_failure");
        }

        str=props.getProperty("use_send_queues");
        if(str != null) {
            use_send_queues=Boolean.valueOf(str).booleanValue();
            props.remove("use_send_queues");
        }

        str=props.getProperty("send_queue_size");
        if(str != null) {
            send_queue_size=Integer.parseInt(str);
            props.remove("send_queue_size");
        }

        str=props.getProperty("tcp_nodelay");
        if(str != null) {
            tcp_nodelay=Boolean.parseBoolean(str);
            props.remove("tcp_nodelay");
        }

        str=props.getProperty("linger");
        if(str != null) {
            linger=Integer.parseInt(str);
            props.remove("linger");
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

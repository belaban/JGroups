// $Id: TCP.java,v 1.2 2003/09/24 23:19:40 belaban Exp $

package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.jgroups.blocks.ConnectionTable;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Vector;
import java.util.HashMap;




/**
 * TCP based protocol. Creates a server socket, which gives us the local address of this group member. For
 * each accept() on the server socket, a new thread is created that listens on the socket.
 * For each outgoing message m, if m.dest is in the ougoing hashtable, the associated socket will be reused
 * to send message, otherwise a new socket is created and put in the hashtable.
 * When a socket connection breaks or a member is removed from the group, the corresponding items in the
 * incoming and outgoing hashtables will be removed as well.<br>
 * This functionality is in ConnectionTable, which isT used by TCP. TCP sends messages using ct.send() and
 * registers with the connection table to receive all incoming messages.
 * @author Bela Ban
 */
public class TCP extends Protocol implements ConnectionTable.Receiver, ConnectionTable.ConnectionListener {
    private ConnectionTable ct=null;
    private Address         local_addr=null;
    private String          group_addr=null;
    private InetAddress     bind_addr=null;  // local IP address to bind srv sock to (m-homed systems)
    private int             start_port=7800; // find first available port starting at this port
    private Vector          members=new Vector();
    private long            reaper_interval=0;  // time in msecs between connection reaps
    private long            conn_expire_time=0; // max time a conn can be idle before being reaped
    boolean                 loopback=false;     // loops back msgs to self if true

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
      * for example transport independent addresses */
     byte[]                 additional_data=null;



    public TCP() {
    }

    public String toString() {
        return "Protocol TCP(local address: " + local_addr + ")";
    }

    public String getName() {
        return "TCP";
    }


    /**
     DON'T REMOVE ! This prevents the up-handler thread to be created, which essentially is superfluous:
     messages are received from the network rather than from a layer below.
     */
    public void startUpHandler() {
        ;
    }


    public void start() throws Exception {
        if(reaper_interval == 0 && conn_expire_time == 0) {
            ct=new ConnectionTable(this, bind_addr, start_port);
        }
        else {
            if(reaper_interval == 0) {
                reaper_interval=5000;
                Trace.warn("TCP.start()", "reaper_interval was 0, set it to " + reaper_interval);
            }
            if(conn_expire_time == 0) {
                conn_expire_time=1000 * 60 * 5;
                Trace.warn("TCP.start()", "conn_expire_time was 0, set it to " + conn_expire_time);
            }
            ct=new ConnectionTable(this, bind_addr, start_port, reaper_interval, conn_expire_time);
        }
        ct.addConnectionListener(this);
        local_addr=ct.getLocalAddress();
        if(additional_data != null && local_addr instanceof IpAddress)
            ((IpAddress)local_addr).setAdditionalData(additional_data);
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    public void stop() {
        ct.stop();
    }


    /**
     Sent to destination(s) using the ConnectionTable class.
     */
    public void down(Event evt) {
        Message msg;
        Object dest_addr;

        if(evt.getType() != Event.MSG) {
            handleDownEvent(evt);
            return;
        }

        msg=(Message)evt.getArg();

        if(group_addr != null) { // added patch sent by Roland Kurmann (bela March 20 2003)
            /* Add header (includes channel name) */
            msg.putHeader(getName(), new TcpHeader(group_addr));
        }

        dest_addr=msg.getDest();


        /* Because we don't call Protocol.passDown(), we notify the observer directly (e.g. PerfObserver). This way,
           we still have performance numbers for TCP */
        if(observer != null)
            observer.passDown(evt);

        if(dest_addr == null) {  // broadcast (to all members)
            if(group_addr == null) {
                Trace.warn("TCP.down()", "dest address of message is null, and " +
                                         "sending to default address fails as group_addr is null, too !" +
                                         " Discarding message.");
                return;
            }
            else {
                sendMulticastMessage(msg);  // send to current membership
            }
        }
        else {
            sendUnicastMessage(msg);        // send to a single member
        }
    }


    /** ConnectionTable.Receiver interface */
    public void receive(Message msg) {
        TcpHeader hdr=null;
        Event     evt=new Event(Event.MSG, msg);


        /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
           This allows e.g. PerfObserver to get the time of reception of a message */
        if(observer != null)
            observer.up(evt, up_queue.size());


        if(Trace.trace) Trace.info("TCP.receive()", "received msg " + msg);
        //if(Trace.trace)
          //  Trace.info("TCP.receive()", "src=" + msg.getSrc() + ", hdrs:\n" + msg.printObjectHeaders());


        hdr=(TcpHeader)msg.removeHeader(getName());

        if(hdr != null) {
            /* Discard all messages destined for a channel with a different name */
            String ch_name=null;

            if(hdr.group_addr != null)
                ch_name=hdr.group_addr;

            // below lines were commented as patch sent by Roland Kurmann (bela March 20 2003)

//             if(group_addr == null) {
//                if(Trace.trace) Trace.warn("TCP.receive()", "group address in header was null, discarded");
//              return;
//             }

            // Discard if message's group name is not the same as our group name unless the
            // message is a diagnosis message (special group name DIAG_GROUP)
            if(ch_name != null && !group_addr.equals(ch_name) &&
                    !ch_name.equals(Util.DIAG_GROUP)) {
                Trace.warn("TCP.receive()", "discarded message from different group (" +
                                            ch_name + "). Sender was " + msg.getSrc());
                return;
            }
        }

        passUp(evt);
    }


    // ConnectionTable.ConnectionListener interface
    public void connectionOpened(Address peer_addr) {
        if(Trace.trace)
            Trace.info("TCP.connectionOpened()", "opened connection to " + peer_addr);
    }

    public void connectionClosed(Address peer_addr) {
        if(peer_addr != null && Trace.trace)
            Trace.info("TCP.connectionClosed()", "closed connection to " + peer_addr);
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("start_port");
        if(str != null) {
            start_port=new Integer(str).intValue();
            props.remove("start_port");
        }

        str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                Trace.fatal("TCP.setProperties()", "(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
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

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=new Boolean(str).booleanValue();
            props.remove("loopback");
        }

        if(props.size() > 0) {
            System.err.println("TCP.setProperties(): the following properties are not recognized:");
            props.list(System.out);
            return false;
        }
        return true;
    }


    /**
     If the sender is null, set our own address. We cannot just go ahead and set the address
     anyway, as we might be sending a message on behalf of someone else ! E.g. in case of
     retransmission, when the original sender has crashed, or in a FLUSH protocol when we
     have to return all unstable messages with the FLUSH_OK response.
     */
    private void setSourceAddress(Message msg) {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
    }


    /** Send a message to the address specified in msg.dest */
    private void sendUnicastMessage(Message msg) {
        IpAddress dest;
        Message copy;
        Object hdr;
        Event evt;

        dest=(IpAddress)msg.getDest();  // guaranteed not to be null
        if(!(dest instanceof IpAddress)) {
            Trace.error("TCP.sendUnicastMessage()", "destination address is not of type IpAddress !");
            return;
        }
        setSourceAddress(msg);

        /* Don't send if destination is local address. Instead, switch dst and src and put in up_queue  */
        if(loopback && local_addr != null && dest != null && dest.equals(local_addr)) {
            copy=msg.copy();
            hdr=copy.getHeader(getName());
            if(hdr != null && hdr instanceof TcpHeader)
                copy.removeHeader(getName());
            copy.setSrc(local_addr);
            copy.setDest(local_addr);

            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());

            passUp(evt);
            return;
        }
        //if(Trace.trace)
            //Trace.info("TCP.sendUnicastMessage()", "dest=" + msg.getDest() + ", hdrs:\n" + msg.printObjectHeaders());
        ct.send(msg);
    }


    private void sendMulticastMessage(Message msg) {
        Address dest;
        Vector mbrs=(Vector)members.clone();

//        if(Trace.trace)
//            Trace.info("TCP.sendMulticastMessage()", "sending message to " + msg.getDest() +
//                                                     ", mbrs=" + mbrs);
        for(int i=0; i < mbrs.size(); i++) {
            dest=(Address)mbrs.elementAt(i);
            msg.setDest(dest);
            sendUnicastMessage(msg);
        }
    }


    private void handleDownEvent(Event evt) {
        switch(evt.getType()) {

            case Event.TMP_VIEW:
            case Event.VIEW_CHANGE:
                synchronized(members) {
                    members.removeAllElements();
                    Vector tmpvec=((View)evt.getArg()).getMembers();
                    for(int i=0; i < tmpvec.size(); i++)
                        members.addElement(tmpvec.elementAt(i));
                }
                break;

            case Event.GET_LOCAL_ADDRESS:   // return local address -> Event(SET_LOCAL_ADDRESS, local)
                passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
                break;

            case Event.CONNECT:
                group_addr=(String)evt.getArg();

                // removed March 18 2003 (bela), not needed (handled by GMS)
                // Can't remove it; otherwise TCPGOSSIP breaks (bela May 8 2003) !
                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                passUp(new Event(Event.DISCONNECT_OK));
                break;

            case Event.CONFIG:
                if(Trace.trace) Trace.info("UDP.down()", "received CONFIG event: " + evt.getArg());
                handleConfigEvent((HashMap)evt.getArg());
                break;

            case Event.ACK:
                passUp(new Event(Event.ACK_OK));
                break;
        }
    }


    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
    }


}

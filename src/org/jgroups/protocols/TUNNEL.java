// $Id: TUNNEL.java,v 1.9 2004/10/07 10:10:37 belaban Exp $


package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.RouterStub;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;




/**
 * Replacement for UDP. Instead of sending packets via UDP, a TCP connection is opened to a Router
 * (using the RouterStub client-side stub),
 * the IP address/port of which was given using channel properties <code>router_host</code> and
 * <code>router_port</code>. All outgoing traffic is sent via this TCP socket to the Router which
 * distributes it to all connected TUNNELs in this group. Incoming traffic received from Router will
 * simply be passed up the stack.
 * <p>A TUNNEL layer can be used to penetrate a firewall, most firewalls allow creating TCP connections
 * to the outside world, however, they do not permit outside hosts to initiate a TCP connection to a host
 * inside the firewall. Therefore, the connection created by the inside host is reused by Router to
 * send traffic from an outside host to a host inside the firewall.
 * @author Bela Ban
 */
public class TUNNEL extends Protocol implements Runnable {
    final Properties properties=null;
    String channel_name=null;
    final Vector members=new Vector();
    String router_host=null;
    int router_port=0;
    Address local_addr=null;  // sock's local addr and local port
    Thread receiver=null;
    RouterStub stub=null;
    private final Object stub_mutex=new Object();

    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean  loopback=true;

    TimeScheduler timer=null;

    Reconnector reconnector=null;
    private final Object reconnector_mutex=new Object();


    public TUNNEL() {
    }


    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ')';
    }




    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "TUNNEL";
    }

    public void init() throws Exception {
        super.init();
        timer=stack.timer;
    }

    public void start() throws Exception {
        createTunnel();  // will generate and pass up a SET_LOCAL_ADDRESS event
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }

    public void stop() {
        if(receiver != null)
            receiver=null;
        teardownTunnel();
        stopReconnector();
    }



    /**
     * DON'T REMOVE ! This prevents the up-handler thread to be created, which essentially is superfluous:
     * messages are received from the network rather than from a layer below.
     */
    public void startUpHandler() {
        ;
    }

    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("router_host");
        if(str != null) {
            router_host=str;
            props.remove("router_host");
        }

        str=props.getProperty("router_port");
        if(str != null) {
            router_port=Integer.parseInt(str);
            props.remove("router_port");
        }

        if(log.isDebugEnabled()) {
            log.debug("router_host=" + router_host + ";router_port=" + router_port);
        }

        if(router_host == null || router_port == 0) {
            if(log.isErrorEnabled()) {
                log.error("both router_host and router_port have to be set !");
                return false;
            }
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
        }

        if(props.size() > 0) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            if(log.isErrorEnabled()) log.error("The following properties are not recognized: " + sb);
            return false;
        }
        return true;
    }


    /** Caller by the layer above this layer. We just pass it on to the router. */
    public void down(Event evt) {
        Message      msg;
        TunnelHeader hdr;
        Address dest;

        if(log.isDebugEnabled()) {
            log.debug(evt.toString());
        }

        if(evt.getType() != Event.MSG) {
            handleDownEvent(evt);
            return;
        }

        hdr=new TunnelHeader(channel_name);
        msg=(Message)evt.getArg();
        dest=msg.getDest();
        msg.putHeader(getName(), hdr);

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(loopback && (dest == null || dest.equals(local_addr) || dest.isMulticastAddress())) {
            Message copy=msg.copy();
            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            /* Because Protocol.up() is never called by this bottommost layer, we call up() directly in the observer.
               This allows e.g. PerfObserver to get the time of reception of a message */
            if(observer != null)
                observer.up(evt, up_queue.size());
            if(log.isTraceEnabled()) log.trace("looped back local message " + copy);
            passUp(evt);
            if(dest != null && !dest.isMulticastAddress())
                return;
        }

        if(!stub.isConnected() || !stub.send(msg, channel_name)) { // if msg is not sent okay,
            startReconnector();
        }
    }


    /** Creates a TCP connection to the router */
    void createTunnel() throws Exception {
        if(router_host == null || router_port == 0)
            throw new Exception("router_host and/or router_port not set correctly; tunnel cannot be created");

        synchronized(stub_mutex) {
            stub=new RouterStub(router_host, router_port);
            local_addr=stub.connect();
        }
        if(local_addr == null)
            throw new Exception("could not obtain local address !");
    }


    /** Tears the TCP connection to the router down */
    void teardownTunnel() {
        synchronized(stub_mutex) {
            if(stub != null) {
                stub.disconnect();
                stub=null;
            }
        }
    }

    /*--------------------------- End of Protocol interface -------------------------- */






    public void run() {
        Message msg;

        if(stub == null) {
            if(log.isErrorEnabled()) log.error("router stub is null; cannot receive messages from router !");
            return;
        }

        while(receiver != null) {
            msg=stub.receive();
            if(msg == null) {
                if(receiver == null) break;
                if(log.isErrorEnabled()) log.error("received a null message. Trying to reconnect to router");
                if(!stub.isConnected())
                    startReconnector();
                Util.sleep(5000);
                continue;
            }
            handleIncomingMessage(msg);
        }
    }





    /* ------------------------------ Private methods -------------------------------- */




    public void handleIncomingMessage(Message msg) {
        TunnelHeader hdr=(TunnelHeader)msg.removeHeader(getName());

        // discard my own multicast loopback copy
        if(loopback) {
            Address dst=msg.getDest();
            Address src=msg.getSrc();

            if(dst != null && dst.isMulticastAddress() && src != null && local_addr.equals(src)) {
                if(log.isTraceEnabled())
                    log.trace("discarded own loopback multicast packet");
                return;
            }
        }

         if(log.isDebugEnabled()) {
             log.debug("received message " + msg);
         }


        /* Discard all messages destined for a channel with a different name */

        String ch_name=hdr != null? hdr.channel_name : null;
        if(ch_name != null && !channel_name.equals(ch_name))
            return;

        passUp(new Event(Event.MSG, msg));
    }


    void handleDownEvent(Event evt) {

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

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.CONNECT:
                channel_name=(String)evt.getArg();
                if(stub == null) {
                    if(log.isErrorEnabled()) log.error("CONNECT:  router stub is null!");
                }
                else {
                    stub.register(channel_name);
                }

                receiver=new Thread(this, "TUNNEL receiver thread");
                receiver.setDaemon(true);
                receiver.start();

                passUp(new Event(Event.CONNECT_OK));
                break;

            case Event.DISCONNECT:
                if(receiver != null) {
                    receiver=null;
                    if(stub != null)
                        stub.disconnect();
                }
                teardownTunnel();
                passUp(new Event(Event.DISCONNECT_OK));
                passUp(new Event(Event.SET_LOCAL_ADDRESS, null));
                break;

            case Event.ACK:
                passUp(new Event(Event.ACK_OK));
                break;
        }
    }

    private void startReconnector() {
        synchronized(reconnector_mutex) {
            if(reconnector == null || reconnector.cancelled()) {
                reconnector=new Reconnector();
                timer.add(reconnector);
            }
        }
    }

    private void stopReconnector() {
        synchronized(reconnector_mutex) {
            if(reconnector != null) {
                reconnector.stop();
                reconnector=null;
            }
        }
    }

    /* ------------------------------------------------------------------------------- */


    private class Reconnector implements TimeScheduler.Task {
        boolean cancelled=false;


        public void stop() {
            cancelled=true;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return 5000;
        }

        public void run() {
            if(stub.reconnect()) {
                stub.register(channel_name);
                if(log.isDebugEnabled()) log.debug("reconnected");
                stop();
            }
        }
    }


}

// $Id: TUNNEL.java,v 1.30 2006/12/31 06:26:58 belaban Exp $


package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;
import java.net.InetAddress;
import java.net.UnknownHostException;


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
    RouterStub stub=new RouterStub();
    InetAddress bind_addr=null;
    private final Object stub_mutex=new Object();

    /** If true, messages sent to self are treated specially: unicast messages are
     * looped back immediately, multicast messages get a local copy first and -
     * when the real copy arrives - it will be discarded. Useful for Window
     * media (non)sense */
    boolean  loopback=true;

    private final Reconnector reconnector=new Reconnector();
    private final Object reconnector_mutex=new Object();

    /** If set it will be added to <tt>local_addr</tt>. Used to implement
     * for example transport independent addresses */
    byte[]          additional_data=null;

    /** time to wait in ms between reconnect attempts */
    long            reconnect_interval=5000;




    public TUNNEL() {
    }


    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ')';
    }


    public boolean isConnected() {
        return stub.isConnected();
    }

    public RouterStub getRouterStub() {
        return stub;
    }

    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "TUNNEL";
    }

    public void init() throws Exception {
        super.init();
    }


    public void start() throws Exception {
        super.start();
        local_addr=stub.getLocalAddress();
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }


    public void stop() {
        if(receiver != null)
            receiver=null;
        teardownTunnel();
        stopReconnector();
        local_addr=null;
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

        str=props.getProperty("reconnect_interval");
        if(str != null) {
            reconnect_interval=Long.parseLong(str);
            props.remove("reconnect_interval");
        }

        str=props.getProperty("loopback");
        if(str != null) {
            loopback=Boolean.valueOf(str).booleanValue();
            props.remove("loopback");
        }

        boolean ignore_systemprops=Util.isBindAddressPropertyIgnored();
        str=Util.getProperty(new String[]{Global.BIND_ADDR, Global.BIND_ADDR_OLD}, props, "bind_addr",
                             ignore_systemprops, null);
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                log.error("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        if(bind_addr != null)
            stub.setBindAddress(bind_addr);

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

        if(trace)
            log.trace(msg + ", hdrs: " + msg.printHeaders());

        // Don't send if destination is local address. Instead, switch dst and src and put in up_queue.
        // If multicast message, loopback a copy directly to us (but still multicast). Once we receive this,
        // we will discard our own multicast message
        if(loopback && (dest == null || dest.equals(local_addr) || dest.isMulticastAddress())) {
            Message copy=msg.copy();
            // copy.removeHeader(name); // we don't remove the header
            copy.setSrc(local_addr);
            // copy.setDest(dest);
            evt=new Event(Event.MSG, copy);

            if(trace) log.trace("looped back local message " + copy);
            passUp(evt);
            if(dest != null && !dest.isMulticastAddress())
                return;
        }



        if(!stub.isConnected()) {
            startReconnector();
        }
        else {
            if(stub.send(msg, channel_name) == false) {
                startReconnector();
            }
        }
    }


    /** Creates a TCP connection to the router */
    void createTunnel() throws Exception {
        if(router_host == null || router_port == 0)
            throw new Exception("router_host and/or router_port not set correctly; tunnel cannot be created");

        synchronized(stub_mutex) {
            stub.connect(channel_name, router_host, router_port);
            if(additional_data != null && local_addr instanceof IpAddress)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
        }
    }


    /** Tears the TCP connection to the router down */
    void teardownTunnel() {
        stub.disconnect();
    }

    /*--------------------------- End of Protocol interface -------------------------- */






    public void run() {
        Message msg;

        while(receiver != null && Thread.currentThread().equals(receiver)) {
            try {
                msg=stub.receive();
                if(msg == null) {
                    if(receiver == null) break;
                    if(log.isTraceEnabled()) log.trace("received a null message. Trying to reconnect to router");
                    if(!stub.isConnected())
                        startReconnector();
                    Util.sleep(5000);
                    continue;
                }
                handleIncomingMessage(msg);
            }
            catch(Exception e) {
                if(receiver == null || !Thread.currentThread().equals(receiver))
                    return;
                else {
                    if(log.isTraceEnabled())
                        log.trace("exception in receiver thread", e);
                }
            }
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
                if(trace)
                    log.trace("discarded own loopback multicast packet");
                return;
            }
        }

         if(trace)
             log.trace(msg + ", hdrs: " + msg.printHeaders());

        /* Discard all messages destined for a channel with a different name */

        String ch_name=hdr != null? hdr.channel_name : null;
        if(ch_name != null && !channel_name.equals(ch_name))
            return;

        passUp(new Event(Event.MSG, msg));
    }


    void handleDownEvent(Event evt) {
        if(trace)
            log.trace(evt);

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
            if(local_addr instanceof IpAddress && additional_data != null)
                ((IpAddress)local_addr).setAdditionalData(additional_data);
            break;

        case Event.CONNECT:
            channel_name=(String)evt.getArg();
            if(stub == null) {
                if(log.isErrorEnabled()) log.error("CONNECT:  router stub is null!");
            }
            else {
                try {
                    createTunnel();
                }
                catch(Exception e) {
                    if(log.isErrorEnabled())
                        log.error("failed connecting to GossipRouter at " + router_host + ":" + router_port);
                    break;
                }
            }

            receiver=new Thread(this, "TUNNEL receiver thread");
            receiver.setDaemon(true);
            receiver.start();

            passUp(new Event(Event.CONNECT_OK));
            break;

        case Event.DISCONNECT:
            if(receiver != null) {
                receiver=null;
                stub.disconnect();
            }
            teardownTunnel();
            passUp(new Event(Event.DISCONNECT_OK));
            passUp(new Event(Event.SET_LOCAL_ADDRESS, null));
            local_addr=null;
            break;

        case Event.CONFIG:
            if(log.isDebugEnabled()) log.debug("received CONFIG event: " + evt.getArg());
            handleConfigEvent((HashMap)evt.getArg());
            break;
        }
    }

    private void startReconnector() {
        synchronized(reconnector_mutex) {
            reconnector.start();
        }
    }

    private void stopReconnector() {
        synchronized(reconnector_mutex) {
            reconnector.stop();
        }
    }

    void handleConfigEvent(HashMap map) {
        if(map == null) return;
        if(map.containsKey("additional_data"))
            additional_data=(byte[])map.get("additional_data");
    }

    /* ------------------------------------------------------------------------------- */


    private class Reconnector implements Runnable {
        Thread  my_thread=null;


        public void start() {
            synchronized(this) {
                if(my_thread == null || !my_thread.isAlive()) {
                    my_thread=new Thread(this, "Reconnector");
                    my_thread.setDaemon(true);
                    my_thread.start();
                }
            }
        }

        public void stop() {
            synchronized(this) {
                my_thread=null;
            }
        }


        public void run() {
            while(Thread.currentThread().equals(my_thread)) {
                try {
                    stub.reconnect();
                    if(log.isTraceEnabled()) log.trace("reconnected");
                    break;
                }
                catch(Exception e) {
                }
                Util.sleep(reconnect_interval);
            }
        }
    }


}

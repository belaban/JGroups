// $Id: TUNNEL.java,v 1.1 2003/09/09 01:24:11 belaban Exp $


package org.jgroups.protocols;


import java.util.Properties;
import java.util.Vector;
import java.util.Enumeration;

import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;




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
    Properties properties=null;
    String channel_name=null;
    Vector members=new Vector();
    String router_host=null;
    int router_port=0;
    Address local_addr=null;  // sock's local addr and local port
    Thread receiver=null;
    RouterStub stub=null;


    public TUNNEL() {
    }


    public String toString() {
        return "Protocol TUNNEL(local_addr=" + local_addr + ")";
    }




    /*------------------------------ Protocol interface ------------------------------ */

    public String getName() {
        return "TUNNEL";
    }

    public void start() throws Exception {
        createTunnel();  // will generate and pass up a SET_LOCAL_ADDRESS event
        passUp(new Event(Event.SET_LOCAL_ADDRESS, local_addr));
    }

    public void stop() {
        if(receiver != null) {
            receiver=null;
            if(stub != null)
                stub.disconnect();
        }
        teardownTunnel();
    }


    /** Setup the Protocol instance acording to the configuration string */
    public boolean setProperties(Properties props) {
        String str;

        str=props.getProperty("router_host");
        if(str != null) {
            router_host=new String(str);
            props.remove("router_host");
        }

        str=props.getProperty("router_port");
        if(str != null) {
            router_port=new Integer(str).intValue();
            props.remove("router_port");
        }

        if(Trace.trace)
            Trace.info("TUNNEL.setProperties()", "router_host=" + router_host + ";router_port=" + router_port);

        if(router_host == null || router_port == 0) {
            Trace.error("TUNNEL.setProperties()", "both router_host and router_port have to be set !");
            //System.exit(-1);
        }

        if(props.size() > 0) {
            StringBuffer sb=new StringBuffer();
            for(Enumeration e=props.propertyNames(); e.hasMoreElements();) {
                sb.append(e.nextElement().toString());
                if(e.hasMoreElements()) {
                    sb.append(", ");
                }
            }
            Trace.error("TUNNEL.setProperties()", "The following properties are not recognized: " + sb.toString());
            return false;
        }
        return true;
    }


    /** Caller by the layer above this layer. We just pass it on to the router. */
    public synchronized void down(Event evt) {
        Message      msg;
        TunnelHeader hdr;

        if(Trace.trace) Trace.info("TUNNEL.down()", "event is " + evt);

        if(evt.getType() != Event.MSG) {
            handleDownEvent(evt);
            return;
        }

        hdr=new TunnelHeader(channel_name);
        msg=(Message)evt.getArg();
        msg.putHeader(getName(), hdr);

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        if(!stub.send(msg, channel_name)) { // if msg is not sent okay,
            if(stub.reconnect())                 // try reconnecting until router is ok again
                stub.register(channel_name);
        }
    }


    /** Creates a TCP connection to the router */
    void createTunnel() throws Exception {
        if(router_host == null || router_port == 0)
            throw new Exception("router_host and/or router_port not set correctly; tunnel cannot be created");

        stub=new RouterStub(router_host, router_port);
        local_addr=stub.connect();
        if(local_addr == null)
            throw new Exception("could not obtain local address !");
    }


    /** Tears the TCP connection to the router down */
    synchronized void teardownTunnel() {
        if(stub != null) {
            stub.disconnect();
            stub=null;
        }
    }

    /*--------------------------- End of Protocol interface -------------------------- */






    public void run() {
        Message msg;

        if(stub == null) {
            Trace.error("TUNNEL.run()", "router stub is null; cannot receive messages from router !");
            return;
        }

        while(receiver != null) {
            msg=stub.receive();
            if(msg == null) {
                if(receiver == null) break;
                Trace.error("TUNNEL.run()", "received a null message. Trying to reconnect to router");
                if(stub.reconnect()) {
                    stub.register(channel_name);
                    Trace.warn("TUNNEL.run()", "Reconnected!");
                }
                continue;
            }
            handleIncomingMessage(msg);
        }
    }





    /* ------------------------------ Private methods -------------------------------- */




    public void handleIncomingMessage(Message msg) {
        TunnelHeader hdr=(TunnelHeader)msg.removeHeader(getName());

        if(Trace.trace) Trace.info("TUNNEL.handleIncomingMessage()", "received msg " + msg);


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
                if(stub == null)
                    Trace.error("TUNNEL.handleDownEvent()", "CONNECT:  router stub is null!");
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
        }
    }



    /* ------------------------------------------------------------------------------- */


}

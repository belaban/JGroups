package org.jgroups.protocols;

import org.jgroups.Message;

import java.util.Properties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.MulticastSocket;

/**
 * Uses its own IP multicast socket to send and receive discovery requests/responses. Can be used in
 * conjuntion with a non-UDP transport, e.g. TCP.
 * @author Bela Ban
 * @version $Id: MPING.java,v 1.1 2005/03/30 22:15:29 belaban Exp $
 */
public class MPING extends PING implements Runnable {
    MulticastSocket mcast_sock=null;
    Thread         receiver=null;
    InetAddress    bind_addr=null;
    int            ip_ttl=16;
    InetAddress    mcast_addr=null;
    int            mcast_port=0;
    static final String IGNORE_BIND_ADDRESS_PROPERTY="ignore.bind.address";


    public boolean setProperties(Properties props) {
        String tmp=null, str;

        // PropertyPermission not granted if running in an untrusted environment with JNLP.
        try {
            tmp=System.getProperty("bind.address");
            if(Boolean.getBoolean(IGNORE_BIND_ADDRESS_PROPERTY)) {
                tmp=null;
            }
        }
        catch (SecurityException ex){
        }

        if(tmp != null)
            str=tmp;
        else
            str=props.getProperty("bind_addr");
        if(str != null) {
            try {
                bind_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException unknown) {
                if(log.isFatalEnabled()) log.fatal("(bind_addr): host " + str + " not known");
                return false;
            }
            props.remove("bind_addr");
        }

        str=props.getProperty("mcast_addr");
        if(str != null) {
            try {
                mcast_addr=InetAddress.getByName(str);
            }
            catch(UnknownHostException e) {
                log.error("could not resolve " + str, e);
                return false;
            }
            props.remove("mcast_addr");
        }

        str=props.getProperty("mcast_port");
        if(str != null) {
            mcast_port=Integer.parseInt(str);
            props.remove("mcast_port");
        }

        str=props.getProperty("ip_ttl");
        if(str != null) {
            ip_ttl=Integer.parseInt(str);
            props.remove("ip_ttl");
        }

        return super.setProperties(props);
    }


    public void start() throws Exception {
        if(bind_addr == null) {
            InetAddress[] interfaces=InetAddress.getAllByName(InetAddress.getLocalHost().getHostAddress());
            if(interfaces != null && interfaces.length > 0)
                bind_addr=interfaces[0];
        }
        if(bind_addr == null)
            bind_addr=InetAddress.getLocalHost();

        if(bind_addr != null)
            if(log.isInfoEnabled()) log.info("sockets will use interface " + bind_addr.getHostAddress());

        mcast_sock=new MulticastSocket(mcast_port);
        mcast_sock.setTimeToLive(ip_ttl);
        if(bind_addr != null)
            mcast_sock.setInterface(bind_addr);
        mcast_sock.joinGroup(mcast_addr);
        startReceiver();
        super.start();
    }

    private void startReceiver() {
        if(receiver == null || !receiver.isAlive()) {
            receiver=new Thread(this, "ReceiverThread");
            receiver.setDaemon(true);
            receiver.start();
            if(log.isTraceEnabled())
                log.trace("receiver thread started");
        }
    }

    public void stop() {
        mcast_sock.close();
        mcast_sock=null;
        receiver=null;
        super.stop();
    }

    void sendMcastDiscoveryRequest(Message discovery_request) {
        // todo: marshal msg into buffer, create datagram, send it
    }

    public void run() {
        while(mcast_sock != null && receiver != null && Thread.currentThread().equals(receiver)) {
            // todo: receive datagram, convert to Message, pass up the stack
        }
        if(log.isTraceEnabled())
            log.trace("receiver thread terminated");
    }
}

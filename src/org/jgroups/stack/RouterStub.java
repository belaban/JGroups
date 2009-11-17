package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.protocols.TUNNEL.StubReceiver;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Client stub that talks to a remote GossipRouter
 * @author Bela Ban
 * @version $Id: RouterStub.java,v 1.54 2009/11/17 08:48:35 belaban Exp $
 */
public class RouterStub {

    public static enum ConnectionStatus {INITIAL, DISCONNECTED, CONNECTED};

    private final String router_host; // name of the router host

    private final int router_port; // port on which router listens on

    private Socket sock=null; // socket connecting to the router

    private DataOutputStream output=null;

    private DataInputStream input=null;

    private volatile ConnectionStatus connectionState=ConnectionStatus.INITIAL;

    private static final Log log=LogFactory.getLog(RouterStub.class);

    private ConnectionListener conn_listener;

    private final InetAddress bind_addr;

    private int sock_conn_timeout=3000; // max number of ms to wait for socket establishment to
    // GossipRouter

    private int sock_read_timeout=3000; // max number of ms to wait for socket reads (0 means block
    // forever, or until the sock is closed)
    
    private StubReceiver receiver;

    public interface ConnectionListener {
        void connectionStatusChange(ConnectionStatus state);
    }

    /**
     * Creates a stub for a remote Router object.
     * @param routerHost The name of the router's host
     * @param routerPort The router's port
     * @throws SocketException
     */
    public RouterStub(String routerHost, int routerPort, InetAddress bindAddress) {
        router_host=routerHost != null? routerHost : "localhost";
        router_port=routerPort;
        bind_addr=bindAddress;
    }
    
    public synchronized void setReceiver(StubReceiver receiver) {
        this.receiver = receiver;
    }
    
    public synchronized void interrupt() {
        if(receiver != null) {
            Thread thread = receiver.getThread();
            if(thread != null)
                thread.interrupt();
        }
    }


    public int getSocketConnectionTimeout() {
        return sock_conn_timeout;
    }

    public void setSocketConnectionTimeout(int sock_conn_timeout) {
        this.sock_conn_timeout=sock_conn_timeout;
    }

    public int getSocketReadTimeout() {
        return sock_read_timeout;
    }

    public void setSocketReadTimeout(int sock_read_timeout) {
        this.sock_read_timeout=sock_read_timeout;
    }

    public boolean isConnected() {
        return connectionState == ConnectionStatus.CONNECTED;
    }

    public ConnectionStatus getConnectionStatus() {
        return connectionState;
    }

    public void setConnectionListener(ConnectionListener conn_listener) {
        this.conn_listener=conn_listener;
    }


    /**
     * Register this process with the router under <code>group</code>.
     * @param group The name of the group under which to register
     */
    public synchronized void connect(String group, Address addr, String logical_name, List<PhysicalAddress> phys_addrs) throws Exception {
        doConnect();
        GossipData request=new GossipData(GossipRouter.CONNECT, group, addr, logical_name, phys_addrs);
        request.writeTo(output);
        output.flush();
    }

    public synchronized void doConnect() throws Exception {
        if(!isConnected()) {
            try {
                sock=new Socket();
                sock.bind(new InetSocketAddress(bind_addr, 0));
                sock.setSoTimeout(sock_read_timeout);
                sock.setSoLinger(true, 2);
                Util.connect(sock, new InetSocketAddress(router_host, router_port), sock_conn_timeout);
                output=new DataOutputStream(sock.getOutputStream());
                input=new DataInputStream(sock.getInputStream());
                connectionStateChanged(ConnectionStatus.CONNECTED);
            }
            catch(Exception e) {
                if(log.isWarnEnabled())
                    log.warn(this + " failed connecting to " + router_host + ":" + router_port);
                Util.close(sock);
                Util.close(input);
                Util.close(output);
                connectionStateChanged(ConnectionStatus.DISCONNECTED);
                throw e;
            }
        }
    }

    /**
     * Checks whether the connection is open
     * @return
     */
    public synchronized void checkConnection() {
        GossipData request=new GossipData(GossipRouter.PING);
        try {
            request.writeTo(output);
            output.flush();
        }
        catch(IOException e) {
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
        }
    }


    public synchronized void disconnect(String group, Address addr) {
        try {
            GossipData request=new GossipData(GossipRouter.DISCONNECT, group, addr);
            request.writeTo(output);
            output.flush();
        }
        catch(Exception e) {
        }
    }

    public synchronized void destroy() {
        try {
            GossipData request = new GossipData(GossipRouter.CLOSE);
            request.writeTo(output);
            output.flush();
        } catch (Exception e) {
        } finally {
            Util.close(output);
            Util.close(input);
            Util.close(sock);
        }
    }
    
    
    /*
     * Used only in testing, never access socket directly
     * 
     */
    public Socket getSocket() {
        return sock;
    }


    public synchronized List<PingData> getMembers(final String group) throws Exception {
        List<PingData> retval=new ArrayList<PingData>();
        try {

            GossipData request=new GossipData(GossipRouter.GOSSIP_GET, group, null);
            request.writeTo(output);
            output.flush();

            short num_rsps=input.readShort();
            for(int i=0; i < num_rsps; i++) {
                PingData rsp=new PingData();
                rsp.readFrom(input);
                retval.add(rsp);
            }
        }
        catch(SocketException se) {
            if(log.isWarnEnabled())
                log.warn("Router stub " + this + " did not send message", se);
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
        }
        catch(Exception e) {
            if(log.isErrorEnabled())
                log.error("Router stub " + this + " failed sending message to router");
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
            throw new Exception("Connection broken", e);
        }
        return retval;
    }

    public InetSocketAddress getGossipRouterAddress() {
        return new InetSocketAddress(router_host, router_port);
    }
    
    public String toString() {
        return "RouterStub[localsocket=" + ((sock != null) ? sock.getLocalSocketAddress().toString()
                        : "null")+ ",router_host=" + router_host + "::" + router_port
                                        + ",connected=" + isConnected() + "]";
    }

    public void sendToAllMembers(String group, byte[] data, int offset, int length) throws Exception {
        sendToMember(group, null, data, offset, length); // null destination represents mcast
    }

    public synchronized void sendToMember(String group, Address dest, byte[] data, int offset, int length)
            throws Exception {
        try {
            GossipData request=new GossipData(GossipRouter.MESSAGE, group, dest, data, offset, length);
            request.writeTo(output);
            output.flush();
        }
        catch(SocketException se) {
            if(log.isWarnEnabled())
                log.warn("Router stub " + this + " did not send message to "
                        + (dest == null? "mcast" : dest + " since underlying socket is closed"));
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
            throw new Exception("dest=" + dest + " (" + length + " bytes)", se);
        }
        catch(Exception e) {
            if(log.isErrorEnabled())
                log.error("Router stub " + this + " failed sending message to router");
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
            throw new Exception("dest=" + dest + " (" + length + " bytes)", e);
        }
    }

    public DataInputStream getInputStream() {
        return input;
    }

    private void connectionStateChanged(ConnectionStatus newState) {
        boolean notify=connectionState != newState;
        connectionState=newState;
        if(notify && conn_listener != null) {
            try {
                conn_listener.connectionStatusChange(newState);
            }
            catch(Throwable t) {
                log.error("failed notifying ConnectionListener " + conn_listener, t);
            }
        }
    }
}

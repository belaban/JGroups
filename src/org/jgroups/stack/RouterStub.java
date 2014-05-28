package org.jgroups.stack;

import org.jgroups.Address;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.TUNNEL.StubReceiver;
import org.jgroups.util.Responses;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client stub that talks to a remote GossipRouter
 * @author Bela Ban
 */
public class RouterStub implements Comparable<RouterStub> {

    public static enum ConnectionStatus {INITIAL, CONNECTION_BROKEN, CONNECTION_ESTABLISHED, CONNECTED,DISCONNECTED};

    protected final String router_host; // name of the router host

    protected final int router_port; // port on which router listens on

    protected Socket sock=null; // socket connecting to the router

    protected DataOutputStream output=null;

    protected DataInputStream input=null;

    protected volatile ConnectionStatus connectionState=ConnectionStatus.INITIAL;

    protected static final Log log=LogFactory.getLog(RouterStub.class);

    protected final ConnectionListener conn_listener;

    protected final InetAddress bind_addr;

    protected int sock_conn_timeout=3000; // max number of ms to wait for socket establishment to
    // GossipRouter

    protected int sock_read_timeout=3000; // max number of ms to wait for socket reads (0 means block
    // forever, or until the sock is closed)

    protected boolean tcp_nodelay=true;
    
    protected volatile StubReceiver receiver;

    // used to synchronize access to socket, and input and output stream
    protected final ReentrantLock lock=new ReentrantLock();

    public interface ConnectionListener {
        void connectionStatusChange(RouterStub stub, ConnectionStatus state);
    }

    /**
     * Creates a stub for a remote Router object.
     * @param routerHost The name of the router's host
     * @param routerPort The router's port
     * @throws SocketException
     */
    public RouterStub(String routerHost, int routerPort, InetAddress bindAddress, ConnectionListener l) {
        router_host=routerHost != null? routerHost : "localhost";
        router_port=routerPort;
        bind_addr=bindAddress;
        conn_listener=l;        
    }

    public RouterStub(InetSocketAddress addr) {
        this(addr.getHostName(), addr.getPort(), null, null);
    }

    public void setReceiver(StubReceiver receiver) {
        this.receiver = receiver;
    }
    
    public StubReceiver  getReceiver() {
        return receiver;
    }

    public boolean isTcpNoDelay() {
        return tcp_nodelay;
    }

    public void setTcpNoDelay(boolean tcp_nodelay) {
        this.tcp_nodelay=tcp_nodelay;
    }

    // Note that this would fail to return 0 if we had a dotted decimal and a symbolic addr resolving to the same host !
    public int compareTo(RouterStub o) {
        int rc=router_host.compareTo(o.router_host);
        if(rc != 0)
            return rc;
        return router_port < o.router_port? -1 : router_port > o.router_port? 1 : 0;
    }

    public boolean equals(Object obj) {
        RouterStub o=(RouterStub)obj;
        return compareTo(o) == 0;
    }

    public int hashCode() {
        return router_host.hashCode() + router_port;
    }

    public void interrupt() {
        StubReceiver tmp=receiver;
        if(tmp != null) {
            Thread thread=tmp.getThread();
            if(thread != null)
                thread.interrupt();
        }
    }
    
    public void join(long wait) throws InterruptedException {
        StubReceiver tmp=receiver;
        if(tmp != null) {
            Thread thread=tmp.getThread();
            if(thread != null)
                thread.join(wait);
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
        return !(connectionState == ConnectionStatus.CONNECTION_BROKEN || connectionState == ConnectionStatus.INITIAL);
    }

    public ConnectionStatus getConnectionStatus() {
        return connectionState;
    }


    /**
     * Register this process with the router under <code>group</code>.
     * @param group The name of the group under which to register
     */
    public void connect(String group, Address addr, String logical_name, PhysicalAddress phys_addr) throws Exception {
        lock.lock();
        try {
            _doConnect();
            GossipData request=new GossipData(GossipRouter.CONNECT, group, addr, logical_name, phys_addr);
            request.writeTo(output);
            output.flush();
            byte result = input.readByte();
            if(result == GossipRouter.CONNECT_OK) {
                connectionStateChanged(ConnectionStatus.CONNECTED);
            } else {
                connectionStateChanged(ConnectionStatus.DISCONNECTED);
                throw new Exception("Connect failed received from GR " + getGossipRouterAddress());
            }
        }
        finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    public void doConnect() throws Exception {
        lock.lock();
        try {
            _doConnect();
        }
        finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    @GuardedBy("lock")
    protected void _doConnect() throws Exception {
        if(!isConnected()) {
            try {
                sock=new Socket();
                InetSocketAddress dest=new InetSocketAddress(router_host,router_port);
                InetAddress tmp_bind_addr=bind_addr;
                if(!Util.sameAddresses(bind_addr, tmp_bind_addr))
                    tmp_bind_addr=null;
                sock.bind(new InetSocketAddress(tmp_bind_addr, 0));
                sock.setSoTimeout(sock_read_timeout);
                sock.setSoLinger(true, 2);
                sock.setTcpNoDelay(tcp_nodelay);
                sock.setKeepAlive(true);
                Util.connect(sock, dest, sock_conn_timeout);
                output=new DataOutputStream(sock.getOutputStream());
                input=new DataInputStream(sock.getInputStream());
                connectionStateChanged(ConnectionStatus.CONNECTION_ESTABLISHED);
            }
            catch(Exception e) {
                Util.close(sock);
                Util.close(input, output);
                connectionStateChanged(ConnectionStatus.CONNECTION_BROKEN);
                throw new Exception("Could not connect to " + getGossipRouterAddress() , e);
            }
        }
    }


    /**
     * Checks whether the connection is open
     * @return
     */
    public void checkConnection() {
        GossipData request=new GossipData(GossipRouter.PING);
        lock.lock();
        try {
            request.writeTo(output);
            output.flush();
        }
        catch(Exception e) {
            connectionStateChanged(ConnectionStatus.CONNECTION_BROKEN);
        }
        finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }


    public void disconnect(String group, Address addr) {
        lock.lock();
        try {
            GossipData request=new GossipData(GossipRouter.DISCONNECT, group, addr);
            request.writeTo(output);
            output.flush();
        }
        catch(Exception e) {
        }
        finally {
            connectionStateChanged(ConnectionStatus.DISCONNECTED);
            if(lock.isHeldByCurrentThread()) // not needed as connectionStateChanged() unlocks, but this gets rid of the warning
                lock.unlock();
        }
    }

    public void destroy() {
        lock.lock();
        try {
            GossipData request = new GossipData(GossipRouter.CLOSE);
            request.writeTo(output);
            output.flush();
        }
        catch (Exception e) {
        }
        finally {
            Util.close(output);
            Util.close(input);
            Util.close(sock);
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }
    
    
    /*
     * Used only in testing, never access socket directly
     * 
     */
    public Socket getSocket() {
        return sock;
    }


    public void getMembers(final String group, Responses rsps) throws Exception {
        lock.lock();
        try {
            if(!isConnected() || input == null) throw new Exception ("not connected");
            // we might get a spurious SUSPECT message from the router, just ignore it
            if(input.available() > 0) // fixes https://jira.jboss.org/jira/browse/JGRP-1151
                input.skipBytes(input.available());

            GossipData request=new GossipData(GossipRouter.GOSSIP_GET, group, null);
            request.writeTo(output);
            output.flush();

            short num_rsps=input.readShort();
            for(int i=0; i < num_rsps; i++) {
                PingData rsp=new PingData();
                rsp.readFrom(input);
                rsps.addResponse(rsp, false);
            }
        }
        catch(Exception e) {           
            connectionStateChanged(ConnectionStatus.CONNECTION_BROKEN);
            throw new Exception("Connection to " + getGossipRouterAddress() + " broken. Could not send GOSSIP_GET request", e);
        }
        finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    public InetSocketAddress getGossipRouterAddress() {
        return new InetSocketAddress(router_host, router_port);
    }
    
    public String toString() {
        return "RouterStub[localsocket=" + ((sock != null) ? sock.getLocalSocketAddress()
                        : "null")+ ",router_host=" + router_host + "::" + router_port + ",connected=" + isConnected() + "]";
    }

    public void sendToAllMembers(String group, byte[] data, int offset, int length) throws Exception {
        sendToMember(group, null, data, offset, length); // null destination represents mcast
    }

    public void sendToMember(String group, Address dest, byte[] data, int offset, int length) throws Exception {
        lock.lock();
        try {
            GossipData request = new GossipData(GossipRouter.MESSAGE, group, dest, data, offset, length);
            request.writeTo(output);
            output.flush();
        }
        catch (Exception e) {
            connectionStateChanged(ConnectionStatus.CONNECTION_BROKEN);
            throw new Exception("Connection to " + getGossipRouterAddress()
                            + " broken. Could not send message to " + dest, e);
        }
        finally {
            if(lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    public DataInputStream getInputStream() {
        return input;
    }

    protected void connectionStateChanged(ConnectionStatus newState) {
        boolean notify=connectionState != newState;
        connectionState=newState;
        if(notify && conn_listener != null) {
            // release lock as the callback below might block: https://issues.jboss.org/browse/JGRP-1526
            if(lock.isHeldByCurrentThread())
                lock.unlock();
            try {
                conn_listener.connectionStatusChange(this, newState);
            }
            catch(Throwable t) {
                log.error("failed notifying ConnectionListener " + conn_listener, t);
            }
        }
    }
}

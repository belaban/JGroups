package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

/**
 * Class that manages allows to send and receive messages via TCP sockets. Uses 1 thread/connection to read messages.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class TcpServer extends TcpBaseServer {
    protected ServerSocket srv_sock;
    protected Thread       acceptor;



    /**
     * Creates an instance of {@link TcpServer} that creates a server socket and listens for connections
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param port The local port to bind to. If 0, the port will be picked by the OS.
     * @param port If srv_port is taken, the next port is tried, until end_port has been reached, in which case an
     *             exception will be thrown. If srv_port == end_port, only 1 port will be tried.
     * @return An instance of {@link TcpServer}. Needs to be started next.
     * @throws Exception Thrown if the creation failed
     */
    public TcpServer(InetAddress bind_addr, int port) throws Exception {
        this(new DefaultThreadFactory("tcp", false), new DefaultSocketFactory(), bind_addr, port, port+50, null, 0);
    }

    /**
     * Creates an instance of TcpServer.
     * @param bind_addr The local bind address and port. If null, a bind address and port will be picked by the OS.
     */
    public TcpServer(IpAddress bind_addr) throws Exception {
        this(bind_addr != null? bind_addr.getIpAddress() : null, bind_addr != null? bind_addr.getPort() : 0);
    }

    /**
     * Creates an instance of {@link TcpServer} that creates a server socket and listens for connections
     * @param thread_factory The thread factory used to create new threads
     * @param socket_factory The socket factory used to create sockets
     * @param bind_addr The local address to bind to. If null, a bind address will be picked by the OS.
     * @param end_port If srv_port is taken, the next port is tried, until end_port has been reached, in which case an
     *                 exception will be thrown. If srv_port == end_port, only 1 port will be tried.
     * @param external_addr The external address and port in case of NAT. Ignored if null.
     * @throws Exception Thrown if the creation failed
     */
    public TcpServer(ThreadFactory thread_factory, SocketFactory socket_factory,
                     IpAddress bind_addr, int end_port, IpAddress external_addr) throws Exception {
        this(thread_factory, socket_factory, bind_addr != null? bind_addr.getIpAddress() : null,
             bind_addr != null? bind_addr.getPort() : 0, end_port,
             external_addr != null? external_addr.getIpAddress() : null, external_addr != null? external_addr.getPort() : 0);
    }

    /**
     * Creates an instance of {@link TcpServer} that creates a server socket and listens for connections
     * @param thread_factory The thread factory used to create new threads
     * @param socket_factory The socket factory used to create sockets
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param srv_port The local port to bind to. If 0, the port will be picked by the OS.
     * @param end_port If srv_port is taken, the next port is tried, until end_port has been reached, in which case an
     *                 exception will be thrown. If srv_port == end_port, only 1 port will be tried.
     * @param external_addr The external address in case of NAT. Ignored if null.
     * @param external_port The external port on the NA. If 0, srv_port is used.
     * @return An instance of {@link TcpServer}. Needs to be started next.
     * @throws Exception Thrown if the creation failed
     */
    public TcpServer(ThreadFactory thread_factory, SocketFactory socket_factory,
                     InetAddress bind_addr, int srv_port, int end_port,
                     InetAddress external_addr, int external_port) throws Exception {
        this(thread_factory, socket_factory);
        this.srv_sock=Util.createServerSocket(this.socket_factory, "jgroups.tcp.server", bind_addr, srv_port, end_port);
        acceptor=factory.newThread(new Acceptor(),"TcpServer.Acceptor [" + srv_sock.getLocalPort() + "]");
        local_addr=localAddress(bind_addr, srv_sock.getLocalPort(), external_addr, external_port);
        addConnectionListener(this);
    }


    protected TcpServer(ThreadFactory thread_factory, SocketFactory socket_factory) {
        super(thread_factory);
        this.socket_factory=socket_factory;
    }


    @Override
    public void start() throws Exception {
        if(running.compareAndSet(false, true)) {
            acceptor.start();
            super.start();
        }
    }

    @Override
    public void stop() {
        if(running.compareAndSet(true, false)) {
            Util.close(srv_sock);
            Util.interruptAndWaitToDie(acceptor);
            super.stop();
        }
    }


    protected class Acceptor implements Runnable {
        /**
         * Acceptor thread. Continuously accept new connections. Create a new thread for each new connection and put
         * it in conns. When the thread should stop, it is interrupted by the thread creator.
         */
        public void run() {
            while(!srv_sock.isClosed() && !Thread.currentThread().isInterrupted()) {
                Socket client_sock=null;
                try {
                    client_sock=srv_sock.accept();
                    handleAccept(client_sock);
                }
                catch(Exception ex) {
                    if(ex instanceof SocketException && srv_sock.isClosed() || Thread.currentThread().isInterrupted())
                        break;
                    log.warn(Util.getMessage("AcceptError"), ex);
                    Util.close(client_sock);
                }
            }
        }


        protected void handleAccept(final Socket client_sock) throws Exception {
            TcpConnection conn=null;
            try {
                conn=new TcpConnection(client_sock, TcpServer.this);
                Address peer_addr=conn.peerAddress();
                synchronized(this) {
                    boolean conn_exists=hasConnection(peer_addr),
                      replace=conn_exists && use_peer_connections && local_addr.compareTo(peer_addr) < 0; // bigger conn wins

                    if(!conn_exists || replace) {
                        replaceConnection(peer_addr, conn); // closes old conn
                        conn.start();
                        log.trace("%s: accepted connection from %s", local_addr, peer_addr);
                    }
                    else {
                        log.trace("%s: rejected connection from %s %s", local_addr, peer_addr, explanation(conn_exists, replace));
                        Util.close(conn); // keep our existing conn, reject accept() and close client_sock
                    }
                }
            }
            catch(Exception ex) {
                Util.close(conn);
                throw ex;
            }
        }
    }



}


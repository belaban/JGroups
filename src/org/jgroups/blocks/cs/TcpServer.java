package org.jgroups.blocks.cs;

import org.jgroups.Address;
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
    protected int          buffered_inputstream_size;
    protected int          buffered_outputstream_size;
    protected boolean      log_accept_error=true;


    public int       getBufferedInputStreamSize()       {return buffered_inputstream_size;}
    public TcpServer setBufferedInputStreamSize(int s)  {this.buffered_inputstream_size=s; return this;}
    public int       getBufferedOutputStreamSize()      {return buffered_outputstream_size;}
    public TcpServer setBufferedOutputStreamSize(int s) {this.buffered_outputstream_size=s; return this;}
    public boolean   getLogAcceptError()                {return log_accept_error;}
    public TcpServer setLogAcceptError(boolean l)       {log_accept_error=l; return this;}


    /**
     * Creates an instance of {@link TcpServer} that creates a server socket and listens for connections.
     * The end port defaults to (port + 50).  Needs to be started next.
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param port The local port to bind to. If 0, the port will be picked by the OS.
     * @throws Exception Thrown if the creation failed
     */
    public TcpServer(InetAddress bind_addr, int port) throws Exception {
        this(new DefaultThreadFactory("tcp", false),
             new DefaultSocketFactory(), bind_addr, port, port+50, null, 0, 0);
    }



    /**
     * Creates an instance of {@link TcpServer} that creates a server socket and listens for connections
     * Needs to be started next.
     * @param thread_factory The thread factory used to create new threads
     * @param socket_factory The socket factory used to create sockets
     * @param bind_addr The local address to bind to. If null, the address will be picked by the OS
     * @param srv_port The local port to bind to. If 0, the port will be picked by the OS.
     * @param end_port If srv_port is taken, the next port is tried, until end_port has been reached, in which case an
     *                 exception will be thrown. If srv_port == end_port, only 1 port will be tried.
     * @param external_addr The external address in case of NAT. Ignored if null.
     * @param external_port The external port on the NA. If 0, srv_port is used.
     * @param recv_buf_size The size of the initial TCP receive window (in bytes)
     * @throws Exception Thrown if the creation failed
     */
    public TcpServer(ThreadFactory thread_factory, SocketFactory socket_factory,
                     InetAddress bind_addr, int srv_port, int end_port,
                     InetAddress external_addr, int external_port, int recv_buf_size) throws Exception {
        super(thread_factory, socket_factory, recv_buf_size);
        this.srv_sock=Util.createServerSocket(this.socket_factory, "jgroups.tcp.server", bind_addr,
                                              srv_port, end_port, recv_buf_size);
        acceptor=factory.newThread(new Acceptor(),"TcpServer.Acceptor[" + srv_sock.getLocalPort() + "]");
        local_addr=localAddress(bind_addr, srv_sock.getLocalPort(), external_addr, external_port);
        addConnectionListener(this);
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


    /** Acceptor thread. Continuously accepts new connections, creating a new thread for each new connection and
     *  putting it in conns. When the thread should stop, it is interrupted by the thread creator */
    protected class Acceptor implements Runnable {
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
                    if(log_accept_error)
                        log.warn(Util.getMessage("AcceptError"), client_sock, ex);
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


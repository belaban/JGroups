package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.nio.Receiver;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

/**
 * Class that manages allows to send and receive messages via TCP sockets. Uses 1 thread/connection to read messages.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class TcpServer extends BaseServer<Address,TcpConnection> {
    protected final ServerSocket  srv_sock;
    protected final Thread        acceptor;
    protected volatile boolean    use_send_queues=true;
    protected int                 send_queue_size=2000;
    protected int                 peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address

    public TcpServer(IpAddress local_addr) throws Exception {
        this("jgroups.tcp.server", new DefaultThreadFactory("tcp", false), new DefaultSocketFactory(), null, local_addr.getIpAddress(), null, 0,
             local_addr.getPort(), local_addr.getPort() + 50, 0, 0);
    }

    public TcpServer(InetAddress bind_addr, int port) throws Exception {
        this("jgroups.tcp.server", new DefaultThreadFactory("tcp", true),
             new DefaultSocketFactory(), null, bind_addr, null, 0, port, port+50, 0, 0);
    }



    public TcpServer(String service_name, ThreadFactory f, SocketFactory socket_factory, Receiver<Address> r,
                     InetAddress bind_addr, InetAddress external_addr, int external_port, int srv_port, int max_port,
                     long reaper_interval, long conn_expire_time) throws Exception {
        super(f, socket_factory, r, reaper_interval, conn_expire_time);

        this.srv_sock=Util.createServerSocket(this.socket_factory, service_name, bind_addr, srv_port, max_port);
        if(external_addr != null) {
            if(external_port <= 0)
                local_addr=new IpAddress(external_addr, srv_sock.getLocalPort());
            else
                local_addr=new IpAddress(external_addr, external_port);
        }
        else if(bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());

        acceptor=f.newThread(new Acceptor(),"TcpServer.Acceptor [" + local_addr + "]");
    }



    public int       peerAddressReadTimeout()                {return peer_addr_read_timeout;}
    public TcpServer peerAddressReadTimeout(int timeout)     {this.peer_addr_read_timeout=timeout; return this;}
    public int       sendQueueSize()                         {return send_queue_size;}
    public TcpServer sendQueueSize(int send_queue_size)      {this.send_queue_size=send_queue_size; return this;}
    public boolean   useSendQueues()                         {return use_send_queues;}
    public TcpServer useSendQueues(boolean flag)             {this.use_send_queues=flag; return this;}



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
            try {
                socketFactory().close(srv_sock);
            }
            catch(IOException e) {
            }
            Util.interruptAndWaitToDie(acceptor);
            super.stop();
        }
    }

    protected TcpConnection createConnection(Address dest) throws Exception {
        return new TcpConnection(dest, this);
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
                Address peer_addr=conn.getPeerAddress();
                synchronized(this) {
                    boolean conn_exists=hasConnection(peer_addr),
                      replace=conn_exists && local_addr.compareTo(peer_addr) < 0; // bigger conn wins

                    if(!conn_exists || replace) {
                        replaceConnection(peer_addr, conn); // closes old conn
                        conn.start();
                        log.trace("%s: accepted connection from %s %s", local_addr, peer_addr, explanation(conn_exists, replace));
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


// $Id: ConnectionTable.java,v 1.62.2.5 2009/04/23 08:58:28 belaban Exp $

package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.util.PortsManager;
import org.jgroups.stack.IpAddress;

import java.io.IOException;
import java.net.*;


/**
 * Manages incoming and outgoing TCP connections. For each outgoing message to destination P, if there
 * is not yet a connection for P, one will be created. Subsequent outgoing messages will use this
 * connection.  For incoming messages, one server socket is created at startup. For each new incoming
 * client connecting, a new thread from a thread pool is allocated and listens for incoming messages
 * until the socket is closed by the peer.<br>Sockets/threads with no activity will be killed
 * after some time.
 * <p>
 * Incoming messages from any of the sockets can be received by setting the message listener.
 * @author Bela Ban
 */
public class ConnectionTable extends BasicConnectionTable implements Runnable {

   /**
    * Regular ConnectionTable without expiration of idle connections
    * @param srv_port The port on which the server will listen. If this port is reserved, the next
    *                 free port will be taken (incrementing srv_port).
    */
   public ConnectionTable(int srv_port) throws Exception {
       this.srv_port=srv_port;       
       init();
   }


    public ConnectionTable(InetAddress bind_addr, int srv_port) throws Exception {
        this.srv_port=srv_port;
        this.bind_addr=bind_addr;
        init();
    }


    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     * @param srv_port The port on which the server will listen
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable(int srv_port, long reaper_interval, long conn_expire_time) throws Exception {
        this.srv_port=srv_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        init();
    }


    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr,
                           int srv_port, int max_port) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.external_addr=external_addr;
        this.srv_port=srv_port;
        this.max_port=max_port;
        init();
    }


    /**
     * Create a ConnectionTable
     * @param r A reference to a receiver of all messages received by this class. Method <code>receive()</code>
     *          will be called.
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *	  	        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *                  Solaris or /dev/eth0 on Linux systems).
     * @param external_addr The address which will be broadcast to the group (the externally visible address
     *                   which this host should be contacted on). If external_addr is null, it will default to
     *                   the same address that the server socket is bound to.
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param max_port The largest port number that the server socket will be bound to. If max_port < srv_port
     *                 then there is no limit.
     */
    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr,
                           int srv_port, int max_port, PortsManager pm) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.external_addr=external_addr;
        this.srv_port=srv_port;
        this.max_port=max_port;
        this.pm=pm;
        init();
    }


    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port,
                           long reaper_interval, long conn_expire_time) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.external_addr=external_addr;
        this.srv_port=srv_port;
        this.max_port=max_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        init();
    }

    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     *
     * @param r The Receiver
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *		        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *		        Solaris or /dev/eth0 on Linux systems).
     * @param external_addr The address which will be broadcast to the group (the externally visible address
     *                   which this host should be contacted on). If external_addr is null, it will default to
     *                   the same address that the server socket is bound to.
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param max_port The largest port number that the server socket will be bound to. If max_port < srv_port
     *                 then there is no limit.
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port,
                           long reaper_interval, long conn_expire_time, PortsManager pm) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.external_addr=external_addr;
        this.srv_port=srv_port;
        this.max_port=max_port;
        this.pm=pm;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;   
        init();
    }



   /** Try to obtain correct Connection (or create one if not yet existent) */
   Connection getConnection(Address dest) throws Exception {
       Connection conn=null;
       Socket sock;

       synchronized(conns) {
           conn=conns.get(dest);
           if(conn == null) {
               // changed by bela Jan 18 2004: use the bind address for the client sockets as well
               SocketAddress tmpBindAddr=new InetSocketAddress(bind_addr, 0);
               InetAddress tmpDest=((IpAddress)dest).getIpAddress();
               SocketAddress destAddr=new InetSocketAddress(tmpDest, ((IpAddress)dest).getPort());
               sock=new Socket();
               sock.bind(tmpBindAddr);
               sock.setKeepAlive(true);
               sock.setTcpNoDelay(tcp_nodelay);
               if(linger > 0)
                   sock.setSoLinger(true, linger);
               else
                   sock.setSoLinger(false, -1);
               sock.connect(destAddr, sock_conn_timeout);

               try {
                   sock.setSendBufferSize(send_buf_size);
               }
               catch(IllegalArgumentException ex) {
                   if(log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                           send_buf_size + " bytes", ex);
               }
               try {
                   sock.setReceiveBufferSize(recv_buf_size);
               }
               catch(IllegalArgumentException ex) {
                   if(log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                           send_buf_size + " bytes", ex);
               }
               conn=new Connection(sock, dest);
               conn.sendLocalAddress(local_addr);
               notifyConnectionOpened(dest);
               addConnection(dest, conn);
               conn.init();
               if(log.isTraceEnabled()) log.trace("created socket to " + dest);
           }
           return conn;
       }
   }


    public final void start() throws Exception {                       
        acceptor=getThreadFactory().newThread(thread_group,this, "ConnectionTable.AcceptorThread");       
        acceptor.start();

        // start the connection reaper - will periodically remove unused connections
        if(use_reaper && reaper == null) {
            reaper=new Reaper();
            reaper.start();
        }
        super.start();                     
    }

    protected void init() throws Exception {

        srv_sock=createServerSocket(srv_port, max_port);

        if (external_addr!=null)
            local_addr=new IpAddress(external_addr, srv_sock.getLocalPort());
        else if (bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());

        if(log.isDebugEnabled()) log.debug("server socket listening on " + local_addr);
    }

   


   /**
    * Acceptor thread. Continuously accept new connections. Create a new thread for each new
    * connection and put it in conns. When the thread should stop, it is
    * interrupted by the thread creator.
    */
   public void run() {
       Socket     client_sock=null;
       Connection conn=null;
       Address    peer_addr;

       while(srv_sock != null) {
           try {
               conn=null;
               client_sock=srv_sock.accept();
               if(!running) {
                   if(log.isWarnEnabled())
                       log.warn("cannot accept connection from " + client_sock.getRemoteSocketAddress() + " as I'm closed");
                   break;
               }
               if(log.isTraceEnabled())
                   log.trace("[" +local_addr + "] accepted connection from " + client_sock.getInetAddress() + ":" + client_sock.getPort());
               try {
                   client_sock.setSendBufferSize(send_buf_size);
               }
               catch(IllegalArgumentException ex) {
                   if(log.isErrorEnabled()) log.error("exception setting send buffer size to " + send_buf_size + " bytes", ex);
               }
               try {
                   client_sock.setReceiveBufferSize(recv_buf_size);
               }
               catch(IllegalArgumentException ex) {
                   if(log.isErrorEnabled()) log.error("exception setting receive buffer size to " + send_buf_size + " bytes", ex);
               }

               client_sock.setKeepAlive(true);
               client_sock.setTcpNoDelay(tcp_nodelay);
               if(linger > 0)
                   client_sock.setSoLinger(true, linger);
               else
                   client_sock.setSoLinger(false, -1);

               // create new thread and add to conn table
               conn=new Connection(client_sock, null); // will call receive(msg)
               // get peer's address
               peer_addr=conn.readPeerAddress(client_sock);

               // client_addr=new IpAddress(client_sock.getInetAddress(), client_port);
               conn.setPeerAddress(peer_addr);

               synchronized(conns) {
                   Connection tmp=conns.get(peer_addr);
                   //Vladimir Nov, 5th, 2007
                   //we might have a connection to peer but is that 
                   //connection still open? 
                   boolean connectionOpen  = tmp != null && !tmp.isSocketClosed();
                   if(connectionOpen) {                       
                       if(peer_addr.compareTo(local_addr) > 0) {
                           if(log.isTraceEnabled())
                               log.trace("peer's address (" + peer_addr + ") is greater than our local address (" +
                                       local_addr + "), replacing our existing connection");
                           // peer's address is greater, add peer's connection to ConnectionTable, destroy existing connection
                           removeConnection(peer_addr);
                           addConnection(peer_addr,  conn);                           
                           notifyConnectionOpened(peer_addr);
                       }
                       else {
                           if(log.isTraceEnabled())
                               log.trace("peer's address (" + peer_addr + ") is smaller than our local address (" +
                                       local_addr + "), rejecting peer connection request");
                           conn.destroy();
                           continue;
                       }                      
                   }
                   else {                       
                       addConnection(peer_addr, conn);
                       notifyConnectionOpened(peer_addr);
                   }
               }

               conn.init(); // starts handler thread on this socket
           }
           catch(SocketTimeoutException timeout_ex) {
               if(log.isWarnEnabled()) log.warn("timed out waiting for peer address, closing connection " + conn + ": " + timeout_ex);
               if(conn != null)
                   conn.destroy();
               if(srv_sock == null)
                   break;  // socket was closed, therefore stop
           }
           catch(SocketException sock_ex) {
               if(log.isWarnEnabled() && srv_sock != null) log.warn("Could not read accept connection from peer " + sock_ex);
               if(conn != null)
                   conn.destroy();
               if(srv_sock == null)
                   break;  // socket was closed, therefore stop
           }
           catch(Throwable ex) {
               if(log.isWarnEnabled()) log.warn("Could not read accept connection from peer " + ex);
               if(srv_sock == null)
                   break;  // socket was closed, therefore stop
           }
       }
       if(client_sock != null)
           try {client_sock.close();} catch(IOException e) {}
       if(log.isTraceEnabled())
           log.trace(Thread.currentThread().getName() + " terminated");
   }


   /** Finds first available port starting at start_port and returns server socket.
     * Will not bind to port >end_port. Sets srv_port */
   protected ServerSocket createServerSocket(int start_port, int end_port) throws Exception {
       ServerSocket ret=null;

       while(true) {
           try {
               if(start_port > 0 && pm != null)
                   start_port=pm.getNextAvailablePort(start_port);
               if(bind_addr == null)
                   ret=new ServerSocket(start_port);
               else {
                   // changed (bela Sept 7 2007): we accept connections on all NICs
                   ret=new ServerSocket(start_port, backlog, bind_addr);
               }
           }
           catch(BindException bind_ex) {
               if (start_port==end_port) throw new BindException("No available port to bind to");
               if(bind_addr != null && !bind_addr.isLoopbackAddress()) {
                   NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                   if(nic == null)
                       throw new BindException("bind_addr " + bind_addr + " is not a valid interface");
               }
               start_port++;
               continue;
           }
           catch(IOException io_ex) {
               if(log.isErrorEnabled()) log.error("exception is " + io_ex);
           }
           srv_port=start_port;
           break;
       }
       if(ret == null)
    	   throw new IOException("Could not create server socket in port range " + start_port + " - " +end_port);
       return ret;
   }




}


// $Id: ConnectionTable1_4.java,v 1.3 2003/12/21 03:54:56 akbollu Exp $

package org.jgroups.blocks;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.Util1_4;


/**
 * Manages incoming and outgoing TCP connections. For each outgoing message to destination P, if there
 * is not yet a connection for P, one will be created. Subsequent outgoing messages will use this
 * connection.  For incoming messages, one server socket is created at startup. For each new incoming
 * client connecting, a new thread from a thread pool is allocated and listens for incoming messages
 * until the socket is closed by the peer.<br>Sockets/threads with no activity will be killed
 * after some time.<br> Incoming messages from any of the sockets can be received by setting the
 * message listener.
 * @author Bela Ban
 */
public class ConnectionTable1_4 implements Runnable {
    Hashtable     conns=new Hashtable();       // keys: Addresses (peer address), values: Connection
    Receiver      receiver=null;
    ServerSocketChannel srv_sock_ch=null;
    ServerSocket  srv_sock=null;
    InetAddress   bind_addr=null;
    Address       local_addr=null;             // bind_addr + port of srv_sock
    int           srv_port=7800;
    Thread        acceptor=null;               // continuously calls srv_sock.accept()
    final int     backlog=20;                  // 20 conn requests are queued by ServerSocket (addtl will be discarded)
    Vector        conn_listeners=new Vector(); // listeners to be notified when a conn is established/torn down
    Object        recv_mutex=new Object();     // to serialize simultaneous access to receive() from multiple Connections
    Reaper        reaper=null;                 // closes conns that have been idle for more than n secs
    long          reaper_interval=60000;       // reap unused conns once a minute
    long          conn_expire_time=300000;     // connections can be idle for 5 minutes before they are reaped
    boolean       use_reaper=false;            // by default we don't reap idle conns
    ThreadGroup   thread_group=null;
    final byte[]  cookie={'b', 'e', 'l', 'a'};
	private Selector selector = null;
	private ArrayList pendingSocksList=null;
    /** Used for message reception */
    public interface Receiver {
        void receive(Message msg);
    }


    /** Used to be notified about connection establishment and teardown */
    public interface ConnectionListener {
        void connectionOpened(Address peer_addr);
        void connectionClosed(Address peer_addr);
    }


    private ConnectionTable1_4() { // cannot be used, other ctor has to be used

    }

    /**
     * Regular ConnectionTable1_4 without expiration of idle connections
     * @param srv_port The port on which the server will listen. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     */
    public ConnectionTable1_4(int srv_port) throws Exception {
        this.srv_port=srv_port;
        start();
    }


    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     * @param srv_port The port on which the server will listen
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable1_4(int srv_port, long reaper_interval, long conn_expire_time) throws Exception {
        this.srv_port=srv_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        start();
    }


    /**
     * Create a ConnectionTable
     * @param r A reference to a receiver of all messages received by this class. Method <code>receive()</code>
     *          will be called.
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *	  	        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *                  Solaris or /dev/eth0 on Linux systems).
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     */
    public ConnectionTable1_4(Receiver r, InetAddress bind_addr, int srv_port) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.srv_port=srv_port;
        start();
    }


    /**
     * ConnectionTable including a connection reaper. Connections that have been idle for more than conn_expire_time
     * milliseconds will be closed and removed from the connection table. On next access they will be re-created.
     *
     * @param srv_port The port on which the server will listen.If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param bind_addr The host name or IP address of the interface to which the server socket will bind.
     *                  This is interesting only in multi-homed systems. If bind_addr is null, the
     *		        server socket will bind to the first available interface (e.g. /dev/hme0 on
     *		        Solaris or /dev/eth0 on Linux systems).
     * @param srv_port The port to which the server socket will bind to. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     * @param reaper_interval Number of milliseconds to wait for reaper between attepts to reap idle connections
     * @param conn_expire_time Number of milliseconds a connection can be idle (no traffic sent or received until
     *                         it will be reaped
     */
    public ConnectionTable1_4(Receiver r, InetAddress bind_addr, int srv_port,
                           long reaper_interval, long conn_expire_time) throws Exception {
        setReceiver(r);
        this.bind_addr=bind_addr;
        this.srv_port=srv_port;
        this.reaper_interval=reaper_interval;
        this.conn_expire_time=conn_expire_time;
        use_reaper=true;
        start();
    }


    public void setReceiver(Receiver r) {
        receiver=r;
    }


    public void addConnectionListener(ConnectionListener l) {
        if(l != null && !conn_listeners.contains(l))
            conn_listeners.addElement(l);
    }


    public void removeConnectionListener(ConnectionListener l) {
        if(l != null) conn_listeners.removeElement(l);
    }


    public Address getLocalAddress() {
        if(local_addr == null)
            local_addr=bind_addr != null ? new IpAddress(bind_addr, srv_port) : null;
        return local_addr;
    }


    /** Sends a message to a unicast destination. The destination has to be set */
    public void send(Message msg) {
        Address dest=msg != null ? msg.getDest() : null;
        Connection conn;

        if(dest == null) {
            Trace.error("ConnectionTable1_4.send()", "msg is null or message's destination is null");
            return;
        }

        // 1. Try to obtain correct Connection (or create one if not yet existent)
        try {
            conn=getConnection(dest);
            if(conn == null) return;
        }
        catch(Throwable ex) {
            Trace.info("ConnectionTable1_4.send()", "connection to " + dest + " could not be established: " + ex);
            return;
        }

        // 2. Send the message using that connection
        try {
            conn.send(msg);
        }
        catch(Throwable ex) {
            if(Trace.trace)
                Trace.info("ConnectionTable1_4.send()", "sending message to " + dest + " failed (ex=" +
                                                     ex.getClass().getName() + "); removing from connection table");
            remove(dest);
        }
    }


    /** Try to obtain correct Connection (or create one if not yet existent) */
    Connection getConnection(Address dest) throws Exception {
        Connection conn=null;
        SocketChannel sock_ch;

        synchronized(conns) {
            conn=(Connection)conns.get(dest);
            if(conn == null) {
				InetSocketAddress destAddress =
					new InetSocketAddress(
						((IpAddress) dest).getIpAddress(),
						((IpAddress) dest).getPort());
				sock_ch = SocketChannel.open(destAddress);
                conn=new Connection(sock_ch, dest);
                conn.sendLocalAddress(local_addr);
                // conns.put(dest, conn);
                addConnection(dest, conn);
                pendingSocksList.add(conn);
                selector.wakeup();
                notifyConnectionOpened(dest);
                if(Trace.trace) Trace.info("ConnectionTable1_4.getConnection()", "created socket to " + dest);
            }
            return conn;
        }
    }


    public void start() throws Exception {
		this.selector = Selector.open();
        srv_sock=createServerSocket(srv_port);

        if(bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());

        if(Trace.trace) {
            Trace.info("ConnectionTable1_4.start()", "server socket created " +
                                                  "on " + local_addr);
        }

        //Roland Kurmann 4/7/2003, build new thread group
        thread_group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "ConnectionTableGroup");
        //Roland Kurmann 4/7/2003, put in thread_group
        acceptor=new Thread(thread_group, this, "ConnectionTable1_4.AcceptorThread");
        acceptor.setDaemon(true);
        acceptor.start();

        // start the connection reaper - will periodically remove unused connections
        if(use_reaper && reaper == null) {
            reaper=new Reaper();
            reaper.start();
        }
        pendingSocksList = new ArrayList();
    }


    /** Closes all open sockets, the server socket and all threads waiting for incoming messages */
    public void stop() {
        Iterator it=null;
        Connection conn;
        ServerSocket tmp;

        // 1. close the server socket (this also stops the acceptor thread)
        if(srv_sock != null) {
            try {
                tmp=srv_sock;
                srv_sock=null;
                tmp.close();
            }
            catch(Exception e) {            	
            }
        }


        // 2. then close the connections
        synchronized(conns) {
            it=conns.values().iterator();
            while(it.hasNext()) {
                conn=(Connection)it.next();
                conn.destroy();
            }
            conns.clear();
        }
        local_addr=null;
    }


    /**
     Remove <code>addr</code>from connection table. This is typically triggered when a member is suspected.
     */
    public void remove(Address addr) {
        Connection conn;

        synchronized(conns) {
            conn=(Connection)conns.get(addr);

            if(conn != null) {
                try {
                    conn.destroy();  // won't do anything if already destroyed
                }
                catch(Exception e) {                	
                }
                conns.remove(addr);
            }
            if(Trace.trace)
                Trace.info("ConnectionTable1_4.remove()", "addr=" + addr + ", connections are " + toString());
        }
    }


    /**
	 * Acceptor thread. Continuously accept new connections. Create a new
	 * thread for each new connection and put it in conns. When the thread
	 * should stop, it is interrupted by the thread creator.
	 */
	public void run()
	{
		Socket client_sock;
		Connection conn = null;
		Address peer_addr;

		while (srv_sock_ch != null)
		{
			try
			{
				if (selector.select() > 0)
				{
					Set readyKeys = selector.selectedKeys();
					for (Iterator i = readyKeys.iterator(); i.hasNext();)
					{
						SelectionKey key = (SelectionKey) i.next();
						i.remove();
						if ((key.readyOps() & SelectionKey.OP_ACCEPT)
							== SelectionKey.OP_ACCEPT)
						{
							ServerSocketChannel readyChannel =
								(ServerSocketChannel) key.channel();

							SocketChannel client_sock_ch=
								readyChannel.accept();
							client_sock = client_sock_ch.socket();
							if (Trace.trace)
								Trace.info(
									"ConnectionTable1_4.run()",
									"accepted connection, client_sock="
										+ client_sock);

							conn = new Connection(client_sock_ch, null);
							// will call receive(msg)
							// get peer's address
							peer_addr = conn.readPeerAddress(client_sock);

							conn.setPeerAddress(peer_addr);

							synchronized (conns)
							{
								if (conns.contains(peer_addr))
								{
									if (Trace.trace)
										Trace.warn(
											"ConnectionTable1_4.run()",
											peer_addr
												+ " is already there, will terminate connection");
									conn.destroy();
									return;
								}
								addConnection(peer_addr, conn);
							}
							conn.init();
							notifyConnectionOpened(peer_addr);
						}
						else if (
							(key.readyOps() & SelectionKey.OP_READ)
								== SelectionKey.OP_READ)
						{
							SocketChannel incomingChannel =
							(SocketChannel) key.channel();
							conn = (Connection) key.attachment();
							ByteBuffer buff = conn.getNIOMsgReader().readCompleteMsgBuffer();
							if(buff != null)
							{
								receive((Message)Util.objectFromByteBuffer(buff.array()));
								conn.getNIOMsgReader().reset();
							}
						}
					}
				}
				else
				{
					/*In addition to the accepted Sockets, we must registe 
					 * sockets opend by this for OP_READ, because peer may
					 * use the same socket to sends data using the same socket,
					 * instead of opening a new connection. We can not register 
					 * with this selectior from a different thread. set pending
					 * and wakeup this selector.
					 */
					synchronized (conns)
					{
						Connection pendingConnection;
						while( (pendingSocksList.size()>0) && (null != (pendingConnection = (Connection)pendingSocksList.remove(0))) )
						{
							pendingConnection.init();
						}
					}	
				}
			}
			catch (SocketException sock_ex)
			{
				if (Trace.trace)
					Trace.info(
						"ConnectionTable1_4.run()",
						"exception is " + sock_ex);
				if (conn != null)
					conn.destroy();
				if (srv_sock == null)
					break; // socket was closed, therefore stop
			}
			catch (Throwable ex)
			{
				if (Trace.trace)
					Trace.warn(
						"ConnectionTable1_4.run()",
						"exception is " + ex);
			}
		}
	}


    /**
     * Calls the receiver callback. We serialize access to this method because it may be called concurrently
     * by several Connection handler threads. Therefore the receiver doesn't need to synchronize.
     */
    public void receive(Message msg) {
        if(receiver != null) {
            synchronized(recv_mutex) {
                receiver.receive(msg);
            }
        }
        else
            Trace.error("ConnectionTable1_4.receive()", "receiver is null (not set) !");
    }


    public String toString() {
        StringBuffer ret=new StringBuffer();
        Address key;
        Connection val;

        synchronized(conns) {
            ret.append("connections (" + conns.size() + "):\n");
            for(Enumeration e=conns.keys(); e.hasMoreElements();) {
                key=(Address)e.nextElement();
                val=(Connection)conns.get(key);
                ret.append("key: " + key.toString() + ": " + val.toString() + "\n");
            }
        }
        ret.append("\n");
        return ret.toString();
    }


    /** Finds first available port starting at start_port and returns server socket. Sets srv_port */
    ServerSocket createServerSocket(int start_port) throws Exception {
        srv_sock_ch = ServerSocketChannel.open();
		srv_sock_ch.configureBlocking(false);
        while(true) {
            try {
                if(bind_addr == null)
				srv_sock_ch.socket().bind(new InetSocketAddress(start_port));
                else
				srv_sock_ch.socket().bind(new InetSocketAddress( bind_addr,start_port), backlog);
            }
            catch(BindException bind_ex) {
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                Trace.error("ConnectionTable1_4.createServerSocket()", "exception is " + io_ex);
            }
            srv_port=start_port;
            break;
        }
		srv_sock_ch.register(this.selector, SelectionKey.OP_ACCEPT);
		return srv_sock_ch.socket();
    }


    void notifyConnectionOpened(Address peer) {
        if(peer == null) return;
        for(int i=0; i < conn_listeners.size(); i++)
            ((ConnectionListener)conn_listeners.elementAt(i)).connectionOpened(peer);
    }

    void notifyConnectionClosed(Address peer) {
        if(peer == null) return;
        for(int i=0; i < conn_listeners.size(); i++)
            ((ConnectionListener)conn_listeners.elementAt(i)).connectionClosed(peer);
    }


    void addConnection(Address peer, Connection c) {
        conns.put(peer, c);
        if(reaper != null && !reaper.isRunning())
            reaper.start();
    }
    

    class Connection {
        Socket           sock=null;                // socket to/from peer (result of srv_sock.accept() or new Socket())
        SocketChannel sock_ch = null;
        DataOutputStream out=null;                 // for sending messages
        DataInputStream  in=null;                  // for receiving messages
        Address          peer_addr=null;           // address of the 'other end' of the connection
        Object           send_mutex=new Object();  // serialize sends
        long             last_access=System.currentTimeMillis(); // last time a message was sent or received
        private static final int HEADER_SIZE = 4;
        private static final int DEFAULT_BUFF_SIZE = 256;
        ByteBuffer		 headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
        NBMessageForm1_4 	 nioMsgReader = null;
        
        Connection(SocketChannel s, Address peer_addr) {
			sock_ch=s;
			sock = sock_ch.socket();
            this.peer_addr=peer_addr;
            try {
                out=new DataOutputStream(sock.getOutputStream());
                in=new DataInputStream(sock.getInputStream());
            }
            catch(Exception ex) {
                Trace.error("ConnectionTable1_4.Connection()", "exception is " + ex);
            }
        }

        void setPeerAddress(Address peer_addr) {
            this.peer_addr=peer_addr;
        }

        void updateLastAccessed() {
            //if(Trace.trace)
            ///Trace.info("ConnectionTable1_4.Connection.updateLastAccessed()", "connections are " + conns);
            last_access=System.currentTimeMillis();
        }

        void init() {
        	try
        	{
        		in = null;
        		out = null;
        	}
        	catch(Exception ex)
        	{
        	}
        	
        	try
			{
				sock_ch.configureBlocking(false);
				nioMsgReader = new NBMessageForm1_4(DEFAULT_BUFF_SIZE,sock_ch);
				sock_ch.register(selector, SelectionKey.OP_READ, this);
			}
			catch (IOException e)
			{
			}
            if(Trace.trace)
                Trace.info("ConnectionTable1_4.Connection.init()", "connection was created to " + peer_addr);

        }


        void destroy() {
            closeSocket();
            nioMsgReader = null;
        }

        void send(Message msg) {
            synchronized(send_mutex) {
                try {
                    doSend(msg);
                    updateLastAccessed();
                }
                catch(IOException io_ex) {
                    if(Trace.trace)
                        Trace.warn("ConnectionTable1_4.Connection.send()", "peer closed connection, " +
                                                                        "trying to re-establish connection and re-send msg.");
                    try {
                        doSend(msg);
                        updateLastAccessed();
                    }
                    catch(IOException io_ex2) {
                        if(Trace.trace) Trace.error("ConnectionTable1_4.Connection.send()", "2nd attempt to send data failed too");
                    }
                    catch(Exception ex2) {
                        if(Trace.trace) Trace.error("ConnectionTable1_4.Connection.send()", "exception is " + ex2);
                    }
                }
                catch(Exception ex) {
                    if(Trace.trace) Trace.error("ConnectionTable1_4.Connection.send()", "exception is " + ex);                  
                }
            }
        }


        void doSend(Message msg) throws Exception {
            IpAddress dst_addr=(IpAddress)msg.getDest();
            byte[]    buffie=null;

            if(dst_addr == null || dst_addr.getIpAddress() == null) {
                Trace.error("ConnectionTable1_4.Connection.doSend()", "the destination address is null; aborting send");
                return;
            }

            try {
                // set the source address if not yet set
                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                buffie=Util.objectToByteBuffer(msg);
                if(buffie.length <= 0) {
                    Trace.error("ConnectionTable1_4.Connection.doSend()", "buffer.length is 0. Will not send message");
                    return;
                }


                // we're using 'double-writes', sending the buffer to the destination twice. this would
                // ensure that, if the peer closed the connection while we were idle, we would get an exception.
                // this won't happen if we use a single write (see Stevens, ch. 5.13).
				headerBuffer.clear();
				headerBuffer.putInt(buffie.length);
				headerBuffer.flip();
				Util1_4.writeFully(headerBuffer,sock_ch);
				ByteBuffer sendBuffer = ByteBuffer.wrap(buffie);
				Util1_4.writeFully(sendBuffer, sock_ch);
            }
            catch(Exception ex) {
                if(Trace.trace)
                    Trace.error("ConnectionTable1_4.Connection.doSend()",
                                "to " + dst_addr + ", exception is " + ex + ", stack trace:\n" +
                                Util.printStackTrace(ex));                
                remove(dst_addr);
                throw ex;
            }
        }


        /**
         * Reads the peer's address. First a cookie has to be sent which has to match my own cookie, otherwise
         * the connection will be refused
         */
        Address readPeerAddress(Socket client_sock) throws Exception {
            Address     peer_addr=null;
            byte[]      version, buf, input_cookie=new byte[cookie.length];
            int         len=0, client_port=client_sock != null? client_sock.getPort() : 0;
            InetAddress client_addr=client_sock != null? client_sock.getInetAddress() : null;

            if(in != null) {
                initCookie(input_cookie);

                // read the cookie first
                in.read(input_cookie, 0, input_cookie.length);
                if(!matchCookie(input_cookie))
                    throw new SocketException("ConnectionTable1_4.Connection.readPeerAddress(): cookie sent by " +
                                              peer_addr + " does not match own cookie; terminating connection");
                // then read the version
                version=new byte[Version.version_id.length];
                in.read(version, 0, version.length);

                if(Version.compareTo(version) == false) {
                    Trace.warn("ConnectionTable1_4.readPeerAddress()",
                               "packet from " + client_addr + ":" + client_port +
                               " has different version (" +
                               Version.printVersionId(version, Version.version_id.length) +
                               ") from ours (" + Version.printVersionId(Version.version_id) +
                               "). This may cause problems");
                }

                // read the length of the address
                len=in.readInt();

                // finally read the address itself
                buf=new byte[len];
                in.readFully(buf, 0, len);
                peer_addr=(Address)Util.objectFromByteBuffer(buf);                
                updateLastAccessed();
            }
            return peer_addr;
        }


        /**
         * Send the cookie first, then the our port number. If the cookie doesn't match the receiver's cookie,
         * the receiver will reject the connection and close it.
         */
        void sendLocalAddress(Address local_addr) {
            byte[] buf;

            if(local_addr == null) {
                Trace.warn("ConnectionTable1_4.Connection.sendLocalAddress()", "local_addr is null");
                return;
            }
            if(out != null) {
                try {
                    buf=Util.objectToByteBuffer(local_addr);

                    // write the cookie
                    out.write(cookie, 0, cookie.length);

                    // write the version
                    out.write(Version.version_id, 0, Version.version_id.length);

                    // write the length of the buffer
                    out.writeInt(buf.length);

                    // and finally write the buffer itself
                    out.write(buf, 0, buf.length);
                    out.flush(); // needed ?
                    updateLastAccessed();
                }
                catch(Throwable t) {
                    Trace.error("ConnectionTable1_4.Connection.sendLocalAddress()", "exception is " + t);
                }
            }
        }


        void initCookie(byte[] c) {
            if(c != null)
                for(int i=0; i < c.length; i++)
                    c[i]=0;
        }

        boolean matchCookie(byte[] input) {
            if(input == null || input.length < cookie.length) return false;
            if(Trace.trace)
                Trace.info("ConnectionTable1_4.Connection.matchCookie()", "input_cookie is " + printCookie(input));
            for(int i=0; i < cookie.length; i++)
                if(cookie[i] != input[i]) return false;
            return true;
        }


        String printCookie(byte[] c) {
            if(c == null) return "";
            return new String(c);
        }

        public String toString() {
            StringBuffer ret=new StringBuffer();
            InetAddress local=null, remote=null;
            String local_str, remote_str;

            if(sock == null)
                ret.append("<null socket>");
            else {
                //since the sock variable gets set to null we want to make
                //make sure we make it through here without a nullpointer exception
                Socket tmp_sock=sock;
                local=tmp_sock.getLocalAddress();
                remote=tmp_sock.getInetAddress();
                local_str=local != null ? Util.shortName(local.getHostName()) : "<null>";
                remote_str=remote != null ? Util.shortName(local.getHostName()) : "<null>";
                ret.append("<" + local_str + ":" + tmp_sock.getLocalPort() +
                           " --> " + remote_str + ":" + tmp_sock.getPort() + "> (" +
                           ((System.currentTimeMillis() - last_access) / 1000) + " secs old)");
                tmp_sock=null;
            }

            return ret.toString();
        }


        void closeSocket() {
            if(sock != null) {
                try {
                    sock_ch.close();
                }
                catch(Exception e) {                	
                }
                sock=null;
            }
        }
        
        NBMessageForm1_4 getNIOMsgReader()
        {
        	return nioMsgReader;
        }
    }


    class Reaper implements Runnable {
        Thread t=null;

        Reaper() {
            ;
        }

        public void start() {
            if(conns.size() == 0)
                return;
            if(t != null && !t.isAlive())
                t=null;
            if(t == null) {
                //RKU 7.4.2003, put in threadgroup
                t=new Thread(thread_group, this, "ConnectionTable1_4.ReaperThread");
                t.setDaemon(true); // will allow us to terminate if all remaining threads are daemons
                t.start();
            }
        }

        public void stop() {
            if(t != null)
                t=null;
        }


        public boolean isRunning() {
            return t != null;
        }

        public void run() {
            Connection value;
            Map.Entry entry;
            long curr_time;

            if(Trace.trace)
                Trace.info("ConnectionTable1_4.Reaper.run()", "connection reaper thread was started. Number of connections=" +
                                                           conns.size() + ", reaper_interval=" + reaper_interval + ", conn_expire_time=" +
                                                           conn_expire_time);

            while(conns.size() > 0 && t != null) {
                // first sleep
                Util.sleep(reaper_interval);
                synchronized(conns) {
                    curr_time=System.currentTimeMillis();
                    for(Iterator it=conns.entrySet().iterator(); it.hasNext();) {
                        entry=(Map.Entry)it.next();
                        value=(Connection)entry.getValue();
                        if(Trace.trace)
                            Trace.info("ConnectionTable1_4.Reaper.run()", "connection is " +
                                                                       ((curr_time - value.last_access) / 1000) + " seconds old (curr-time=" +
                                                                       curr_time + ", last_access=" + value.last_access + ")");
                        if(value.last_access + conn_expire_time < curr_time) {
                            if(Trace.trace)
                                Trace.info("ConnectionTable1_4.Reaper.run()", "connection " + value +
                                                                           " has been idle for too long (conn_expire_time=" + conn_expire_time +
                                                                           "), will be removed");

                            value.destroy();
                            it.remove();
                        }
                    }
                }
            }
            if(Trace.trace)
                Trace.info("ConnectionTable1_4.Reaper.run()", "reaper terminated");
            t=null;
        }
    }    
}
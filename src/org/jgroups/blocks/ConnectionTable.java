// $Id: ConnectionTable.java,v 1.2 2004/01/18 21:24:40 belaban Exp $

package org.jgroups.blocks;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.*;
import java.util.*;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Version;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;


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
public class ConnectionTable implements Runnable {
    Hashtable     conns=new Hashtable();       // keys: Addresses (peer address), values: Connection
    Receiver      receiver=null;
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


    /** Used for message reception */
    public interface Receiver {
        void receive(Message msg);
    }


    /** Used to be notified about connection establishment and teardown */
    public interface ConnectionListener {
        void connectionOpened(Address peer_addr);
        void connectionClosed(Address peer_addr);
    }


    /**
     * Regular ConnectionTable without expiration of idle connections
     * @param srv_port The port on which the server will listen. If this port is reserved, the next
     *                 free port will be taken (incrementing srv_port).
     */
    public ConnectionTable(int srv_port) throws Exception {
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
    public ConnectionTable(int srv_port, long reaper_interval, long conn_expire_time) throws Exception {
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
    public ConnectionTable(Receiver r, InetAddress bind_addr, int srv_port) throws Exception {
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
    public ConnectionTable(Receiver r, InetAddress bind_addr, int srv_port,
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
            Trace.error("ConnectionTable.send()", "msg is null or message's destination is null");
            return;
        }

        // 1. Try to obtain correct Connection (or create one if not yet existent)
        try {
            conn=getConnection(dest);
            if(conn == null) return;
        }
        catch(Throwable ex) {
            Trace.info("ConnectionTable.send()", "connection to " + dest + " could not be established: " + ex);
            return;
        }

        // 2. Send the message using that connection
        try {
            conn.send(msg);
        }
        catch(Throwable ex) {
            if(Trace.trace)
                Trace.info("ConnectionTable.send()", "sending message to " + dest + " failed (ex=" +
                                                     ex.getClass().getName() + "); removing from connection table");
            remove(dest);
        }
    }


    /** Try to obtain correct Connection (or create one if not yet existent) */
    Connection getConnection(Address dest) throws Exception {
        Connection conn=null;
        Socket sock;

        synchronized(conns) {
            conn=(Connection)conns.get(dest);
            if(conn == null) {
                // changed by bela Jan 18 2004: use the bind address for the client sockets as well
                sock=new Socket(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort(), bind_addr, 0);
                conn=new Connection(sock, dest);
                conn.sendLocalAddress(local_addr);
                notifyConnectionOpened(dest);
                // conns.put(dest, conn);
                addConnection(dest, conn);
                conn.init();
                if(Trace.trace) Trace.info("ConnectionTable.getConnection()", "created socket to " + dest);
            }
            return conn;
        }
    }


    public void start() throws Exception {
        srv_sock=createServerSocket(srv_port);

        if(bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());

        if(Trace.trace) {
            Trace.info("ConnectionTable.start()", "server socket created " +
                                                  "on " + local_addr);
        }

        //Roland Kurmann 4/7/2003, build new thread group
        thread_group = new ThreadGroup(Thread.currentThread().getThreadGroup(), "ConnectionTableGroup");
        //Roland Kurmann 4/7/2003, put in thread_group
        acceptor=new Thread(thread_group, this, "ConnectionTable.AcceptorThread");
        acceptor.setDaemon(true);
        acceptor.start();

        // start the connection reaper - will periodically remove unused connections
        if(use_reaper && reaper == null) {
            reaper=new Reaper();
            reaper.start();
        }
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
                Trace.info("ConnectionTable.remove()", "addr=" + addr + ", connections are " + toString());
        }
    }


    /**
     * Acceptor thread. Continuously accept new connections. Create a new thread for each new
     * connection and put it in conns. When the thread should stop, it is
     * interrupted by the thread creator.
     */
    public void run() {
        Socket     client_sock;
        Connection conn=null;
        Address    peer_addr;

        while(srv_sock != null) {
            try {
                client_sock=srv_sock.accept();
                if(Trace.trace)
                    Trace.info("ConnectionTable.run()", "accepted connection, client_sock=" + client_sock);

                // create new thread and add to conn table
                conn=new Connection(client_sock, null); // will call receive(msg)
                // get peer's address
                peer_addr=conn.readPeerAddress(client_sock);

                // client_addr=new IpAddress(client_sock.getInetAddress(), client_port);
                conn.setPeerAddress(peer_addr);

                synchronized(conns) {
                    if(conns.contains(peer_addr)) {
                        if(Trace.trace)
                            Trace.warn("ConnectionTable.run()", peer_addr +
                                                                " is already there, will terminate connection");
                        conn.destroy();
                        return;
                    }
                    // conns.put(peer_addr, conn);
                    addConnection(peer_addr, conn);
                }
                notifyConnectionOpened(peer_addr);
                conn.init(); // starts handler thread on this socket
            }
            catch(SocketException sock_ex) {
                if(Trace.trace) Trace.info("ConnectionTable.run()", "exception is " + sock_ex);
                if(conn != null)
                    conn.destroy();
                if(srv_sock == null)
                    break;  // socket was closed, therefore stop
            }
            catch(Throwable ex) {
                if(Trace.trace) Trace.warn("ConnectionTable.run()", "exception is " + ex);
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
            Trace.error("ConnectionTable.receive()", "receiver is null (not set) !");
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
        ServerSocket ret=null;

        while(true) {
            try {
                if(bind_addr == null)
                    ret=new ServerSocket(start_port);
                else
                    ret=new ServerSocket(start_port, backlog, bind_addr);
            }
            catch(BindException bind_ex) {
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                Trace.error("ConnectionTable.createServerSocket()", "exception is " + io_ex);
            }
            srv_port=start_port;
            break;
        }
        return ret;
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


    class Connection implements Runnable {
        Socket           sock=null;                // socket to/from peer (result of srv_sock.accept() or new Socket())
        DataOutputStream out=null;                 // for sending messages
        DataInputStream  in=null;                  // for receiving messages
        Thread           handler=null;             // thread for receiving messages
        Address          peer_addr=null;           // address of the 'other end' of the connection
        Object           send_mutex=new Object();  // serialize sends
        long             last_access=System.currentTimeMillis(); // last time a message was sent or received
        // final byte[]     cookie={(byte)'b', (byte)'e', (byte)'l', (byte)'a'};


        Connection(Socket s, Address peer_addr) {
            sock=s;
            this.peer_addr=peer_addr;
            try {
                out=new DataOutputStream(sock.getOutputStream());
                in=new DataInputStream(sock.getInputStream());
            }
            catch(Exception ex) {
                Trace.error("ConnectionTable.Connection()", "exception is " + ex);
            }
        }


        boolean established() {
            return handler != null;
        }


        void setPeerAddress(Address peer_addr) {
            this.peer_addr=peer_addr;
        }

        void updateLastAccessed() {
            //if(Trace.trace)
            ///Trace.info("ConnectionTable.Connection.updateLastAccessed()", "connections are " + conns);
            last_access=System.currentTimeMillis();
        }

        void init() {
            if(Trace.trace)
                Trace.info("ConnectionTable.Connection.init()", "connection was created to " + peer_addr);
            if(handler == null) {
                // Roland Kurmann 4/7/2003, put in thread_group
                handler=new Thread(thread_group, this, "ConnectionTable.Connection.HandlerThread");
                handler.setDaemon(true);
                handler.start();
            }
        }


        void destroy() {
            closeSocket(); // should terminate handler as well
            handler=null;
        }


        void send(Message msg) {
            synchronized(send_mutex) {
                try {
                    doSend(msg);
                    updateLastAccessed();
                }
                catch(IOException io_ex) {
                    if(Trace.trace)
                        Trace.warn("ConnectionTable.Connection.send()", "peer closed connection, " +
                                                                        "trying to re-establish connection and re-send msg.");
                    try {
                        doSend(msg);
                        updateLastAccessed();
                    }
                    catch(IOException io_ex2) {
                        if(Trace.trace) Trace.error("ConnectionTable.Connection.send()", "2nd attempt to send data failed too");
                    }
                    catch(Exception ex2) {
                        if(Trace.trace) Trace.error("ConnectionTable.Connection.send()", "exception is " + ex2);
                    }
                }
                catch(Exception ex) {
                    if(Trace.trace) Trace.error("ConnectionTable.Connection.send()", "exception is " + ex);
                }
            }
        }


        void doSend(Message msg) throws Exception {
            IpAddress dst_addr=(IpAddress)msg.getDest();
            byte[]    buffie=null;

            if(dst_addr == null || dst_addr.getIpAddress() == null) {
                Trace.error("ConnectionTable.Connection.doSend()", "the destination address is null; aborting send");
                return;
            }

            try {
                // set the source address if not yet set
                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                buffie=Util.objectToByteBuffer(msg);
                if(buffie.length <= 0) {
                    Trace.error("ConnectionTable.Connection.doSend()", "buffer.length is 0. Will not send message");
                    return;
                }


                // we're using 'double-writes', sending the buffer to the destination twice. this would
                // ensure that, if the peer closed the connection while we were idle, we would get an exception.
                // this won't happen if we use a single write (see Stevens, ch. 5.13).
                if(out != null) {
                    out.writeInt(buffie.length); // write the length of the data buffer first
                    Util.doubleWrite(buffie, out);
                    out.flush();  // may not be very efficient (but safe)
                }
            }
            catch(Exception ex) {
                if(Trace.trace)
                    Trace.error("ConnectionTable.Connection.doSend()",
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
                    throw new SocketException("ConnectionTable.Connection.readPeerAddress(): cookie sent by " +
                                              peer_addr + " does not match own cookie; terminating connection");
                // then read the version
                version=new byte[Version.version_id.length];
                in.read(version, 0, version.length);

                if(Version.compareTo(version) == false) {
                    Trace.warn("ConnectionTable.readPeerAddress()",
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
                Trace.warn("ConnectionTable.Connection.sendLocalAddress()", "local_addr is null");
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
                    Trace.error("ConnectionTable.Connection.sendLocalAddress()", "exception is " + t);
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
                Trace.info("ConnectionTable.Connection.matchCookie()", "input_cookie is " + printCookie(input));
            for(int i=0; i < cookie.length; i++)
                if(cookie[i] != input[i]) return false;
            return true;
        }


        String printCookie(byte[] c) {
            if(c == null) return "";
            return new String(c);
        }


        public void run() {
            Message msg;
            byte[] buf=new byte[256];
            int len=0;

            while(handler != null) {
                try {
                    if(in == null) {
                        Trace.error("ConnectionTable.Connection.run()", "input stream is null !");
                        break;
                    }
                    len=in.readInt();
                    if(len > buf.length)
                        buf=new byte[len];
                    in.readFully(buf, 0, len);
                    updateLastAccessed();
                    msg=(Message)Util.objectFromByteBuffer(buf);
                    receive(msg); // calls receiver.receiver(msg)
                }
                catch(OutOfMemoryError mem_ex) {
                    Trace.warn("ConnectionTable.Connection.run()", "dropped invalid message, closing connection");
                    break; // continue;
                }
                catch(EOFException eof_ex) {  // peer closed connection
                    Trace.info("ConnectionTable.Connection.run()", "exception is " + eof_ex);
                    notifyConnectionClosed(peer_addr);
                    break;
                }
                catch(IOException io_ex) {
                    Trace.info("ConnectionTable.Connection.run()", "exception is " + io_ex);
                    notifyConnectionClosed(peer_addr);
                    break;
                }
                catch(Exception e) {
                    Trace.warn("ConnectionTable.Connection.run()", "exception is " + e);
                }
            }
            handler=null;
            closeSocket();
            remove(peer_addr);
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
                    sock.close(); // should actually close in/out (so we don't need to close them explicitly)
                }
                catch(Exception e) {
                }
                sock=null;
            }
            if(out != null) {
                try {
                    out.close(); // flushes data
                }
                catch(Exception e) {
                }
                // removed 4/22/2003 (request by Roland Kurmann)
                // out=null;
            }
            if(in != null) {
                try {
                    in.close();
                }
                catch(Exception ex) {
                }
                in=null;
            }
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
                t=new Thread(thread_group, this, "ConnectionTable.ReaperThread");
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
                Trace.info("ConnectionTable.Reaper.run()", "connection reaper thread was started. Number of connections=" +
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
                            Trace.info("ConnectionTable.Reaper.run()", "connection is " +
                                                                       ((curr_time - value.last_access) / 1000) + " seconds old (curr-time=" +
                                                                       curr_time + ", last_access=" + value.last_access + ")");
                        if(value.last_access + conn_expire_time < curr_time) {
                            if(Trace.trace)
                                Trace.info("ConnectionTable.Reaper.run()", "connection " + value +
                                                                           " has been idle for too long (conn_expire_time=" + conn_expire_time +
                                                                           "), will be removed");

                            value.destroy();
                            it.remove();
                        }
                    }
                }
            }
            if(Trace.trace)
                Trace.info("ConnectionTable.Reaper.run()", "reaper terminated");
            t=null;
        }
    }


}



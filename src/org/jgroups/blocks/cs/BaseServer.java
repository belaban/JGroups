package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.TimeService;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class for a server handling sending, receiving and connection management.
 * @since 3.6.5
 */
@MBean(description="Server used to accept connections from other servers (or clients) and send data to servers")
public abstract class BaseServer implements Closeable, ConnectionListener {
    protected Address                         local_addr; // typically the address of the server socket or channel
    protected final List<ConnectionListener>  conn_listeners=new CopyOnWriteArrayList<>();
    protected final Map<Address,Connection>   conns=new HashMap<>();
    protected final Lock                      sock_creation_lock=new ReentrantLock(true); // syncs socket establishment
    protected final ThreadFactory             factory;
    protected long                            reaperInterval;
    protected Reaper                          reaper;
    protected Receiver                        receiver;
    protected final AtomicBoolean             running=new AtomicBoolean(false);
    protected Log                             log=LogFactory.getLog(getClass());
    protected InetAddress                     client_bind_addr;
    protected int                             client_bind_port;
    protected boolean                         defer_client_binding;
    @ManagedAttribute(description="Time (ms) after which an idle connection is closed. 0 disables connection reaping",writable=true)
    protected long                            conn_expire_time;  // ns
    @ManagedAttribute(description="Size (bytes) of the receive channel/socket",writable=true)
    protected int                             recv_buf_size=120000;
    @ManagedAttribute(description="Size (bytes) of the send channel/socket",writable=true)
    protected int                             send_buf_size=60000;

    @ManagedAttribute(description="When A connects to B, B reuses the same TCP connection to send data to A")
    protected boolean                         use_peer_connections;
    protected int                             sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    protected boolean                         tcp_nodelay=false;
    protected int                             linger=-1;
    protected TimeService                     time_service;


    protected BaseServer(ThreadFactory f) {
        this.factory=f;
    }



    public Receiver         receiver()                              {return receiver;}
    public BaseServer       receiver(Receiver r)                    {this.receiver=r; return this;}
    public long             reaperInterval()                        {return reaperInterval;}
    public BaseServer       reaperInterval(long interval)           {this.reaperInterval=interval; return this;}
    public Log              log()                                   {return log;}
    public BaseServer       log(Log the_log)                        {this.log=the_log; return this;}
    public Address          localAddress()                          {return local_addr;}
    public InetAddress      clientBindAddress()                     {return client_bind_addr;}
    public BaseServer       clientBindAddress(InetAddress addr)     {this.client_bind_addr=addr; return this;}
    public int              clientBindPort()                        {return client_bind_port;}
    public BaseServer       clientBindPort(int port)                {this.client_bind_port=port; return this;}
    public boolean          deferClientBinding()                    {return defer_client_binding;}
    public BaseServer       deferClientBinding(boolean defer)       {this.defer_client_binding=defer; return this;}
    public boolean          usePeerConnections()                    {return use_peer_connections;}
    public BaseServer       usePeerConnections(boolean flag)        {this.use_peer_connections=flag; return this;}
    public int              socketConnectionTimeout()               {return sock_conn_timeout;}
    public BaseServer       socketConnectionTimeout(int timeout)    {this.sock_conn_timeout = timeout; return this;}
    public long             connExpireTime()                        {return conn_expire_time;}
    public BaseServer       connExpireTimeout(long t)               {conn_expire_time=TimeUnit.NANOSECONDS.convert(t, TimeUnit.MILLISECONDS); return this;}
    public TimeService      timeService()                           {return time_service;}
    public BaseServer       timeService(TimeService ts)             {this.time_service=ts; return this;}
    public int              receiveBufferSize()                     {return recv_buf_size;}
    public BaseServer       receiveBufferSize(int recv_buf_size)    {this.recv_buf_size = recv_buf_size; return this;}
    public int              sendBufferSize()                        {return send_buf_size;}
    public BaseServer       sendBufferSize(int send_buf_size)       {this.send_buf_size = send_buf_size; return this;}
    public int              linger()                                {return linger;}
    public BaseServer       linger(int linger)                      {this.linger=linger; return this;}
    public boolean          tcpNodelay()                            {return tcp_nodelay;}
    public BaseServer       tcpNodelay(boolean tcp_nodelay)         {this.tcp_nodelay = tcp_nodelay; return this;}
    @ManagedAttribute(description="True if the server is running, else false")
    public boolean          running()                               {return running.get();}


    @ManagedAttribute(description="Number of connections")
    public synchronized int getNumConnections() {
        return conns.size();
    }

    @ManagedAttribute(description="Number of currently open connections")
    public synchronized int getNumOpenConnections() {
        int retval=0;
        for(Connection conn: conns.values())
            if(conn.isOpen())
                retval++;
        return retval;
    }


    /**
     * Starts accepting connections. Typically, socket handler or selectors thread are started here.
     */
    public void start() throws Exception {
        if(reaperInterval > 0 && (reaper == null || !reaper.isAlive())) {
            reaper=new Reaper();
            reaper.start();
        }
    }

    /**
     * Stops listening for connections and handling traffic. Typically, socket handler or selector threads are stopped,
     * and server sockets or channels are closed.
     */
    public void stop() {
        Util.close(reaper);
        reaper=null;

        synchronized(this) {
            for(Map.Entry<Address,Connection> entry: conns.entrySet())
                Util.close(entry.getValue());
            conns.clear();
        }
        conn_listeners.clear();
    }

    public void close() throws IOException {
        stop();
    }


    /**
     * Called by a {@link Connection} implementation when a message has been received. Note that data might be a
     * reused buffer, so unless used to de-serialize an object from it, it should be copied (e.g. if we store a ref
     * to it beyone the scope of this receive() method)
     */
    public void receive(Address sender, byte[] data, int offset, int length) {
        if(this.receiver != null)
            this.receiver.receive(sender, data, offset, length);
    }

    /**
     * Called by a {@link Connection} implementation when a message has been received
     */
    public void receive(Address sender, ByteBuffer buf) {
        if(this.receiver != null)
            this.receiver.receive(sender, buf);
    }



    public void send(Address dest, byte[] data, int offset, int length) throws Exception {
        if(!validateArgs(dest, data))
            return;

        if(dest == null) {
            sendToAll(data, offset, length);
            return;
        }

        if(dest.equals(local_addr)) {
            receive(dest, data, offset, length);
            return;
        }

        // Get a connection (or create one if not yet existent) and send the data
        Connection conn=null;
        try {
            conn=getConnection(dest);
            conn.send(data, offset, length);
        }
        catch(Exception ex) {
            removeConnectionIfPresent(dest, conn);
            throw ex;
        }
    }


    public void send(Address dest, ByteBuffer data) throws Exception {
        if(!validateArgs(dest, data))
            return;

        if(dest == null) {
            sendToAll(data);
            return;
        }

        if(dest.equals(local_addr)) {
            receive(dest, data);
            return;
        }

        // Get a connection (or create one if not yet existent) and send the data
        Connection conn=null;
        try {
            conn=getConnection(dest);
            conn.send(data);
        }
        catch(Exception ex) {
            removeConnectionIfPresent(dest, conn);
            throw ex;
        }
    }


    @Override
    public void connectionClosed(Connection conn, String reason) {
        removeConnectionIfPresent(conn.peerAddress(), conn);
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    /** Creates a new connection object to target dest, but doesn't yet connect it */
    protected abstract Connection createConnection(Address dest) throws Exception;

    public synchronized boolean hasConnection(Address address) {
        return conns.containsKey(address);
    }

    public synchronized boolean connectionEstablishedTo(Address address) {
        Connection conn=conns.get(address);
        return conn != null && conn.isConnected();
    }

    /** Creates a new connection to dest, or returns an existing one */
    public Connection getConnection(Address dest) throws Exception {
        Connection conn;
        synchronized(this) {
            if((conn=conns.get(dest)) != null && conn.isOpen()) // keep FAST path on the most common case
                return conn;
        }

        Exception connect_exception=null; // set if connect() throws an exception
        sock_creation_lock.lockInterruptibly();
        try {
            // lock / release, create new conn under sock_creation_lock, it can be skipped but then it takes
            // extra check in conn map and closing the new connection, w/ sock_creation_lock it looks much simpler
            // (slow path, so not important)

            synchronized(this) {
                conn=conns.get(dest); // check again after obtaining sock_creation_lock
                if(conn != null && conn.isOpen())
                    return conn;

                // create conn stub
                conn=createConnection(dest);
                replaceConnection(dest, conn);
            }

            // now connect to dest:
            try {
                log.trace("%s: connecting to %s", local_addr, dest);
                conn.connect(dest);
                notifyConnectionEstablished(conn);
                conn.start();
            }
            catch(Exception connect_ex) {
                connect_exception=connect_ex;
            }

            synchronized(this) {
                Connection existing_conn=conns.get(dest); // check again after obtaining sock_creation_lock
                if(existing_conn != null && existing_conn.isOpen() // added by a successful accept()
                  && existing_conn != conn) {
                    log.trace("%s: found existing connection to %s, using it and deleting own conn-stub", local_addr, dest);
                    Util.close(conn); // close our connection; not really needed as conn was closed by accept()
                    return existing_conn;
                }

                if(connect_exception != null) {
                    log.trace("%s: failed connecting to %s: %s", local_addr, dest, connect_exception);
                    removeConnectionIfPresent(dest, conn); // removes and closes the conn
                    throw connect_exception;
                }
                return conn;
            }
        }
        finally {
            sock_creation_lock.unlock();
        }
    }

    @GuardedBy("this")
    public void replaceConnection(Address address, Connection conn) {
        Connection previous=conns.put(address, conn);
        Util.close(previous); // closes previous connection (if present)
        notifyConnectionEstablished(conn);
    }

    public void closeConnection(Connection conn, Throwable ex) {
        Util.close(conn);
        notifyConnectionClosed(conn, ex.toString());
        removeConnectionIfPresent(conn != null? conn.peerAddress() : null, conn);
    }


    public synchronized void addConnection(Address peer_addr, Connection conn) throws Exception {
        boolean conn_exists=hasConnection(peer_addr),
          replace=conn_exists && local_addr.compareTo(peer_addr) < 0; // bigger conn wins

        if(!conn_exists || replace) {
            replaceConnection(peer_addr, conn); // closes old conn
            conn.start();
        }
        else {
            log.trace("%s: rejected connection from %s %s", local_addr, peer_addr, explanation(conn_exists, replace));
            Util.close(conn); // keep our existing conn, reject accept() and close client_sock
        }
    }


    public synchronized void addConnectionListener(ConnectionListener cml) {
        if(cml != null && !conn_listeners.contains(cml))
            conn_listeners.add(cml);
    }

    public synchronized void removeConnectionListener(ConnectionListener cml) {
        if(cml != null)
            conn_listeners.remove(cml);
    }
    

    @ManagedOperation(description="Prints all connections")
    public String printConnections() {
        StringBuilder sb=new StringBuilder("\n");
        synchronized(this) {
            for(Map.Entry<Address,Connection> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(Address address, Connection conn) {
        if(address == null || conn == null)
            return;

        synchronized(this) {
            Connection existing=conns.get(address);
            if(conn == existing) {
                Connection tmp=conns.remove(address);
                log.trace("%s: removed connection to %s", local_addr, address);
                Util.close(tmp);
            }
        }
    }

    /** Used only for testing ! */
    public synchronized void clearConnections() {
        for(Connection conn: conns.values())
            Util.close(conn);
        conns.clear();
    }

    /** Removes all connections which are not in current_mbrs */
    public void retainAll(Collection<Address> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<Address,Connection> copy=null;
        synchronized(this) {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        }
        copy.keySet().removeAll(current_mbrs);
        for(Map.Entry<Address,Connection> entry: copy.entrySet())
            Util.close(entry.getValue());
        copy.clear();
    }

    public void notifyConnectionClosed(Connection conn, String cause) {
        for(ConnectionListener l: conn_listeners) {
            try {
                l.connectionClosed(conn, cause);
            }
            catch(Throwable t) {
                log.warn("failed notifying listener %s of connection close: %s", l, t);
            }
        }
    }

    public void notifyConnectionEstablished(Connection conn) {
        for(ConnectionListener l: conn_listeners) {
            try {
                l.connectionEstablished(conn);
            }
            catch(Throwable t) {
                log.warn("failed notifying listener %s of connection establishment: %s", l, t);
            }
        }
    }


    public String toString() {
        return new StringBuilder(getClass().getSimpleName()).append(": local_addr=").append(local_addr).append("\n")
          .append("connections (" + conns.size() + "):\n").append(super.toString()).append('\n').toString();
    }


    protected void sendToAll(byte[] data, int offset, int length) {
        for(Map.Entry<Address,Connection> entry: conns.entrySet()) {
            Connection conn=entry.getValue();
            try {
                conn.send(data, offset, length);
            }
            catch(Throwable ex) {
                Address dest=entry.getKey();
                removeConnectionIfPresent(dest, conn);
                log.error("failed sending data to %s: %s", dest, ex);
            }
        }
    }


    protected void sendToAll(ByteBuffer data) {
        for(Map.Entry<Address,Connection> entry: conns.entrySet()) {
            Connection conn=entry.getValue();
            try {
                conn.send(data.duplicate());
            }
            catch(Throwable ex) {
                Address dest=entry.getKey();
                removeConnectionIfPresent(dest, conn);
                log.error("failed sending data to %s: %s", dest, ex);
            }
        }
    }


    protected static org.jgroups.Address localAddress(InetAddress bind_addr, int local_port, InetAddress external_addr, int external_port) {
        if(external_addr != null)
            return new IpAddress(external_addr, external_port > 0? external_port : local_port);
        return bind_addr != null? new IpAddress(bind_addr, local_port) : new IpAddress(local_port);
    }

    protected <T> boolean validateArgs(Address dest, T buffer) {
        if(buffer == null) {
            log.warn("%s: data is null; discarding message to %s", local_addr, dest);
            return false;
        }

        if(!running.get()) {
            log.trace("%s: server is not running, discarding message to %s", local_addr, dest);
            return false;
        }
        return true;
    }



    protected static String explanation(boolean connection_existed, boolean replace) {
        StringBuilder sb=new StringBuilder();
        if(connection_existed) {
            sb.append(" (connection existed");
            if(replace)
                sb.append(" but was replaced because my address is lower)");
            else
                sb.append(" and my address won as it's higher)");
        }
        else
            sb.append(" (connection didn't exist)");
        return sb.toString();
    }


    protected class Reaper implements Runnable, Closeable {
        private Thread thread;

        public synchronized void start() {
            if(thread == null || !thread.isAlive()) {
                thread=factory.newThread(new Reaper(), "Reaper");
                thread.start();
            }
        }

        public synchronized void stop() {
            if(thread != null && thread.isAlive()) {
                thread.interrupt();
                try {
                    thread.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                }
                catch(InterruptedException ignored) {
                }
            }
            thread=null;
        }

        public void close() throws IOException {stop();}

        public synchronized boolean isAlive() {return thread != null && thread.isDaemon();}

        public void run() {
            while(!Thread.currentThread().isInterrupted()) {
                synchronized(BaseServer.this) {
                    for(Iterator<Entry<Address,Connection>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<Address,Connection> entry=it.next();
                        Connection c=entry.getValue();
                        if(c.isExpired(System.nanoTime())) {
                            Util.close(c);
                            it.remove();                           
                        }
                    }
                }
                Util.sleep(reaperInterval);
            }           
        }
    }

}

package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import javax.net.ssl.SSLException;
import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Abstract class for a server handling sending, receiving and connection management.
 * @since 3.6.5
 */
@MBean(description="Server used to accept connections from other servers (or clients) and send data to servers")
public abstract class BaseServer implements Closeable, ConnectionListener {
    protected final Lock                      lock=new ReentrantLock();
    protected Address                         local_addr; // typically the address of the server socket or channel
    protected final List<ConnectionListener>  conn_listeners=new CopyOnWriteArrayList<>();
    protected final Map<Address,Connection>   conns=new ConcurrentHashMap<>();
    protected final ThreadFactory             factory;
    protected SocketFactory                   socket_factory=new DefaultSocketFactory();
    protected long                            reaperInterval;
    protected Reaper reaper;
    protected Receiver                        receiver;
    protected final AtomicBoolean             running=new AtomicBoolean(false);
    protected Log                             log=LogFactory.getLog(getClass());
    protected InetAddress                     client_bind_addr;
    protected int                             client_bind_port;
    protected boolean                         defer_client_binding;
    @ManagedAttribute(description="Time (ms) after which an idle connection is closed. 0 disables connection reaping",
      writable=true,type=AttributeType.TIME)
    protected long                            conn_expire_time;  // ns
    @ManagedAttribute(description="Size (bytes) of the receive channel/socket",writable=true,type=AttributeType.BYTES)
    protected int                             recv_buf_size;
    @ManagedAttribute(description="Size (bytes) of the send channel/socket",writable=true,type=AttributeType.BYTES)
    protected int                             send_buf_size;

    @ManagedAttribute(description="The max number of bytes a message can have. If greater, an exception will be " +
      "thrown. 0 disables this",
      writable=true,type=AttributeType.BYTES)
    protected int                             max_length;

    @ManagedAttribute(description="When A connects to B, B reuses the same TCP connection to send data to A")
    protected boolean                         use_peer_connections;
    @ManagedAttribute(description="Wait for an ack from the server when a connection is established, and retry " +
      "connection establishment until a valid connection has been established, or the connection to the peer cannot " +
      "be established (https://issues.redhat.com/browse/JGRP-2684)",writable=true)
    protected boolean                         use_acks;
    @ManagedAttribute(description="Log a stack trace when a connection is closed")
    protected boolean                         log_details=true;
    protected int                             sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    protected boolean                         tcp_nodelay=false;
    protected int                             linger=-1;
    protected TimeService                     time_service;
    public static final byte[]                OK={1,2,3,4};   // ack (srv->client) on successful connection establishment



    protected BaseServer(ThreadFactory f, SocketFactory sf, int recv_buf_size) {
        this.factory=f;
        this.recv_buf_size=recv_buf_size;
        if(sf != null)
            this.socket_factory=sf;
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
    public SocketFactory    socketFactory()                         {return socket_factory;}
    public BaseServer       socketFactory(SocketFactory factory)    {this.socket_factory=factory; return this;}
    public boolean          usePeerConnections()                    {return use_peer_connections;}
    public BaseServer       usePeerConnections(boolean flag)        {this.use_peer_connections=flag; return this;}
    public boolean          useAcks()                               {return use_acks;}
    public BaseServer       useAcks(boolean f)                      {use_acks=f; return this;}
    public boolean          logDetails()                            {return log_details;}
    public BaseServer       logDetails(boolean l)                   {log_details=l; return this;}
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
    public int              getMaxLength()                          {return max_length;}
    public BaseServer       setMaxLength(int len)                   {max_length=len; return this;}
    public int              linger()                                {return linger;}
    public BaseServer       linger(int linger)                      {this.linger=linger; return this;}
    public boolean          tcpNodelay()                            {return tcp_nodelay;}
    public BaseServer       tcpNodelay(boolean tcp_nodelay)         {this.tcp_nodelay = tcp_nodelay; return this;}
    @ManagedAttribute(description="True if the server is running, else false")
    public boolean          running()                               {return running.get();}


    @ManagedAttribute(description="Number of connections")
    public int getNumConnections() {
        return conns.size();
    }

    @ManagedAttribute(description="Number of currently open connections")
    public int getNumOpenConnections() {
        int retval=0;
        for(Connection conn: conns.values())
            if(!conn.isClosed())
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

        lock.lock();
        try {
            for(Connection c: conns.values())
                Util.close(c);
            conns.clear();
        } finally {
            lock.unlock();
        }
        conn_listeners.clear();
    }

    public void close() throws IOException {
        stop();
    }

    public synchronized void flush(Address dest) {
        if(dest != null) {
            Connection conn=conns.get(dest);
            if(conn != null)
                conn.flush();
        }
    }

    public synchronized void flushAll() {
        for(Connection c: conns.values())
            c.flush();
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

    public void receive(Address sender, DataInput in, int len) throws Exception {
        // https://issues.redhat.com/browse/JGRP-2523: check if max_length has been exceeded
        if(max_length > 0 && len > max_length)
            throw new IllegalStateException(String.format("the length of a message (%s) from %s is bigger than the " +
                                                            "max accepted length (%s): discarding the message",
                                                          Util.printBytes(len), sender, Util.printBytes(max_length)));
        if(this.receiver != null)
            this.receiver.receive(sender, in, len);
        else {
            // discard len bytes (in.skip() is not guaranteed to discard *all* len bytes)
            byte[] buf=new byte[len];
            in.readFully(buf, 0, len);
        }
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
            conn=getConnection(dest, use_acks);
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
            conn=getConnection(dest, use_acks);
            conn.send(data);
        }
        catch(Exception ex) {
            removeConnectionIfPresent(dest, conn);
            throw ex;
        }
    }


    @Override
    public void connectionClosed(Connection conn) {
        removeConnectionIfPresent(conn.peerAddress(), conn);
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    /** Creates a new connection object to target dest, but doesn't yet connect it */
    protected abstract Connection createConnection(Address dest) throws Exception;

    public boolean hasConnection(Address address) {
        return conns.containsKey(address);
    }

    public boolean connectionEstablishedTo(Address address) {
        return connected(conns.get(address));
    }

    public Connection getConnection(Address dest, boolean retry) throws Exception {
        if(!retry)
            return getConnection(dest);
        Connection conn=null;
        do {
            try {
                conn=getConnection(dest);
            }
            catch(ConnectException | SocketTimeoutException | SSLException ex) {
                throw ex;
            }
            catch(Exception ise) {
                Util.sleepRandom(1, 100);
                log.trace("%s: retrying connection to %s", local_addr, dest);
            }
        }
        while(!connected(conn));
        return conn;
    }


    /** Creates a new connection to dest, or returns an existing one */
    public Connection getConnection(Address dest) throws Exception {
        Connection conn;
        // keep FAST path on the most common case
        if(connected(conn=conns.get(dest)))
            return conn;

        lock.lock();
        try {
            if(connected(conn=conns.get(dest)))
                return conn;
            conn=createConnection(dest);
            replaceConnection(dest, conn);

            // now connect to dest:
            try {
                log.trace("%s: connecting to %s", local_addr, dest);
                conn.connect(dest);
                notifyConnectionEstablished(conn);
                conn.start();
            }
            catch(Exception connect_ex) {
                log.trace("%s: failed connecting to %s: %s", local_addr, dest, connect_ex);
                removeConnectionIfPresent(dest, conn); // removes and closes the conn
                throw connect_ex;
            }
        } finally {
            lock.unlock();
        }
        return conn;
    }

    @GuardedBy("this")
    public void replaceConnection(Address address, Connection conn) {
        Connection previous=conns.put(address, conn);
        if(previous != null) {
            previous.flush();
            Util.close(previous);
        }
    }

    public void closeConnection(Connection conn) {
        closeConnection(conn, true);
    }

    public void closeConnection(Connection conn, boolean notify) {
        Util.close(conn);
        if(notify)
            notifyConnectionClosed(conn);
        removeConnectionIfPresent(conn != null? conn.peerAddress() : null, conn);
    }

    public boolean closeConnection(Address addr) {
        return closeConnection(addr, true);
    }

    public boolean closeConnection(Address addr, boolean notify) {
        Connection c;
        if(addr != null && (c=conns.get(addr)) != null) {
            closeConnection(c, notify);
            return true;
        }
        return false;
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


    public BaseServer addConnectionListener(ConnectionListener cl) {
        if(cl == null)
            return this;
        synchronized(conn_listeners) {
            if(!conn_listeners.contains(cl))
                conn_listeners.add(cl);
        }
        return this;
    }

    public BaseServer removeConnectionListener(ConnectionListener cl) {
        if(cl == null)
            return this;
        synchronized(conn_listeners) {
            conn_listeners.remove(cl);
        }
        return this;
    }
    

    @ManagedOperation(description="Prints all connections")
    public String printConnections() {
        StringBuilder sb=new StringBuilder("\n");
        for(Map.Entry<Address,Connection> entry: conns.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(Address address, Connection conn) {
        if(address == null || conn == null)
            return;
        Connection tmp=null;

        lock.lock();
        try {
            Connection existing=conns.get(address);
            if(conn == existing) {
                tmp=conns.remove(address);
            }
        } finally {
            lock.unlock();
        }
        if(tmp != null) { // Moved conn close outside of sync block (https://issues.redhat.com/browse/JGRP-2053)
            log.trace("%s: removed connection to %s", local_addr, address);
            Util.close(tmp);
        }
    }

    /** Used only for testing ! */
    public synchronized void clearConnections() {
        conns.values().forEach(Util::close);
        conns.clear();
    }

    public void forAllConnections(BiConsumer<Address,Connection> c) {
        conns.forEach(c);
    }

    /** Removes all connections which are not in current_mbrs */
    public void retainAll(Collection<Address> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<Address,Connection> copy=null;
        lock.lock();
        try {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        } finally {
            lock.unlock();
        }
        copy.keySet().removeAll(current_mbrs);
        for(Map.Entry<Address,Connection> entry: copy.entrySet())
            Util.close(entry.getValue());
        copy.clear();
    }

    public void notifyConnectionClosed(Connection conn) {
        for(ConnectionListener l: conn_listeners) {
            try {
                l.connectionClosed(conn);
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
        return toString(false);
    }

    public String toString(boolean details) {
        String s=String.format("%s (%s, %d conns)", getClass().getSimpleName(), local_addr, conns.size());
        if(details && !conns.isEmpty()) {
            String tmp=conns.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
            return String.format("%s:\n%s", s, tmp);
        }
        return s;
    }


    public void sendToAll(byte[] data, int offset, int length) {
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


    public void sendToAll(ByteBuffer data) {
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

    protected static boolean connected(Connection c) {
        return c != null && !c.isClosed() && (c.isConnected() || c.isConnectionPending());
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
                lock.lock();
                try {
                    for(Iterator<Entry<Address,Connection>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<Address,Connection> entry=it.next();
                        Connection c=entry.getValue();
                        if(c.isExpired(System.nanoTime())) {
                            Util.close(c);
                            it.remove();                           
                        }
                    }
                } finally {
                    lock.unlock();
                }
                Util.sleep(reaperInterval);
            }           
        }
    }

}

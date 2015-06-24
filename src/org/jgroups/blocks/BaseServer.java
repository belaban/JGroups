package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.nio.Receiver;
import org.jgroups.nio.Server;
import org.jgroups.util.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class for a server handling sending, receiving and connection management.
 * @param <A> The type of the address, e.g. {@link Address}
 * @param <C> The type of the connection, e.g. {@link TcpConnection}
 * @since 3.6.5
 */
public abstract class BaseServer<A extends Address, C extends Connection> implements Server<A>, Closeable {
    protected A                                      local_addr;
    protected final List<ConnectionListener<A,C>>    conn_listeners=new ArrayList<>();
    protected final Map<A,C>                         conns=new HashMap<>();
    protected final Lock                             sock_creation_lock=new ReentrantLock(true); // syncs socket establishment
    protected final ThreadFactory                    factory;
    protected long                                   reaperInterval;
    protected final Reaper                           reaper;
    protected Receiver<A>                            receiver;
    protected final AtomicBoolean                    running=new AtomicBoolean(false);
    protected Log                                    log=LogFactory.getLog(getClass());
    protected InetAddress                            client_bind_addr;
    protected int                                    client_bind_port;
    protected boolean                                defer_client_binding;
    protected long                                   conn_expire_time;  // ns
    protected int                                    recv_buf_size=120000;
    protected int                                    send_buf_size=60000;
    protected int                                    sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    protected boolean                                tcp_nodelay=false;
    protected int                                    linger=-1;
    protected SocketFactory                          socket_factory=new DefaultSocketFactory();
    protected TimeService                            time_service;




    public BaseServer(ThreadFactory f, SocketFactory socket_factory, Receiver<A> r,
                      long reaper_interval, long conn_expire_time) throws Exception {
        this.factory=f;
        this.receiver=r;
        this.conn_expire_time=TimeUnit.NANOSECONDS.convert(conn_expire_time, TimeUnit.MILLISECONDS);
        this.socket_factory=socket_factory;
        this.reaperInterval=reaper_interval;
        reaper=reaperInterval > 0?  new Reaper() : null;
    }



    public Receiver<A>       receiver()                              {return receiver;}
    public BaseServer<A,C>   receiver(Receiver<A> r)                 {this.receiver=r; return this;}
    public long              reaperInterval()                        {return reaperInterval;}
    public BaseServer<A,C>   reaperInterval(long interval)           {this.reaperInterval=interval; return this;}
    public Log               log()                                   {return log;}
    public BaseServer<A,C>   log(Log the_log)                        {this.log=the_log; return this;}
    public A                 localAddress()                          {return local_addr;}
    public SocketFactory     socketFactory()                         {return socket_factory;}
    public BaseServer<A,C>   socketFactory(SocketFactory factory)    {this.socket_factory=factory; return this;}
    public InetAddress       clientBindAddress()                     {return client_bind_addr;}
    public BaseServer<A,C>   clientBindAddress(InetAddress addr)     {this.client_bind_addr=addr; return this;}
    public int               clientBindPort()                        {return client_bind_port;}
    public BaseServer<A,C>   clientBindPort(int port)                {this.client_bind_port=port; return this;}
    public boolean           deferClientBinding()                    {return defer_client_binding;}
    public BaseServer<A,C>   deferClientBinding(boolean defer)       {this.defer_client_binding=defer; return this;}
    public int               socketConnectionTimeout()               {return sock_conn_timeout;}
    public BaseServer<A,C>   socketConnectionTimeout(int timeout)    {this.sock_conn_timeout = timeout; return this;}
    public long              connExpireTime()                        {return conn_expire_time;}
    public BaseServer<A,C>   connExpireTimeout(long t)               {conn_expire_time=TimeUnit.NANOSECONDS.convert(t, TimeUnit.MILLISECONDS); return this;}
    public TimeService       timeService()                           {return time_service;}
    public BaseServer<A,C>   timeService(TimeService ts)             {this.time_service=ts; return this;}
    public int               receiveBufferSize()                     {return recv_buf_size;}
    public BaseServer<A,C>   receiveBufferSize(int recv_buf_size)    {this.recv_buf_size = recv_buf_size; return this;}
    public int               sendBufferSize()                        {return send_buf_size;}
    public BaseServer<A,C>   sendBufferSize(int send_buf_size)       {this.send_buf_size = send_buf_size; return this;}
    public int               linger()                                {return linger;}
    public BaseServer<A,C>   linger(int linger)                      {this.linger=linger; return this;}
    public boolean           tcpNodelay()                            {return tcp_nodelay;}
    public BaseServer<A,C>   tcpNodelay(boolean tcp_nodelay)         {this.tcp_nodelay = tcp_nodelay; return this;}


    @Override
    public void start() throws Exception {
        if(reaper != null)
            reaper.start();
    }

    @Override
    public void stop() {
        if(reaper != null)
            reaper.stop();

        synchronized(this) {
            for(Map.Entry<A,C> entry: conns.entrySet())
                Util.close(entry.getValue());
            conns.clear();
        }
        conn_listeners.clear();
    }

    public void close() throws IOException {
        stop();
    }

    /**
     * Called by a {@link Connection} implementation when a message has been received
     */
    public void receive(A sender, byte[] data, int offset, int length) {
        if(this.receiver != null)
            this.receiver.receive(sender, data, offset, length);
    }

    /**
     * Called by a {@link Connection} implementation when a message has been received
     */
    public void receive(A sender, ByteBuffer buf) {
        if(this.receiver != null)
            this.receiver.receive(sender, buf);
    }



    public void send(A dest, byte[] data, int offset, int length) throws Exception {
        if(!validateArgs(dest, data))
            return;

        if(dest.equals(local_addr)) {
            receive(dest, data, offset, length);
            return;
        }

        // Get a connection (or create one if not yet existent) and send the data
        C conn=null;
        try {
            conn=getConnection(dest);
            conn.send(data, offset, length);
        }
        catch(Exception ex) {
            removeConnectionIfPresent(dest, conn);
            throw ex;
        }
    }


    public void send(A dest, ByteBuffer data) throws Exception {
        if(!validateArgs(dest, data))
            return;

        if(dest.equals(local_addr)) {
            receive(dest, data);
            return;
        }

        // Get a connection (or create one if not yet existent) and send the data
        C conn=null;
        try {
            conn=getConnection(dest);
            conn.send(data);
        }
        catch(Exception ex) {
            removeConnectionIfPresent(dest, conn);
            throw ex;
        }
    }



    /** Creates a new connection object to target dest, but doesn't yet connect it */
    protected abstract C createConnection(A dest) throws Exception;

    public synchronized boolean hasConnection(A address) {
        return conns.containsKey(address);
    }

    public synchronized boolean connectionEstablishedTo(A address) {
        C conn=conns.get(address);
        return conn != null && conn.isConnected();
    }

    /** Creates a new connection to dest, or returns an existing one */
    public C getConnection(A dest) throws Exception {
        C conn;
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
                conn.start();
            }
            catch(Exception connect_ex) {
                connect_exception=connect_ex;
            }

            synchronized(this) {
                C existing_conn=conns.get(dest); // check again after obtaining sock_creation_lock
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
    public void replaceConnection(A address, C conn) {
        C previous=conns.put(address, conn);
        Util.close(previous); // closes previous connection (if present)
        notifyConnectionOpened(address, conn);
    }


    public synchronized void addConnection(A peer_addr, C conn) throws Exception {
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


    public synchronized void addConnectionListener(ConnectionListener<A,C> cml) {
        if(cml != null && !conn_listeners.contains(cml))
            conn_listeners.add(cml);
    }

    public synchronized void removeConnectionListener(ConnectionListener<A,C> cml) {
        if(cml != null)
            conn_listeners.remove(cml);
    }
    
    public synchronized int getNumConnections() {
        return conns.size();
    }

    public synchronized int getNumOpenConnections() {
        int retval=0;
        for(Connection conn: conns.values())
            if(conn.isOpen())
                retval++;
        return retval;
    }

    public String printConnections() {
        StringBuilder sb=new StringBuilder("\n");
        synchronized(this) {
            for(Map.Entry<A,C> entry: conns.entrySet())
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    /** Only removes the connection if conns.get(address) == conn */
    public void removeConnectionIfPresent(A address, C conn) {
        if(address == null || conn == null)
            return;

        synchronized(this) {
            C existing=conns.get(address);
            if(conn == existing) {
                C tmp=conns.remove(address);
                Util.close(tmp);
            }
        }
    }


    /** Removes all connections which are not in current_mbrs */
    public void retainAll(Collection<A> current_mbrs) {
        if(current_mbrs == null)
            return;

        Map<A,C> copy=null;
        synchronized(this) {
            copy=new HashMap<>(conns);
            conns.keySet().retainAll(current_mbrs);
        }
        copy.keySet().removeAll(current_mbrs);
        for(Map.Entry<A,C> entry: copy.entrySet())
            Util.close(entry.getValue());
        copy.clear();
    }
    

    public void notifyConnectionClosed(A address) {
        for(ConnectionListener<A,C> l: conn_listeners)
            l.connectionClosed(address);
    }

    public void notifyConnectionOpened(A address, C conn) {
        for(ConnectionListener<A,C> l: conn_listeners)
            l.connectionOpened(address, conn);
    }


    public String toString() {
        return new StringBuilder(getClass().getSimpleName()).append(": local_addr=").append(local_addr).append("\n")
          .append("connections (" + conns.size() + "):\n").append(super.toString()).append('\n').toString();
    }


    protected <T> boolean validateArgs(A dest, T buffer) {
        if(dest == null)
            throw new IllegalArgumentException(String.format("%s: destination is null", local_addr));

        if(buffer == null) {
            log.warn("%s: data is null; discarding message to %s", local_addr, dest);
            return false;
        }

        if(!running.get()) {
            log.warn("%s: server is not running, discarding message to %s", local_addr, dest);
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


    protected class Reaper implements Runnable {
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

        public void run() {
            while(!Thread.currentThread().isInterrupted()) {
                synchronized(BaseServer.this) {
                    for(Iterator<Entry<A,C>> it=conns.entrySet().iterator();it.hasNext();) {
                        Entry<A,C> entry=it.next();
                        C c=entry.getValue();
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

    public interface ConnectionListener<A, V extends Connection> {
        void connectionClosed(A address);
        void connectionOpened(A address, V conn);

    }
}

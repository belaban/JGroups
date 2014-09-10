package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.*;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class that manages TCP connections between members
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class TCPConnectionMap {
    protected final Mapper        mapper;
    protected final InetAddress   bind_addr;
    protected InetAddress         client_bind_addr;
    protected int                 client_bind_port;
    protected boolean             defer_client_binding;
    protected final Address       local_addr; // bind_addr + port of srv_sock
    protected final ServerSocket  srv_sock;
    protected Receiver            recvr;
    protected final long          conn_expire_time;  // ns
    protected Log                 log=LogFactory.getLog(getClass());
    protected int                 recv_buf_size=120000;
    protected int                 send_buf_size=60000;
    protected int                 send_queue_size=2000;
    protected int                 sock_conn_timeout=1000;      // max time in millis to wait for Socket.connect() to return
    protected int                 peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address
    protected boolean             tcp_nodelay=false;
    protected int                 linger=-1;
    protected final Thread        acceptor;
    protected final AtomicBoolean running=new AtomicBoolean(false);
    protected volatile boolean    use_send_queues=true;
    protected SocketFactory       socket_factory=new DefaultSocketFactory();
    protected TimeService         time_service;


    public TCPConnectionMap(String service_name,
                            ThreadFactory f,
                            SocketFactory socket_factory,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int external_port,
                            int srv_port,
                            int max_port
                            ) throws Exception {
        this(service_name, f,socket_factory, r,bind_addr,external_addr,external_port, srv_port,max_port,0,0);
    }

    public TCPConnectionMap(String service_name,
                            ThreadFactory f,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int external_port,
                            int srv_port,
                            int max_port,
                            long reaper_interval,
                            long conn_expire_time
                            ) throws Exception {
        this(service_name, f, null, r, bind_addr, external_addr, external_port, srv_port, max_port, reaper_interval, conn_expire_time);
    }

    public TCPConnectionMap(String service_name,
                            ThreadFactory f,
                            SocketFactory socket_factory,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int external_port,
                            int srv_port,
                            int max_port,
                            long reaper_interval,
                            long conn_expire_time
                            ) throws Exception {
        this.mapper = new Mapper(f,reaper_interval);
        this.recvr=r;
        this.bind_addr=bind_addr;
        this.conn_expire_time = TimeUnit.NANOSECONDS.convert(conn_expire_time, TimeUnit.MILLISECONDS);
        if(socket_factory != null)
            this.socket_factory=socket_factory;
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

        acceptor=f.newThread(new Acceptor(),"ConnectionMap.Acceptor [" + local_addr + "]");
    }

    public Address          getLocalAddress()                       {return local_addr;}
    public Receiver         getReceiver()                           {return recvr;}
    public void             setReceiver(Receiver receiver)          {this.recvr=receiver;}
    public SocketFactory    getSocketFactory()                      {return socket_factory;}
    public void             setSocketFactory(SocketFactory factory) {this.socket_factory=factory;}
    public InetAddress      clientBindAddress()                     {return client_bind_addr;}
    public TCPConnectionMap clientBindAddress(InetAddress addr)     {this.client_bind_addr=addr; return this;}
    public int              clientBindPort()                        {return client_bind_port;}
    public TCPConnectionMap clientBindPort(int port)                {this.client_bind_port=port; return this;}
    public boolean          deferClientBinding()                    {return defer_client_binding;}
    public TCPConnectionMap deferClientBinding(boolean defer)       {this.defer_client_binding=defer; return this;}
    public void             setReceiveBufferSize(int recv_buf_size) {this.recv_buf_size = recv_buf_size;}
    public void             setSocketConnectionTimeout(int timeout) {this.sock_conn_timeout = timeout;}
    public TCPConnectionMap peerAddressReadTimeout(int timeout)     {this.peer_addr_read_timeout=timeout; return this;}
    public TCPConnectionMap timeService(TimeService ts)             {this.time_service=ts; return this;}
    public void             setSendBufferSize(int send_buf_size)    {this.send_buf_size = send_buf_size;}
    public void             setLinger(int linger)                   {this.linger = linger;}
    public void             setTcpNodelay(boolean tcp_nodelay)      {this.tcp_nodelay = tcp_nodelay;}
    public void             setSendQueueSize(int send_queue_size)   {this.send_queue_size = send_queue_size;}
    public void             setUseSendQueues(boolean flag)          {this.use_send_queues=flag;}
    public int              getNumConnections()                     {return mapper.getNumConnections();}
    public int              getNumOpenConnections()                 {return mapper.getNumOpenConnections();}
    public boolean          connectionEstablishedTo(Address addr)   {return mapper.connectionEstablishedTo(addr);}
    public String           printConnections()                      {return mapper.printConnections();}
    public void             retainAll(Collection<Address> members)  {mapper.retainAll(members);}
    public int              getSenderQueueSize()                    {return send_queue_size;}
    public TCPConnectionMap log(Log new_log)                        {this.log=new_log; return this;}

    public void addConnectionMapListener(AbstractConnectionMap.ConnectionMapListener<TCPConnection> l) {
        mapper.addConnectionMapListener(l);
    }

    public void removeConnectionMapListener(AbstractConnectionMap.ConnectionMapListener<TCPConnection> l) {
        mapper.removeConnectionMapListener(l);
    }

    /**
     * Calls the receiver callback. We do not serialize access to this method,
     * and it may be called concurrently by several Connection handler threads.
     * Therefore the receiver needs to be reentrant.
     */
    public void receive(Address sender, byte[] data, int offset, int length) {
        recvr.receive(sender,data,offset,length);
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {        
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error(local_addr +  ": destination is null");
            return;
        }

        if(data == null) {
            log.warn(local_addr + ": data is null; discarding message to " + dest);
            return;
        }      
        
        if(!running.get() ) {
            if(log.isDebugEnabled())
                log.debug(local_addr + ": connection table is not running, discarding message to " + dest);
            return;
        }

        if(dest.equals(local_addr)) {
            receive(local_addr, data, offset, length);
            return;
        }

        // 1. Try to obtain correct Connection (or create one if not yet existent)
        TCPConnection conn=null;
        try {
            conn=mapper.getConnection(dest);
        }
        catch(Throwable t) {
        }

        // 2. Send the message using that connection
        if(conn != null && !conn.isConnected()) { // perhaps not connected because of concurrent connections (JGRP-1549)
            Util.sleepRandom(1, 50);
            try {
                conn=mapper.getConnection(dest); // try one more time
            }
            catch(Throwable t) {
            }
        }

        if(conn != null) {
            try {
                conn.send(data, offset, length);
            }
            catch(Exception ex) {
                mapper.removeConnectionIfPresent(dest,conn);
                throw ex;
            }
        }
    }

    /** Flushes the TCPConnection associated with destination */
    public void flush(Address destination) throws Exception {
        TCPConnection conn=mapper.getConnection(destination);
        if(conn != null)
            conn.flush();
    }

    public void start() throws Exception {        
        if(running.compareAndSet(false, true)) {
            acceptor.start();
            mapper.start();
        }
    }

    public void stop() {
        if(running.compareAndSet(true, false)) {
            try {
                getSocketFactory().close(srv_sock);
            }
            catch(IOException e) {
            }
            Util.interruptAndWaitToDie(acceptor);
            mapper.stop();
        }
    }


    public String toString() {
        StringBuilder ret=new StringBuilder();
        ret.append("local_addr=" + local_addr).append("\n");
        ret.append("connections (" + mapper.size() + "):\n");
        ret.append(mapper.toString());
        ret.append('\n');
        return ret.toString();
    }

    
    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            client_sock.setSendBufferSize(send_buf_size);
        }
        catch(IllegalArgumentException ex) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": exception setting send buffer size to " + send_buf_size + " bytes", ex);
        }
        try {
            client_sock.setReceiveBufferSize(recv_buf_size);
        }
        catch(IllegalArgumentException ex) {
            log.error(local_addr + ": exception setting receive buffer size to " + send_buf_size + " bytes", ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(tcp_nodelay);
        if(linger > 0)
            client_sock.setSoLinger(true, linger);
        else
            client_sock.setSoLinger(false, -1);
    }



    /** Used for message reception. */
    public interface Receiver {
        void receive(Address sender, byte[] data, int offset, int length);
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
                    if(log.isWarnEnabled())
                        log.warn(Util.getMessage("AcceptError"), ex);
                    Util.close(client_sock);
                }
            }
        }


        protected void handleAccept(final Socket client_sock) throws Exception {
            TCPConnection conn=null;
            try {
                conn=new TCPConnection(client_sock);
                Address peer_addr=conn.getPeerAddress();
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": " + peer_addr + " trying to connect to me");
                mapper.getLock().lock();
                try {
                    boolean conn_exists=mapper.hasConnection(peer_addr),
                      replace=conn_exists && local_addr.compareTo(peer_addr) < 0; // bigger conn wins

                    if(!conn_exists || replace) {
                        mapper.addConnection(peer_addr, conn); // closes old conn
                        conn.start(mapper.getThreadFactory());
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": accepted connection from " + peer_addr +
                                        explanation(conn_exists, replace));
                    }
                    else {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": rejected connection from " + peer_addr +
                                        explanation(conn_exists, replace));
                        Util.close(conn); // keep our existing conn, reject accept() and close client_sock
                    }
                }
                finally {
                    mapper.getLock().unlock();
                }
            }
            catch(Exception ex) {
                Util.close(conn);
                throw ex;
            }
        }
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
        else {
            sb.append(" (connection didn't exist)");
        }
        return sb.toString();
    }


    
    public class TCPConnection implements Connection {
        protected final Socket           sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
        protected final ReentrantLock    send_lock=new ReentrantLock(); // serialize send()
        protected final byte[]           cookie= { 'b', 'e', 'l', 'a' };
        protected DataOutputStream       out;
        protected DataInputStream        in;
        protected Address                peer_addr; // address of the 'other end' of the connection
        protected long                   last_access=getTimestamp(); // last time a message was sent or received (ns)
        protected Sender                 sender;
        protected Receiver               receiver;

        /** Creates a connection stub and binds it, use {@link #connect(java.net.SocketAddress)} to connect */
        public TCPConnection(Address peer_addr) throws Exception {
            if(peer_addr == null)
                throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);
            this.peer_addr=peer_addr;
            this.sock=socket_factory.createSocket("jgroups.tcp.sock");
            setSocketParameters(sock);
        }

        public TCPConnection(Socket s) throws Exception {
            if(s == null)
                throw new IllegalArgumentException("Invalid parameter s=" + s);                       
            setSocketParameters(s);
            this.out=new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
            this.in=new DataInputStream(new BufferedInputStream(s.getInputStream()));
            this.peer_addr=readPeerAddress(s);
            this.sock=s;
        }

        protected long getTimestamp() {
            return time_service != null? time_service.timestamp() : System.nanoTime();
        }

        protected Address getPeerAddress() {
            return peer_addr;
        }

        protected boolean isSenderUsed(){
            return getSenderQueueSize() > 0 && use_send_queues;
        }

        protected String getSockAddress() {
            StringBuilder sb=new StringBuilder();
            if(sock != null) {
                sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
                sb.append(" - ").append(sock.getInetAddress().getHostAddress()).append(':').append(sock.getPort());
            }
            return sb.toString();
        }

        protected void updateLastAccessed() {
            if(conn_expire_time > 0)
                last_access=getTimestamp();
        }

        /** Called after {@link TCPConnection#TCPConnection(org.jgroups.Address)} */
        protected void connect(SocketAddress destAddr) throws Exception {
            try {
                if(!defer_client_binding)
                    this.sock.bind(new InetSocketAddress(client_bind_addr, client_bind_port));
                if(this.sock.getLocalSocketAddress() != null && this.sock.getLocalSocketAddress().equals(destAddr))
                    throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
                Util.connect(this.sock, destAddr, sock_conn_timeout);
                this.out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
                this.in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
                sendLocalAddress(getLocalAddress());
            }
            catch(Exception t) {
                socket_factory.close(this.sock);
                throw t;
            }
        }


        protected TCPConnection start(ThreadFactory f) {
            if(receiver != null)
                receiver.stop();
            receiver=new Receiver(f).start();

            if(isSenderUsed()) {
                if(sender != null)
                    sender.stop();
                sender=new Sender(f, getSenderQueueSize()).start();
            }
            return this;
        }
        


        /**
         * 
         * @param data Guaranteed to be non null
         * @param offset
         * @param length
         */
        protected void send(byte[] data, int offset, int length) throws Exception {
            if (sender != null) {
                // we need to copy the byte[] buffer here because the original buffer might get changed meanwhile
                byte[] tmp = new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                sender.addToQueue(tmp);
            }
            else
                _send(data, offset, length, true, true);
        }

        /**
         * Sends data using the 'out' output stream of the socket
         * 
         * @param data
         * @param offset
         * @param length
         * @param acquire_lock
         * @throws Exception 
         */
        protected void _send(byte[] data, int offset, int length, boolean acquire_lock, boolean flush) throws Exception {
            if(acquire_lock)
                send_lock.lock();
            try {
                doSend(data, offset, length, acquire_lock, flush);
                updateLastAccessed();
            }
            catch(InterruptedException iex) {
                Thread.currentThread().interrupt(); // set interrupt flag again
            }        
            finally {
                if(acquire_lock)
                    send_lock.unlock();
            }
        }

        protected void doSend(byte[] data, int offset, int length, boolean acquire_lock, boolean flush) throws Exception {
            out.writeInt(length); // write the length of the data buffer first
            out.write(data,offset,length);
            if(!flush || (acquire_lock && send_lock.hasQueuedThreads()))
                return; // don't flush as some of the waiting threads will do the flush, or flush is false
            out.flush(); // may not be very efficient (but safe)
        }

        protected void flush() throws Exception {
            if(out != null)
                out.flush();
        }

        /**
         * Reads the peer's address. First a cookie has to be sent which has to
         * match my own cookie, otherwise the connection will be refused
         */
        protected Address readPeerAddress(Socket client_sock) throws Exception {                    
            int timeout=client_sock.getSoTimeout();
            client_sock.setSoTimeout(peer_addr_read_timeout);

            try {              
                // read the cookie first
                byte[] input_cookie=new byte[cookie.length];
                in.readFully(input_cookie, 0, input_cookie.length);
                if(!matchCookie(input_cookie))
                    throw new SocketException("ConnectionMap.Connection.readPeerAddress(): cookie read by " + getLocalAddress()
                                              + " does not match own cookie; terminating connection");
                // then read the version
                short version=in.readShort();
                if(!Version.isBinaryCompatible(version))
                    throw new IOException("packet from " + client_sock.getInetAddress() + ":" + client_sock.getPort() +
                                            " has different version (" + Version.print(version) +
                                            ") from ours (" + Version.printVersion() + "); discarding it");
                Address client_peer_addr=new IpAddress();
                client_peer_addr.readFrom(in);
                updateLastAccessed();
                return client_peer_addr;
            }
            finally {
                client_sock.setSoTimeout(timeout);
            }
        }
        
        /**
         * Send the cookie first, then the our port number. If the cookie
         * doesn't match the receiver's cookie, the receiver will reject the
         * connection and close it.
         * 
         * @throws Exception
         */
        protected void sendLocalAddress(Address local_addr) throws Exception {
            // write the cookie
            out.write(cookie, 0, cookie.length);

            // write the version
            out.writeShort(Version.version);
            local_addr.writeTo(out);
            out.flush(); // needed ?
            updateLastAccessed();
        }       

        protected boolean matchCookie(byte[] input) {
            if(input == null || input.length < cookie.length) return false;
            for(int i=0; i < cookie.length; i++)
                if(cookie[i] != input[i]) return false;
            return true;
        }
        
        protected class Receiver implements Runnable {
            protected final Thread     recv;
            protected volatile boolean receiving=true;

            public Receiver(ThreadFactory f) {
                recv = f.newThread(this,"Connection.Receiver [" + getSockAddress() + "]");                
            }


            public Receiver start() {
                receiving=true;
                recv.start();
                return this;
            }

            public Receiver stop() {
                receiving=false;
                recv.interrupt();
                return this;
            }
            
            public boolean isRunning() {
                return receiving;
            }
            
            public boolean canRun() {
                return isRunning() && isConnected();
            }

            public void run() {
                try {
                    while(!Thread.currentThread().isInterrupted() && canRun()) {
                        try {                    
                            int len=in.readInt();
                            byte[] buf=new byte[len];
                            in.readFully(buf, 0, len);
                            updateLastAccessed();
                            TCPConnectionMap.this.recvr.receive(peer_addr,buf,0,len);
                        }
                        catch(OutOfMemoryError mem_ex) {
                            break; // continue;
                        }
                        catch(IOException io_ex) {
                            break;
                        }
                        catch(Throwable e) {
                        }
                    }
                }
                finally {
                    mapper.removeConnectionIfPresent(peer_addr,TCPConnection.this);
                }
            }
        }
        
        protected class Sender implements Runnable {
            protected final BlockingQueue<byte[]> send_queue;
            protected final Thread                runner;
            protected volatile boolean            started=true;


            public Sender(ThreadFactory tf, int send_queue_size) {
                this.runner=tf.newThread(this, "Connection.Sender [" + getSockAddress() + "]");
                this.send_queue=new LinkedBlockingQueue<byte[]>(send_queue_size);
            }
            
            public void addToQueue(byte[] data) throws Exception{
                if(canRun())
                    if (!send_queue.offer(data, sock_conn_timeout, TimeUnit.MILLISECONDS))
                        log.warn("Discarding message because TCP send_queue is full and hasn't been releasing for " + sock_conn_timeout + " ms");
            }

            public Sender start() {
                started=true;
                runner.start();
                return this;
            }

            public Sender stop() {
                started=false;
                runner.interrupt();
                return this;
            }
            
            public boolean isRunning() {
                return started;
            }
            
            public boolean canRun() {
                return isRunning() && isConnected();
            }

            public void run() {
                try {
                    while(!Thread.currentThread().isInterrupted() && canRun()) {
                        byte[] data=null;
                        try {
                            data=send_queue.take();
                        }
                        catch(InterruptedException e) {
                            // Thread.currentThread().interrupt();
                            break;
                        }
    
                        if(data != null) {                        
                            try {
                                _send(data, 0, data.length, false, send_queue.isEmpty());
                            }
                            catch(Throwable ignored) {
                            }
                        }
                    }    
                }
                finally {
                    mapper.removeConnectionIfPresent(peer_addr, TCPConnection.this);
                }                
            }
        }

        public String toString() {
            StringBuilder ret=new StringBuilder();
            InetAddress local=null, remote=null;
            String local_str, remote_str;

            Socket tmp_sock=sock;
            if(tmp_sock == null)
                ret.append("<null socket>");
            else {
                //since the sock variable gets set to null we want to make
                //make sure we make it through here without a nullpointer exception               
                local=tmp_sock.getLocalAddress();
                remote=tmp_sock.getInetAddress();
                local_str=local != null? Util.shortName(local) : "<null>";
                remote_str=remote != null? Util.shortName(remote) : "<null>";
                ret.append('<' + local_str
                           + ':'
                           + tmp_sock.getLocalPort()
                           + " --> "
                           + remote_str
                           + ':'
                           + tmp_sock.getPort()
                           + "> ("
                           + TimeUnit.SECONDS.convert(getTimestamp() - last_access, TimeUnit.NANOSECONDS)
                           + " secs old) [" + (isOpen()? "open]" : "closed]"));
            }
            tmp_sock=null;

            return ret.toString();
        }

        public boolean isExpired(long now) {
            return conn_expire_time > 0 && now - last_access >= conn_expire_time;
        }
        
        public boolean isConnected() {
            return !sock.isClosed() && sock.isConnected();
        }

        public boolean isOpen() {
            return isConnected()
                            && (!isSenderUsed() || sender.isRunning())
                            && (receiver != null && receiver.isRunning());
        }

        public void close() throws IOException {
            // can close even if start was never called...
            send_lock.lock();
            try {
                if(receiver != null) {
                    receiver.stop();
                    receiver=null;
                }
                if(sender != null) {
                    sender.stop();
                    sender=null;
                }
                try {
                    socket_factory.close(sock);
                }
                catch(Throwable t) {}
                Util.close(out);
                Util.close(in);
            }
            finally {
                send_lock.unlock();
            }
            mapper.notifyConnectionClosed(peer_addr);
        }
    }
    
    protected class Mapper extends AbstractConnectionMap<TCPConnection> {

        public Mapper(ThreadFactory factory) {
            super(factory);            
        }
        
        public Mapper(ThreadFactory factory,long reaper_interval) {
            super(factory,reaper_interval);            
        }

        public TCPConnection getConnection(Address dest) throws Exception {
            TCPConnection conn;
            getLock().lock();
            try {
                if((conn=conns.get(dest)) != null && conn.isOpen()) // keep FAST path on the most common case
                    return conn;
            }
            finally {
                getLock().unlock();
            }

            Exception connect_exception=null; // set if connect() throws an exception
            sock_creation_lock.lockInterruptibly();
            try {
                // lock / release, create new conn under sock_creation_lock, it can be skipped but then it takes
                // extra check in conn map and closing the new connection, w/ sock_creation_lock it looks much simpler
                // (slow path, so not important)

                getLock().lock();
                try {
                    conn=conns.get(dest); // check again after obtaining sock_creation_lock
                    if(conn != null && conn.isOpen())
                        return conn;

                    // create conn stub
                    conn=new TCPConnection(dest);
                    addConnection(dest, conn);
                }
                finally {
                    getLock().unlock();
                }

                // now connect to dest:
                try {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": connecting to " + dest);
                    conn.connect(new InetSocketAddress(((IpAddress)dest).getIpAddress(),((IpAddress)dest).getPort()));
                    conn.start(getThreadFactory());
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": connected to " + dest);

                }
                catch(Exception connect_ex) {
                    connect_exception=connect_ex;
                }

                getLock().lock();
                try {
                    TCPConnection existing_conn=conns.get(dest); // check again after obtaining sock_creation_lock
                    if(existing_conn != null && existing_conn.isOpen() // added by a successful accept()
                      && existing_conn != conn) {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": found existing connection to " + dest +
                                        ", using it and deleting own conn-stub");
                        Util.close(conn); // close our connection; not really needed as conn was closed by accept()
                        return existing_conn;
                    }

                    if(connect_exception != null) {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": failed connecting to " + dest + ": " + connect_exception);
                        removeConnectionIfPresent(dest, conn); // removes and closes the conn
                        throw connect_exception;
                    }
                    return conn;
                }
                finally {
                    getLock().unlock();
                }
            }
            finally {
                sock_creation_lock.unlock();
            }
        }


        public boolean connectionEstablishedTo(Address address) {
            lock.lock();
            try {
                TCPConnection conn=conns.get(address);
                return conn != null && conn.isConnected();
            }
            finally {
                lock.unlock();
            }
        }

        public int size() {return conns.size();}

        public String toString() {
            StringBuilder sb=new StringBuilder();

            getLock().lock();
            try {
                for(Map.Entry<Address,TCPConnection> entry: conns.entrySet()) {
                    sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
                }
                return sb.toString();
            }
            finally {
                getLock().unlock();
            }
        }
    }
}


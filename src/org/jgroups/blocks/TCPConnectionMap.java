package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TCPConnectionMap{

    protected final Mapper        mapper;
    protected final InetAddress   bind_addr;
    protected InetAddress         client_bind_addr;
    protected int                 client_bind_port;
    protected boolean 			  defer_client_bind_addr;
    protected final Address       local_addr; // bind_addr + port of srv_sock
    protected final ServerSocket  srv_sock;
    protected Receiver            receiver;
    protected final long          conn_expire_time;
    protected final Log           log=LogFactory.getLog(getClass());
    protected int                 recv_buf_size=120000;
    protected int                 send_buf_size=60000;
    protected int                 send_queue_size = 0;
    protected int                 sock_conn_timeout=1000; // max time in millis to wait for Socket.connect() to return    
    protected boolean             tcp_nodelay=false;
    protected int                 linger=-1;    
    protected final Thread        acceptor;
    protected final AtomicBoolean running = new AtomicBoolean(false);
    protected volatile boolean    use_send_queues=false;
    protected SocketFactory       socket_factory=new DefaultSocketFactory();


    public TCPConnectionMap(String service_name,
                            ThreadFactory f,
                            SocketFactory socket_factory,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int external_port,
                            int srv_port,
                            int max_port,
                            ThreadGroup group
                            ) throws Exception {
        this(service_name, f,socket_factory, r,bind_addr,false,external_addr,external_port, srv_port,max_port,0,0, group);
    }
    
    public TCPConnectionMap(String service_name,
            ThreadFactory f,
            SocketFactory socket_factory,
            Receiver r,
            InetAddress bind_addr,
            boolean defer_client_bind_addr,
            InetAddress external_addr,
            int external_port,
            int srv_port,
            int max_port,
            ThreadGroup group
            ) throws Exception {
    	this(service_name, f,socket_factory, r,bind_addr,defer_client_bind_addr,external_addr,external_port, srv_port,max_port,0,0, group);
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
                            long conn_expire_time,
                            ThreadGroup group
                            ) throws Exception {
        this(service_name, f, null, r, bind_addr, false, external_addr, external_port, srv_port, max_port, reaper_interval, conn_expire_time, group);
    }

    public TCPConnectionMap(String service_name,
                            ThreadFactory f,
                            SocketFactory socket_factory,
                            Receiver r,
                            InetAddress bind_addr,
                            boolean defer_client_bind_addr,
                            InetAddress external_addr,
                            int external_port,
                            int srv_port,
                            int max_port,
                            long reaper_interval,
                            long conn_expire_time,
                            ThreadGroup group
                            ) throws Exception {
        this.mapper = new Mapper(f,reaper_interval);
        this.receiver=r;
        this.bind_addr=bind_addr;
        this.defer_client_bind_addr = defer_client_bind_addr;
        this.conn_expire_time = conn_expire_time;
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

        acceptor=f.newThread(new ConnectionAcceptor(),"ConnectionMap.Acceptor");
    }
    
    public Address          getLocalAddress()                       {return local_addr;}
    public Receiver         getReceiver()                           {return receiver;}
    public void             setReceiver(Receiver receiver)          {this.receiver=receiver;}
    public SocketFactory    getSocketFactory()                      {return socket_factory;}
    public void             setSocketFactory(SocketFactory factory) {this.socket_factory=factory;}
    public InetAddress      clientBindAddress()                     {return client_bind_addr;}
    public TCPConnectionMap clientBindAddress(InetAddress addr)     {this.client_bind_addr=addr; return this;}
    public int              clientBindPort()                        {return client_bind_port;}
    public TCPConnectionMap clientBindPort(int port)                {this.client_bind_port=port; return this;}

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
        receiver.receive(sender, data, offset, length);
    }

    public void send(Address dest, byte[] data, int offset, int length) throws Exception {        
        if(dest == null) {
            if(log.isErrorEnabled())
                log.error("destination is null");
            return;
        }

        if(data == null) {
            log.warn("data is null; discarding packet");
            return;
        }      
        
        if(!running.get() ) {
            if(log.isDebugEnabled())
                log.debug("connection table is not running, discarding message to " + dest);
            return;
        }

        if(dest.equals(local_addr)) {
            receive(local_addr, data, offset, length);
            return;
        }

        // 1. Try to obtain correct Connection (or create one if not yet existent)
        TCPConnection conn;
        conn=mapper.getConnection(dest);           

        // 2. Send the message using that connection
        if(conn != null) {
            try {
                conn.send(data, offset, length);
            }
            catch(Exception ex) {
                mapper.removeConnection(dest);
                throw ex;
            }
        }
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


    
    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            client_sock.setSendBufferSize(send_buf_size);
        }
        catch(IllegalArgumentException ex) {
            if(log.isErrorEnabled())
                log.error("exception setting send buffer size to " + send_buf_size + " bytes", ex);
        }
        try {
            client_sock.setReceiveBufferSize(recv_buf_size);
        }
        catch(IllegalArgumentException ex) {
            if(log.isErrorEnabled())
                log.error("exception setting receive buffer size to " + send_buf_size + " bytes", ex);
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

    protected class ConnectionAcceptor implements Runnable {

        /**
         * Acceptor thread. Continuously accept new connections. Create a new
         * thread for each new connection and put it in conns. When the thread
         * should stop, it is interrupted by the thread creator.
         */
        public void run() {
            while(!srv_sock.isClosed() && !Thread.currentThread().isInterrupted()) {
                TCPConnection conn=null;
                Socket client_sock = null;
                try {
                    client_sock=srv_sock.accept();                      
                    conn=new TCPConnection(client_sock);
                    Address peer_addr=conn.getPeerAddress();
                    mapper.getLock().lock();
                    try {                        
                        boolean currentConnectionOpen=mapper.hasOpenConnection(peer_addr);
                        boolean replaceWithNewConnection=false;
                        if(currentConnectionOpen) {
                            replaceWithNewConnection=peer_addr.compareTo(local_addr) > 0;
                        }
                        if(!currentConnectionOpen || replaceWithNewConnection) {
                            mapper.removeConnection(peer_addr);
                            mapper.addConnection(peer_addr, conn);
                            conn.start(mapper.getThreadFactory()); // starts handler thread on this socket
                        }
                        else {
                            Util.close(conn);
                        }
                    }
                    finally {
                        mapper.getLock().unlock();
                    }
                }
                catch(SocketException se){
                    boolean threadExiting=srv_sock.isClosed() || Thread.currentThread().isInterrupted();
                    if(threadExiting) {
                        break;
                    }
                    else {
                        if(log.isWarnEnabled())
                            log.warn(Util.getMessage("AcceptError", se));
                        Util.close(conn);
                        Util.close(client_sock);
                    }                                            
                }
                catch(Exception ex) {
                    if(log.isWarnEnabled())
                        log.warn(Util.getMessage("AcceptError", ex));
                    Util.close(conn);
                    Util.close(client_sock);
                }
            }
            if(log.isTraceEnabled())
                log.trace(Thread.currentThread().getName() + " terminated");
        }
    }

    public void setReceiveBufferSize(int recv_buf_size) {
       this.recv_buf_size = recv_buf_size;
    }

    public void setSocketConnectionTimeout(int sock_conn_timeout) {
       this.sock_conn_timeout = sock_conn_timeout;
    }

    public void setSendBufferSize(int send_buf_size) {
       this.send_buf_size = send_buf_size;
    }

    public void setLinger(int linger) {
       this.linger = linger;
    }

    public void setTcpNodelay(boolean tcp_nodelay) {
        this.tcp_nodelay = tcp_nodelay;
    }
    
    public void setSendQueueSize(int send_queue_size) {
        this.send_queue_size = send_queue_size;        
    }
    
    public void setUseSendQueues(boolean use_send_queues) {
        this.use_send_queues=use_send_queues;       
    }   
    
    public int getNumConnections() {
        return mapper.getNumConnections();
    }

    public boolean connectionEstablishedTo(Address addr) {
        return mapper.connectionEstablishedTo(addr);
    }

    public String printConnections() {
        return mapper.printConnections();
    }
    
    public void retainAll(Collection<Address> members) {
        mapper.retainAll(members);
     }  
    
    public long getConnectionExpiryTimeout() {
        return conn_expire_time;
    }

    public int getSenderQueueSize() {
        return send_queue_size;
    }

    public String toString() {
        StringBuilder ret=new StringBuilder();
        ret.append("local_addr=" + local_addr).append("\n");
        ret.append("connections (" + mapper.size() + "):\n");
        ret.append(mapper.toString());
        ret.append('\n');
        return ret.toString();
    }
    
    public class TCPConnection implements Connection {
        protected final Socket sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
        protected final Lock send_lock=new ReentrantLock(); // serialize send()        
        protected final Log log=LogFactory.getLog(getClass());        
        protected final byte[] cookie= { 'b', 'e', 'l', 'a' };  
        protected final DataOutputStream out;
        protected final DataInputStream in;    
        protected final Address peer_addr; // address of the 'other end' of the connection
        protected final int peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address
        protected long last_access=System.currentTimeMillis(); // last time a message was sent or received           
        protected Sender sender;
        protected ConnectionPeerReceiver connectionPeerReceiver;
        protected final AtomicBoolean active=new AtomicBoolean(false);

        TCPConnection(Address peer_addr) throws Exception {
            if(peer_addr == null)
                throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);           
            SocketAddress destAddr=new InetSocketAddress(((IpAddress)peer_addr).getIpAddress(),((IpAddress)peer_addr).getPort());
            this.sock=socket_factory.createSocket("jgroups.tcp.sock");
            try {
            	if (!defer_client_bind_addr)
            		this.sock.bind(new InetSocketAddress(client_bind_addr, client_bind_port));
                if(this.sock.getLocalSocketAddress().equals(destAddr))
                    throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
                Util.connect(this.sock, destAddr, sock_conn_timeout);
            }
            catch(Exception t) {
                socket_factory.close(this.sock);
                throw t;
            }
            setSocketParameters(sock);
            this.out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            this.in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            sendLocalAddress(getLocalAddress());
            this.peer_addr=peer_addr;
        }

        TCPConnection(Socket s) throws Exception {
            if(s == null)
                throw new IllegalArgumentException("Invalid parameter s=" + s);                       
            setSocketParameters(s);
            this.out=new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
            this.in=new DataInputStream(new BufferedInputStream(s.getInputStream()));
            this.peer_addr=readPeerAddress(s);            
            this.sock=s;
        }  
        
        protected Address getPeerAddress() {
            return peer_addr;
        }

        protected void updateLastAccessed() {
            last_access=System.currentTimeMillis();
        }

        protected void start(ThreadFactory f) {
            //only start once....
            if(active.compareAndSet(false, true)) {
                connectionPeerReceiver = new ConnectionPeerReceiver(f);            
                connectionPeerReceiver.start();
                
                if(isSenderUsed()) {
                    sender = new Sender(f,getSenderQueueSize());   
                    sender.start();                            
                }
            }
        }
        
        protected boolean isSenderUsed(){
            return getSenderQueueSize() > 0 && use_send_queues;
        }

        protected String getSockAddress() {
            StringBuilder sb=new StringBuilder();
            if(sock != null) {
                sb.append(sock.getLocalAddress().getHostAddress())
                  .append(':')
                  .append(sock.getLocalPort());
                sb.append(" - ")
                  .append(sock.getInetAddress().getHostAddress())
                  .append(':')
                  .append(sock.getPort());

            }
            return sb.toString();
        }

        /**
         * 
         * @param data
         *                Guaranteed to be non null
         * @param offset
         * @param length
         */
        protected void send(byte[] data, int offset, int length) throws Exception {
            if (isSenderUsed()) {
                // we need to copy the byte[] buffer here because the original buffer might get
                // changed meanwhile
                byte[] tmp = new byte[length];
                System.arraycopy(data, offset, tmp, 0, length);
                sender.addToQueue(tmp);
            } else {
                _send(data, offset, length, true);
            }
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
        protected void _send(byte[] data, int offset, int length, boolean acquire_lock) throws Exception {
            if(acquire_lock)
                send_lock.lock();

            try {
                doSend(data, offset, length);
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

        protected void doSend(byte[] data, int offset, int length) throws Exception {
            out.writeInt(length); // write the length of the data buffer first
            out.write(data, offset, length);
            out.flush(); // may not be very efficient (but safe)           
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
        
        protected class ConnectionPeerReceiver implements Runnable {
            protected final Thread recv;
            protected final AtomicBoolean receiving = new AtomicBoolean(false);


            public ConnectionPeerReceiver(ThreadFactory f) {
                recv = f.newThread(this,"Connection.Receiver [" + getSockAddress() + "]");                
            }


            public void start() {
                if(receiving.compareAndSet(false, true)) {
                    recv.start();
                }
            }

            public void stop() {
                if(receiving.compareAndSet(true, false)) {
                    recv.interrupt();
                }
            }
            
            public boolean isRunning() {
                return receiving.get();
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
                            receiver.receive(peer_addr, buf, 0, len);
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
                finally{
                    Util.close(TCPConnection.this);
                }
            }
        }
        
        protected class Sender implements Runnable {

            final BlockingQueue<byte[]> send_queue;
            final Thread runner;
            protected final AtomicBoolean running= new AtomicBoolean(false);

            public Sender(ThreadFactory tf,int send_queue_size) {
                this.runner=tf.newThread(this, "Connection.Sender [" + getSockAddress() + "]");
                this.send_queue=new LinkedBlockingQueue<byte[]>(send_queue_size);
            }
            
            public void addToQueue(byte[] data) throws Exception{
                if(canRun())
                    send_queue.put(data);
            }

            public void start() {
                if(running.compareAndSet(false, true)) {
                    runner.start();
                }
            }

            public void stop() {
                if(running.compareAndSet(true, false)) {
                    runner.interrupt();
                }
            }
            
            public boolean isRunning() {
                return running.get();
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
                                _send(data, 0, data.length, false);
                            }
                            catch(Throwable ignored) {
                            }
                        }
                    }    
                }
                finally {
                    Util.close(TCPConnection.this);    
                }                
                if(log.isTraceEnabled())
                    log.trace("TCPConnection.Sender thread terminated at " + local_addr);
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
                           + ((System.currentTimeMillis() - last_access) / 1000)
                           + " secs old)");
            }
            tmp_sock=null;

            return ret.toString();
        }      

        public boolean isExpired(long now) {
            return getConnectionExpiryTimeout() > 0 && now - last_access >= getConnectionExpiryTimeout();
        }
        
        public boolean isConnected() {
            return !sock.isClosed() && sock.isConnected();
        }

        public boolean isOpen() {
            return isConnected()
                            && (!isSenderUsed() || sender.isRunning())
                            && (connectionPeerReceiver != null && connectionPeerReceiver.isRunning());
        }

        public void close() throws IOException {
            //can close even if start was never called...
            send_lock.lock();
            try {
                connectionPeerReceiver.stop();
                if (isSenderUsed()) {
                    sender.stop();
                }
                try {
                    socket_factory.close(sock);
                }
                catch(Throwable t) {}
                Util.close(out);
                Util.close(in);
            } finally {
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

        protected TCPConnection getConnectionUnderLock(Address dest) throws Exception {
            TCPConnection conn;
            getLock().lock();
            try {
                conn=conns.get(dest);
            }
            finally {
                getLock().unlock();
            }
            if(conn != null && conn.isOpen()) // conn.isOpen should not be under the lock
                return conn;
            return null;
        }

        public TCPConnection getConnection(Address dest) throws Exception { //S. Simeonoff
            TCPConnection conn=getConnectionUnderLock(dest); // keep FAST path on the most common case
            if(conn != null)
                return conn;

            sock_creation_lock.lockInterruptibly();
            try {
                // lock / release, create new conn under sock_creation_lock, it can be skipped but then it takes
                // extra check in conn map and closing the new connection, w/ sock_creation_lock it looks much simpler
                // (slow path,so not important)

                conn=getConnectionUnderLock(dest); // check again after obtaining sock_creation_lock
                if(conn != null)
                    return conn;

                conn=new TCPConnection(dest);
                conn.start(getThreadFactory());
                addConnection(dest,conn); // listener notification should not be under the getLock() either
                if(log.isTraceEnabled())
                    log.trace("created socket to " + dest);

            }
            catch(Exception ex) {
                if(log.isTraceEnabled())
                    log.trace("failed creating connection to " + dest);
                if(conn != null) { // should not happen, either conn.start or addConnection failed -still make sure
                    // it's a "proper rollback"
                    TCPConnection existing;
                    getLock().lock();
                    try {
                        existing=conns.get(dest);
                        if(existing == null)
                            removeConnection(dest);
                    }
                    finally {
                        getLock().unlock();
                    }

                    Util.close(conn);
                    conn=existing;
                }
            }
            finally {
                sock_creation_lock.unlock();
            }
            return conn;
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


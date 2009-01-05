package org.jgroups.blocks;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.PortsManager;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

public class TCPConnectionMap{

    private final Mapper mapper;
    private final InetAddress bind_addr;
    private final Address local_addr; // bind_addr + port of srv_sock
    private final ThreadGroup thread_group=new ThreadGroup(Util.getGlobalThreadGroup(),"ConnectionTable");     
    private final ServerSocket srv_sock;
    private final Receiver receiver;
    private final long conn_expire_time;
    private final Log log=LogFactory.getLog(getClass());
    private int recv_buf_size=120000;
    private int send_buf_size=60000;
    private int send_queue_size = 0;
    private int sock_conn_timeout=1000; // max time in millis to wait for Socket.connect() to return    
    private boolean tcp_nodelay=false;
    private int linger=-1;    
    private final Thread acceptor;
    private PortsManager pm=null;       

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean use_send_queues=false;    

    public TCPConnectionMap(ThreadFactory f,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int srv_port,
                            int max_port,
                            PortsManager pm) throws Exception {
        this(f,r,bind_addr,external_addr,srv_port,max_port,0,0,pm);
    }

    public TCPConnectionMap(ThreadFactory f,
                            Receiver r,
                            InetAddress bind_addr,
                            InetAddress external_addr,
                            int srv_port,
                            int max_port,
                            long reaper_interval,
                            long conn_expire_time,
                            PortsManager pm) throws Exception {
        this.mapper = new Mapper(f,reaper_interval);
        this.receiver=r;
        this.bind_addr=bind_addr;               
        this.pm=pm;   
        this.conn_expire_time = conn_expire_time;
        this.srv_sock=createServerSocket(srv_port, max_port);      

        if(external_addr != null)
            local_addr=new IpAddress(external_addr, srv_sock.getLocalPort());
        else if(bind_addr != null)
            local_addr=new IpAddress(bind_addr, srv_sock.getLocalPort());
        else
            local_addr=new IpAddress(srv_sock.getLocalPort());
        
        acceptor=f.newThread(thread_group, new ConnectionAcceptor(),"ConnectionTable.Acceptor");
    }    
    
    public Address getLocalAddress() {       
        return local_addr;
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
        try {
            conn=mapper.getConnection(dest);           
        }
        catch(Throwable ex) {
            throw new Exception("connection to " + dest + " could not be established", ex);
        }

        // 2. Send the message using that connection
        try {
            conn.send(data, offset, length);
        }
        catch(Exception ex) {            
            mapper.removeConnection(dest);
            throw ex;
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
            if(pm != null) {
                pm.updatePort(srv_sock.getLocalPort());
            }
            Util.close(srv_sock);
            Util.interruptAndWaitToDie(acceptor);
            mapper.stop();
        }
    }

    /**
     * Finds first available port starting at start_port and returns server
     * socket. Will not bind to port >end_port. Sets srv_port
     */
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
                    ret=new ServerSocket(start_port, 20, null);
                }
            }
            catch(BindException bind_ex) {
                if(start_port == end_port)
                    throw new BindException("No available port to bind to");
                if(bind_addr != null) {
                    NetworkInterface nic=NetworkInterface.getByInetAddress(bind_addr);
                    if(nic == null)
                    	log.warn("bind_addr " + bind_addr + " is not a valid interface: " + bind_ex);
                }
                start_port++;
                continue;
            }
            catch(IOException io_ex) {
                if(log.isErrorEnabled())
                    log.error("exception is " + io_ex);
            }           
            break;
        }
        return ret;
    }
    
    private void setSocketParameters(Socket client_sock) throws SocketException {
        if(log.isTraceEnabled())
            log.trace("[" + local_addr
                      + "] accepted connection from "
                      + client_sock.getInetAddress()
                      + ":"
                      + client_sock.getPort());
        try {
            client_sock.setSendBufferSize(send_buf_size);
        }
        catch(IllegalArgumentException ex) {
            if(log.isErrorEnabled())
                log.error("exception setting send buffer size to " + send_buf_size + " bytes",
                          ex);
        }
        try {
            client_sock.setReceiveBufferSize(recv_buf_size);
        }
        catch(IllegalArgumentException ex) {
            if(log.isErrorEnabled())
                log.error("exception setting receive buffer size to " + send_buf_size
                          + " bytes", ex);
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

    private class ConnectionAcceptor implements Runnable {

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
                            log.warn("Could not accept connection from peer ", se);
                        Util.close(conn);
                        Util.close(client_sock);
                    }                                            
                }
                catch(Exception ex) {
                    if(log.isWarnEnabled())
                        log.warn("Could not read accept connection from peer " + ex);
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
    
    public void retainAll(Collection<Address> members) {
        mapper.retainAll(members);
     }  
    
    public long getConnectionExpiryTimeout() {
        return conn_expire_time;
    }

    public int getSenderQueueSize() {
        return send_queue_size;
    }
    
    private class TCPConnection implements Connection {

        private final Socket sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
        private final Lock send_lock=new ReentrantLock(); // serialize send()        
        private final Log log=LogFactory.getLog(getClass());        
        private final byte[] cookie= { 'b', 'e', 'l', 'a' };  
        private final DataOutputStream out;
        private final DataInputStream in;    
        private final Address peer_addr; // address of the 'other end' of the connection
        private final int peer_addr_read_timeout=2000; // max time in milliseconds to block on reading peer address
        private long last_access=System.currentTimeMillis(); // last time a message was sent or received           
        private Sender sender;

        TCPConnection(Address peer_addr) throws Exception {
            if(peer_addr == null)
                throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);           
            SocketAddress destAddr=new InetSocketAddress(((IpAddress)peer_addr).getIpAddress(),((IpAddress)peer_addr).getPort());
            this.sock=new Socket();
            this.sock.bind(new InetSocketAddress(bind_addr, 0));
            this.sock.connect(destAddr, sock_conn_timeout);
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
        
        private Address getPeerAddress() {
            return peer_addr;
        }

        private void updateLastAccessed() {
            last_access=System.currentTimeMillis();
        }

        private void start(ThreadFactory f) {

            Thread t=f.newThread(new ConnectionPeerReceiver(),"Connection.Receiver [" + getSockAddress() + "]");
            t.start();

            if(isSenderUsed()) {
                sender = new Sender(f,getSenderQueueSize());   
                sender.start();                            
            }
        }
        
        private boolean isSenderUsed(){
            return getSenderQueueSize() > 0 && use_send_queues;
        }

        private String getSockAddress() {
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
        private void send(byte[] data, int offset, int length) throws Exception{
            if(isSenderUsed()) {
                try {
                    // we need to copy the byte[] buffer here because the original buffer might get changed meanwhile
                    byte[] tmp=new byte[length];
                    System.arraycopy(data, offset, tmp, 0, length);
                    sender.getQueue().put(tmp);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            else
                _send(data, offset, length, true);           
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
        private void _send(byte[] data, int offset, int length, boolean acquire_lock) throws Exception {
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

        private void doSend(byte[] data, int offset, int length) throws Exception {
            // we're using 'double-writes', sending the buffer to the destination in 2 pieces. this would
            // ensure that, if the peer closed the connection while we were idle, we would get an exception.
            // this won't happen if we use a single write (see Stevens, ch. 5.13).

            out.writeInt(length); // write the length of the data buffer first
            Util.doubleWrite(data, offset, length, out);
            out.flush(); // may not be very efficient (but safe)           
        }

        /**
         * Reads the peer's address. First a cookie has to be sent which has to
         * match my own cookie, otherwise the connection will be refused
         */
        private Address readPeerAddress(Socket client_sock) throws Exception {                    
            int timeout=client_sock.getSoTimeout();
            client_sock.setSoTimeout(peer_addr_read_timeout);

            try {              
                // read the cookie first
                byte[] input_cookie=new byte[cookie.length];
                in.read(input_cookie, 0, input_cookie.length);
                if(!matchCookie(input_cookie))
                    throw new SocketException("ConnectionTable.Connection.readPeerAddress(): cookie read by " + getLocalAddress()
                                              + " does not match own cookie; terminating connection");
                // then read the version
                short version=in.readShort();

                if(!Version.isBinaryCompatible(version) ) {
                    if(log.isWarnEnabled())
                        log.warn(new StringBuilder("packet from ").append(client_sock.getInetAddress())
                                                                  .append(':')
                                                                  .append(client_sock.getPort())
                                                                  .append(" has different version (")
                                                                  .append(Version.print(version))
                                                                  .append(") from ours (")
                                                                  .append(Version.printVersion())
                                                                  .append("). This may cause problems"));
                }
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
         * @throws IOException
         */
        private void sendLocalAddress(Address local_addr) throws IOException {           
            // write the cookie
            out.write(cookie, 0, cookie.length);

            // write the version
            out.writeShort(Version.version);
            local_addr.writeTo(out);
            out.flush(); // needed ?
            updateLastAccessed();
        }       

        private boolean matchCookie(byte[] input) {
            if(input == null || input.length < cookie.length) return false;
            for(int i=0; i < cookie.length; i++)
                if(cookie[i] != input[i]) return false;
            return true;
        }
        
        private class ConnectionPeerReceiver implements Runnable {
            
            public void run() {
                byte[] buf=new byte[256]; // start with 256, increase as we go
                int len=0;

                while(isOpen()) {
                    try {                    
                        len=in.readInt();
                        if(len > buf.length)
                            buf=new byte[len];
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
                Util.close(TCPConnection.this);                 
            }
        }
        
        private class Sender implements Runnable {

            final BlockingQueue<byte[]> send_queue;
            final Thread runner;

            public Sender(ThreadFactory tf,int send_queue_size) {
                this.runner=tf.newThread(this, "Connection.Sender [" + getSockAddress() + "]");
                this.send_queue=new LinkedBlockingQueue<byte[]>(send_queue_size);
            }

            public BlockingQueue<byte[]> getQueue() {
                return send_queue;
            }

            public void start() {
                runner.start();
            }

            public void stop() {
                runner.interrupt();
            }

            public void run() {
                while(!Thread.currentThread().isInterrupted() && isOpen()) {
                    byte[] data=null;
                    try {
                        data=send_queue.take();
                    }
                    catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if(data != null) {                        
                        try {
                            _send(data, 0, data.length, false);
                        }
                        catch(Exception ignored) {
                        }
                    }
                }
                if(log.isTraceEnabled())
                    log.trace("ConnectionTable.Connection.Sender thread terminated");
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
            if(getConnectionExpiryTimeout() > 0) {
                return now - last_access >= getConnectionExpiryTimeout();
            }
            else {
                return false;
            }
        }

        public boolean isOpen() {
            return !sock.isClosed() && sock.isConnected();
        }

        public void close() throws IOException {          
            send_lock.lock();
            try {
                Util.close(sock);
                Util.close(out);
                Util.close(in);
                if(isSenderUsed()){
                    sender.stop();
                }
            }            
            finally {
                send_lock.unlock();                
            }            
            mapper.notifyConnectionClosed(peer_addr);
        }
    }
    
    private class Mapper extends AbstractConnectionMap<TCPConnection>{

        public Mapper(ThreadFactory factory) {
            super(factory);            
        }
        
        public Mapper(ThreadFactory factory,long reaper_interval) {
            super(factory,reaper_interval);            
        }

        public TCPConnection getConnection(Address dest) throws Exception {
            TCPConnection conn=null;
            getLock().lock();
            try {
                conn=conns.get(dest);
                if(conn == null) {
                    conn=new TCPConnection(dest);
                    conn.start(getThreadFactory());
                    addConnection(dest, conn);
                    if(log.isTraceEnabled())
                        log.trace("created socket to " + dest);
                }
            }
            finally {
                getLock().unlock();
            }
            return conn;
        }        
    }   
}


package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocking IO (BIO) connection. Starts 1 reader thread for the peer socket and blocks until data is available.
 * Calls {@link TcpServer#receive(Object,byte[],int,int)} when data has been received.
 * @author Bela Ban
 * @since  3.6.5
 */
public class TcpConnection implements Connection<Address> {
    protected final Socket           sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
    protected final ReentrantLock    send_lock=new ReentrantLock(); // serialize send()
    protected static final byte[]    cookie= { 'b', 'e', 'l', 'a' };
    protected DataOutputStream       out;
    protected DataInputStream        in;
    protected Address                peer_addr; // address of the 'other end' of the connection
    protected long                   last_access;
    protected volatile Sender        sender;
    protected volatile Receiver      receiver;
    protected final TcpServer        server;

    /** Creates a connection stub and binds it, use {@link #connect(Address)} to connect */
    public TcpConnection(Address peer_addr, TcpServer server) throws Exception {
        this.server=server;
        if(peer_addr == null)
            throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);
        this.peer_addr=peer_addr;
        this.sock=server.socketFactory().createSocket("jgroups.tcp.sock");
        setSocketParameters(sock);
        last_access=getTimestamp(); // last time a message was sent or received (ns)
    }

    public TcpConnection(Socket s, TcpServer server) throws Exception {
        this.sock=s;
        this.server=server;
        if(s == null)
            throw new IllegalArgumentException("Invalid parameter s=" + s);
        setSocketParameters(s);
        this.out=new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));
        this.in=new DataInputStream(new BufferedInputStream(s.getInputStream()));
        this.peer_addr=readPeerAddress(s);
        last_access=getTimestamp(); // last time a message was sent or received (ns)
    }

    protected long getTimestamp() {
        return server.timeService() != null? server.timeService().timestamp() : System.nanoTime();
    }

    protected Address getPeerAddress() {
        return peer_addr;
    }

    protected boolean isSenderUsed(){
        return server.sendQueueSize() > 0 && server.use_send_queues;
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
        if(server.connExpireTime() > 0)
            last_access=getTimestamp();
    }

    /** Called after {@link TcpConnection#TcpConnection(Address, SocketFactory)} */
    public void connect(Address dest) throws Exception {
        SocketAddress destAddr=new InetSocketAddress(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort());
        try {
            if(!server.defer_client_binding)
                this.sock.bind(new InetSocketAddress(server.client_bind_addr, server.client_bind_port));
            if(this.sock.getLocalSocketAddress() != null && this.sock.getLocalSocketAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
            Util.connect(this.sock, destAddr, server.sock_conn_timeout);
            this.out=new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
            this.in=new DataInputStream(new BufferedInputStream(sock.getInputStream()));
            sendLocalAddress(server.localAddress());
        }
        catch(Exception t) {
            server.socket_factory.close(this.sock);
            throw t;
        }
    }


    public void start() {
        if(receiver != null)
            receiver.stop();
        receiver=new Receiver(server.factory).start();

        if(isSenderUsed()) {
            if(sender != null)
                sender.stop();
            sender=new Sender(server.factory, server.sendQueueSize()).start();
        }
    }



    /**
     *
     * @param data Guaranteed to be non null
     * @param offset
     * @param length
     */
    public void send(byte[] data, int offset, int length) throws Exception {
        if (sender != null) {
            // we need to copy the byte[] buffer here because the original buffer might get changed meanwhile
            byte[] tmp = new byte[length];
            System.arraycopy(data, offset, tmp, 0, length);
            sender.addToQueue(tmp);
        }
        else
            _send(data, offset, length, true, true);
    }

    public void send(ByteBuffer buf) throws Exception {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() : 0,
          len=buf.remaining();
        if(!buf.isDirect())
            send(buf.array(), offset, len);
        else { // by default use a copy
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, len);
            send(tmp, 0, len);
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


    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            client_sock.setSendBufferSize(server.send_buf_size);
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting send buffer to %d bytes: %s", server.local_addr, server.send_buf_size, ex);
        }
        try {
            client_sock.setReceiveBufferSize(server.recv_buf_size);
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting receive buffer to %d bytes: %s", server.local_addr, server.recv_buf_size, ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(server.tcp_nodelay);
        if(server.linger > 0)
            client_sock.setSoLinger(true, server.linger);
        else
            client_sock.setSoLinger(false, -1);
    }


    /**
     * Reads the peer's address. First a cookie has to be sent which has to
     * match my own cookie, otherwise the connection will be refused
     */
    protected Address readPeerAddress(Socket client_sock) throws Exception {
        int timeout=client_sock.getSoTimeout();
        client_sock.setSoTimeout(server.peerAddressReadTimeout());

        try {
            // read the cookie first
            byte[] input_cookie=new byte[cookie.length];
            in.readFully(input_cookie, 0, input_cookie.length);
            if(!matchCookie(input_cookie))
                throw new SocketException("BaseServer.TcpConnection.readPeerAddress(): cookie read by "
                                            + server.localAddress() + " does not match own cookie; terminating connection");
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
            recv=f.newThread(this,"Connection.Receiver [" + getSockAddress() + "]");
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
                        server.receive(peer_addr, buf, 0, len);
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
                server.removeConnectionIfPresent(peer_addr, TcpConnection.this);
            }
        }
    }

    protected class Sender implements Runnable {
        protected final BlockingQueue<byte[]> send_queue;
        protected final Thread                runner;
        protected volatile boolean            started=true;


        public Sender(ThreadFactory tf, int send_queue_size) {
            this.runner=tf.newThread(this, "Connection.Sender [" + getSockAddress() + "]");
            this.send_queue=new LinkedBlockingQueue<>(send_queue_size);
        }

        public void addToQueue(byte[] data) throws Exception{
            if(canRun())
                if (!send_queue.offer(data, server.sock_conn_timeout, TimeUnit.MILLISECONDS))
                    server.log.warn("%s: discarding message because TCP send_queue is full and hasn't been releasing for %d ms",
                                    server.local_addr, server.sock_conn_timeout);
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
                server.removeConnectionIfPresent(peer_addr, TcpConnection.this);
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
        return server.conn_expire_time > 0 && now - last_access >= server.conn_expire_time;
    }

    public boolean isConnected() {
        return sock != null && !sock.isClosed() && sock.isConnected();
    }

    public boolean isOpen() {
        boolean connected=isConnected();
        Sender tmp_sender=sender;
        boolean sender_ok=!isSenderUsed() || (tmp_sender != null && tmp_sender.isRunning());
        Receiver tmp_receiver=receiver;
        boolean receiver_ok=tmp_receiver != null && tmp_receiver.isRunning();
        return connected && sender_ok && receiver_ok;
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
                server.socket_factory.close(sock);
            }
            catch(Throwable t) {}
            Util.close(out, in);
        }
        finally {
            send_lock.unlock();
        }
        server.notifyConnectionClosed(peer_addr);
    }
}
package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocking IO (BIO) connection. Starts 1 reader thread for the peer socket and blocks until data is available.
 * Calls {@link TcpServer#receive(Address,byte[],int,int)} when data has been received.
 * @author Bela Ban
 * @since  3.6.5
 */
public class TcpConnection extends Connection {
    protected final Socket           sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
    protected final ReentrantLock    send_lock=new ReentrantLock(); // serialize send()
    protected DataOutputStream       out;
    protected DataInputStream        in;
    protected volatile Receiver      receiver;
    protected final TcpBaseServer    server;
    protected final AtomicInteger    writers=new AtomicInteger(0); // to determine the last writer to flush
    protected boolean                connected;

    /** Creates a connection stub and binds it, use {@link #connect(Address)} to connect */
    public TcpConnection(Address peer_addr, TcpBaseServer server) throws Exception {
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
        this.out=new DataOutputStream(createBufferedOutputStream(s.getOutputStream()));
        this.in=new DataInputStream(createBufferedInputStream(s.getInputStream()));
        this.connected=sock.isConnected();
        this.peer_addr=server.usePeerConnections()? readPeerAddress(s)
          : new IpAddress((InetSocketAddress)s.getRemoteSocketAddress());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
    }

    public Address localAddress() {
        InetSocketAddress local_addr=sock != null? (InetSocketAddress)sock.getLocalSocketAddress() : null;
        return local_addr != null? new IpAddress(local_addr) : null;
    }

    public Address peerAddress() {
        return peer_addr;
    }

    protected long getTimestamp() {
        return server.timeService() != null? server.timeService().timestamp() : System.nanoTime();
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

    public void connect(Address dest) throws Exception {
        connect(dest, server.usePeerConnections());
    }

    protected void connect(Address dest, boolean send_local_addr) throws Exception {
        SocketAddress destAddr=new InetSocketAddress(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort());
        try {
            if(!server.defer_client_binding)
                this.sock.bind(new InetSocketAddress(server.client_bind_addr, server.client_bind_port));
            Util.connect(this.sock, destAddr, server.sock_conn_timeout);
            if(this.sock.getLocalSocketAddress() != null && this.sock.getLocalSocketAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
            this.out=new DataOutputStream(createBufferedOutputStream(sock.getOutputStream()));
            this.in=new DataInputStream(createBufferedInputStream(sock.getInputStream()));
            connected=sock.isConnected();
            if(send_local_addr)
                sendLocalAddress(server.localAddress());
        }
        catch(Exception t) {
            Util.close(this.sock);
            connected=false;
            throw t;
        }
    }


    public void start() {
        if(receiver != null)
            receiver.stop();
        receiver=new Receiver(server.factory).start();
    }



    /**
     *
     * @param data Guaranteed to be non null
     * @param offset
     * @param length
     */
    public void send(byte[] data, int offset, int length) throws Exception {
        if(out == null)
            return;
        writers.incrementAndGet();
        send_lock.lock();
        try {
            doSend(data, offset, length);
            updateLastAccessed();
        }
        catch(InterruptedException iex) {
            Thread.currentThread().interrupt(); // set interrupt flag again
        }
        finally {
            if(writers.decrementAndGet() == 0) // only the last active writer thread calls flush()
                flush(); // won't throw an exception
            send_lock.unlock();
        }
    }

    public void send(ByteBuffer buf) throws Exception {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(),
          len=buf.remaining();
        if(!buf.isDirect())
            send(buf.array(), offset, len);
        else { // by default use a copy; but of course implementers of Receiver can override this
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, len);
            send(tmp, 0, len); // will get copied again if send-queues are enabled
        }
    }


    protected void doSend(byte[] data, int offset, int length) throws Exception {
        out.writeInt(length); // write the length of the data buffer first
        out.write(data,offset,length);
    }

    protected void flush() {
        try {
            out.flush();
        }
        catch(Throwable t) {
        }
    }

    protected BufferedOutputStream createBufferedOutputStream(OutputStream out) {
        int size=(server instanceof TcpServer)? ((TcpServer)server).getBufferedOutputStreamSize() : 0;
        return size == 0? new BufferedOutputStream(out) : new BufferedOutputStream(out, size);
    }

    protected BufferedInputStream createBufferedInputStream(InputStream in) {
        int size=(server instanceof TcpServer)? ((TcpServer)server).getBufferedInputStreamSize() : 0;
        return size == 0? new BufferedInputStream(in) : new BufferedInputStream(in, size);
    }

    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            if(server.send_buf_size > 0)
                client_sock.setSendBufferSize(server.send_buf_size);
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting send buffer to %d bytes: %s", server.local_addr, server.send_buf_size, ex);
        }
        try {
            if(server.recv_buf_size > 0)
                client_sock.setReceiveBufferSize(server.recv_buf_size);
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting receive buffer to %d bytes: %s", server.local_addr, server.recv_buf_size, ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(server.tcp_nodelay);
        try { // todo: remove try-catch clause one https://github.com/oracle/graal/issues/1087 has been fixed
            if(server.linger > 0)
                client_sock.setSoLinger(true, server.linger);
            else
                client_sock.setSoLinger(false, -1);
        }
        catch(Throwable t) {
            server.log().warn("%s: failed setting SO_LINGER option: %s", server.localAddress(), t);
        }
    }


    /**
     * Send the cookie first, then the our port number. If the cookie
     * doesn't match the receiver's cookie, the receiver will reject the
     * connection and close it.
     */
    protected void sendLocalAddress(Address local_addr) throws Exception {
        try {
            // write the cookie
            out.write(cookie, 0, cookie.length);

            // write the version
            out.writeShort(Version.version);
            out.writeShort(local_addr.serializedSize()); // address size
            local_addr.writeTo(out);
            out.flush(); // needed ?
            updateLastAccessed();
        }
        catch(Exception ex) {
            server.socket_factory.close(this.sock);
            connected=false;
            throw ex;
        }
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
            if(!Arrays.equals(cookie, input_cookie))
                throw new SocketException(String.format("%s: BaseServer.TcpConnection.readPeerAddress(): cookie sent by " +
                                                          "%s:%d does not match own cookie; terminating connection",
                                                        server.localAddress(), client_sock.getInetAddress(), client_sock.getPort()));
            // then read the version
            short version=in.readShort();
            if(!Version.isBinaryCompatible(version))
                throw new IOException("packet from " + client_sock.getInetAddress() + ":" + client_sock.getPort() +
                                        " has different version (" + Version.print(version) +
                                        ") from ours (" + Version.printVersion() + "); discarding it");
            in.readShort(); // address length is only needed by NioConnection

            Address client_peer_addr=new IpAddress();
            client_peer_addr.readFrom(in);
            updateLastAccessed();
            return client_peer_addr;
        }
        finally {
            client_sock.setSoTimeout(timeout);
        }
    }



    protected class Receiver implements Runnable {
        protected final Thread     recv;
        protected volatile boolean receiving=true;
        protected byte[]           buffer; // no need to be volatile, only accessed by this thread

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
            return this;
        }

        public boolean isRunning()  {return receiving;}
        public boolean canRun()     {return isRunning() && isConnected();}
        public int     bufferSize() {return buffer != null? buffer.length : 0;}

        public void run() {
            try {
                while(canRun()) {
                    int len=in.readInt(); // needed to read messages from TCP_NIO2
                    server.receive(peer_addr, in, len);
                    updateLastAccessed();
                }
            }
            catch(EOFException | SocketException ex) {
                ; // regular use case when a peer closes its connection - we don't want to log this as exception
            }
            catch(Exception e) {
                server.log.warn("failed handling message from %s: %s", peer_addr, e);
            }
            finally {
                server.notifyConnectionClosed(TcpConnection.this);
            }
        }
    }


    public String toString() {
        Socket tmp_sock=sock;
        if(tmp_sock == null)
            return "<null socket>";
        InetAddress local=tmp_sock.getLocalAddress(), remote=tmp_sock.getInetAddress();
        String local_str=local != null? Util.shortName(local) : "<null>";
        String remote_str=remote != null? Util.shortName(remote) : "<null>";
        return String.format("%s:%s --> %s:%s (%d secs old) [%s] [recv_buf=%d]",
                             local_str, tmp_sock.getLocalPort(), remote_str, tmp_sock.getPort(),
                             TimeUnit.SECONDS.convert(getTimestamp() - last_access, TimeUnit.NANOSECONDS),
                             status(), receiver != null? receiver.bufferSize() : 0);
    }

    @Override
    public String status() {
        if(sock == null)    return "n/a";
        if(isConnected())   return "connected";
        if(isOpen())        return "open";
        return                     "closed";
    }

    public boolean isExpired(long now) {
        return server.conn_expire_time > 0 && now - last_access >= server.conn_expire_time;
    }

    public boolean isConnected() {
        return connected;
    }

    public boolean isConnectionPending() {
        return false;
    }

    public boolean isOpen() {
        return sock != null && !sock.isClosed();
    }

    public void close() throws IOException {
        Util.close(sock); // fix for https://issues.jboss.org/browse/JGRP-2350
        send_lock.lock();
        try {
            if(receiver != null) {
                receiver.stop();
                receiver=null;
            }
            Util.close(out,in);
        }
        finally {
            connected=false;
            send_lock.unlock();
        }
    }
}

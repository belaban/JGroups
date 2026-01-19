package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Version;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Bits;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.ThreadFactory;
import org.jgroups.util.Util;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import java.io.*;
import java.lang.System.Logger;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Blocking IO (BIO) connection. Starts 1 reader thread for the peer socket and blocks until data is available.
 * Calls {@link TcpServer#receive(Address,byte[],int,int)} when data has been received.
 * @author Bela Ban
 * @since  3.6.5
 */
public class TcpConnection extends Connection {
    protected long                createdTimestamp;
    protected final Socket        sock; // socket to/from peer (result of srv_sock.accept() or new Socket())
    protected DataOutputStream    out;
    protected DataInputStream     in;
    protected volatile Receiver   receiver;
    protected final AtomicInteger writers=new AtomicInteger(0); // to determine the last writer to flush
    protected volatile boolean    connected;
    protected boolean             use_lock_to_send=true; // e.g. a single sender doesn't need to acquire the send_lock

    /** Creates a connection to a remote peer, use {@link #connect(Address)} to connect */
    public TcpConnection(Address peer_addr, TcpBaseServer server) throws Exception {
        this.server=server;
        if(peer_addr == null)
            throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);
        this.peer_addr=peer_addr;
        this.sock=server.socketFactory().createSocket("jgroups.tcp.sock");
        setSocketParameters(sock);
        last_access=getTimestamp(); // last time a message was sent or received (ns)
        if(sock instanceof SSLSocket) // https://issues.redhat.com/browse/JGRP-2748
            sock.setSoLinger(true, 0);
        this.createdTimestamp = System.nanoTime();
    }

    /** Called by {@link TcpServer.Acceptor#handleAccept(Socket)} */
    public TcpConnection(Socket s, TcpServer server) throws Exception {
        this.sock=s;
        this.server=server;
        if(s == null)
            throw new IllegalArgumentException("Invalid parameter s=" + s);
        setSocketParameters(s);
        this.out=createDataOutputStream(s.getOutputStream());
        this.in=createDataInputStream(s.getInputStream());
        this.connected=sock.isConnected();
        this.peer_addr=server.usePeerConnections()? readPeerAddress(s)
          : new IpAddress((InetSocketAddress)s.getRemoteSocketAddress());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
        if(sock instanceof SSLSocket) // https://issues.redhat.com/browse/JGRP-2748
            sock.setSoLinger(true, 0);
        this.createdTimestamp = this.in.readLong();
    }

    public boolean                  useLockToSend() {return use_lock_to_send;}
    public <T extends Connection> T useLockToSend(boolean u) {this.use_lock_to_send=u; return (T)this;}

    public Address localAddress() {
        InetSocketAddress local_addr=sock != null? (InetSocketAddress)sock.getLocalSocketAddress() : null;
        return local_addr != null? new IpAddress(local_addr) : null;
    }

    protected String getSockAddress() {
        StringBuilder sb=new StringBuilder();
        if(sock != null) {
            sb.append(sock.getLocalAddress().getHostAddress()).append(':').append(sock.getLocalPort());
            sb.append(" - ").append(sock.getInetAddress().getHostAddress()).append(':').append(sock.getPort());
        }
        return sb.toString();
    }

    public void connect(Address dest) throws Exception {
        connect(dest, server.usePeerConnections());
    }

    protected void connect(Address dest, boolean send_local_addr) throws Exception {
        SocketAddress destAddr=((IpAddress)dest).getSocketAddress();
        try {
            if(!server.defer_client_binding)
                this.sock.bind(new InetSocketAddress(server.client_bind_addr, server.client_bind_port));
            Util.connect(this.sock, destAddr, server.sock_conn_timeout);
            if(this.sock.getLocalSocketAddress() != null && this.sock.getLocalSocketAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
            if(sock instanceof SSLSocket)
                ((SSLSocket) sock).startHandshake();
            this.out=createDataOutputStream(sock.getOutputStream());
            this.in=createDataInputStream(sock.getInputStream());
            if(send_local_addr)
                sendLocalAddress(server.localAddress());
            this.out.writeLong(createdTimestamp);
            this.out.flush();
        }
        catch(Exception t) {
            Util.close(this.sock);
            connected=false;
            throw t;
        }
    }

    @Override
    public long getCreatedTimestamp() {
        return createdTimestamp;
    }

    @Override
    public void ack() {
        try {
            // a bit ugly for now
            byte []ok = new byte[] {'o', 'k'};
            this.out.write(ok);
            this.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nack() {
        try {
            // a bit ugly for now
            byte []ok = new byte[] {'k', 'o'};
            this.out.write(ok);
            this.out.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void waitForAck() {
        try {
            // a bit ugly for now
            byte[] ok = new byte[2];
            this.in.read(ok);
            if (Arrays.equals(new byte[] { 'o', 'k' }, ok)) {
                // needs to be at the end or else isConnected() will return this connection and threads can start sending
                // even though we haven't yet sent the local address and waited for the ack (if use_acks==true)
                connected = sock.isConnected();
                rejected = false;
            } else {
                rejected = true;
                connected = false;
                Util.close(this.sock);
            }
        } catch(Exception t) {
            Util.close(this.sock);
            connected=false;
        }
    }

    public void start() {
        if(receiver != null)
            receiver.stop();
        receiver=new Receiver(server.factory).start();
    }

    public void send(byte[] data, int offset, int length) throws Exception {
        if(out == null)
            return;
        if(!use_lock_to_send) {
            locklessSend(data, offset, length);
            return;
        }
        writers.incrementAndGet();
        send_lock.lock();
        try {
            doSend(data, offset, length, false);
        }
        finally {
            send_lock.unlock();
            if(writers.decrementAndGet() == 0) // only the last active writer thread calls flush()
                flush(); // won't throw an exception
        }
    }

    public void locklessSend(byte[] data, int offset, int length) throws Exception {
        if(out == null)
            return;
        doSend(data, offset, length, true);
    }

    public void send(ByteBuffer buf) throws Exception {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(),
          len=buf.remaining();
        if(buf.hasArray())
            send(buf.array(), offset, len);
        else {
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, len);
            send(tmp, 0, len); // will get copied again if send-queues are enabled
        }
    }

    @GuardedBy("send_lock")
    protected void doSend(byte[] data, int offset, int length, boolean flush) throws Exception {
        byte[] length_buf=new byte[Integer.BYTES]; // used to write the length of the data
        Bits.writeInt(length, length_buf, 0); // write the length of the data buffer first

        ByteArrayOutputStream array = new ByteArrayOutputStream();
        array.writeBytes(length_buf);
        array.write(data, offset, length);
        out.write(array.toByteArray());
        if (flush)
            out.flush();
    }

    public void flush() {
        try {
            out.flush();
        }
        catch(Throwable t) {
        }
    }

    protected DataOutputStream createDataOutputStream(OutputStream out) {
        int size=((TcpBaseServer)server).getBufferedOutputStreamSize();
        return new DataOutputStream(size == 0? out : new BufferedOutputStream(out, size));
    }

    protected DataInputStream createDataInputStream(InputStream in) {
        int size=((TcpBaseServer)server).getBufferedInputStreamSize();
        return size == 0? new DataInputStream(in) : new DataInputStream(new BufferedInputStream(in, size));
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
        if(server.linger > 0)
            client_sock.setSoLinger(true, server.linger);
    }


    /**
     * Send the cookie first, then our port number. If the cookie doesn't match the receiver's cookie,
     * the receiver will reject the connection and close it.
     */
    protected void sendLocalAddress(Address local_addr) throws Exception {
        try {
            int addr_size=local_addr.serializedSize();
            ByteArrayDataOutputStream os=new ByteArrayDataOutputStream(addr_size + Short.BYTES*2 + cookie.length);
            os.write(cookie, 0, cookie.length);
            os.writeShort(Version.version);
            os.writeShort(addr_size); // address size
            local_addr.writeTo(os);
            out.write(os.buffer(), 0, os.position());
            out.flush(); // needed ?
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
        client_sock.setSoTimeout(((TcpBaseServer)server).peerAddressReadTimeout());

        try {
            // read the cookie first
            byte[] input_cookie=new byte[cookie.length];
            in.readFully(input_cookie, 0, input_cookie.length);
            if(!Arrays.equals(cookie, input_cookie))
                throw new IOException(String.format("%s: readPeerAddress(): cookie %s sent by %s does not match own cookie; terminating connection",
                                                    server.localAddress(), Util.byteArrayToHexString(input_cookie), client_sock.getRemoteSocketAddress()));
            // then read the version
            short version=in.readShort();
            if(!Version.isBinaryCompatible(version))
                throw new IOException(String.format("%s: readPeerAddress(): packet from %s has different version (%s) from ours (%s); discarding it",
                                                    server.localAddress(), client_sock.getRemoteSocketAddress(), Version.print(version), Version.printVersion()));
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
            recv.interrupt();
            receiving=false;
            return this;
        }

        public boolean isRunning()  {return receiving;}
        public boolean canRun()     {return isRunning() && isConnected();}

        public void run() {
            try {
                while(canRun()) {
                    int len=in.readInt(); // needed to read messages from TCP_NIO2
                    server.receive(peer_addr, in, len);
                    updateLastAccessed();
                }
            }
            catch (InterruptedException e) {
                server.log.debug("%s: was interrupted %s", server.local_addr, peer_addr);
            }
            catch(EOFException e) {
                ; // regular use case when a peer closes its connection - we don't want to log this as exception
                server.log.debug("%s: end of stream %s", server.local_addr, peer_addr);
            }
            catch(SocketException ex) {
                ; // socket has been closed
                server.log.debug("%s: closed socket %s", server.local_addr, peer_addr);
            }
            catch(SSLHandshakeException e) {
                server.log.debug("%s: ssl handshake problem %s", server.local_addr, peer_addr);
                if (e.getCause() instanceof EOFException) {
                    ; // Ignore SSL handshakes closed early (usually liveness probes)
                }
            }
            catch (SSLException e) {
                server.log.debug("%s: ssl general exception %s", server.local_addr, peer_addr);
                if (e.getMessage().contains("Socket closed")) {
                    ; // regular use case when a peer closes its connection - we don't want to log this as exception
                }
            }
            catch(IOException e) {
                ; // the stream has been closed and the contained input stream does not support reading after close, or another I/O error occurs.
                server.log.debug("%s: the input stream not support reading after close %s", server.local_addr, peer_addr);
            }
            catch(Exception e) {
                if(server.logDetails())
                    server.log.warn("%s: failed handling message for %s with exception message %s", server.local_addr, TcpConnection.this, e);
                else
                    server.log.warn("%s: failed handling message for %s with exception message %s", server.local_addr, TcpConnection.this, e);
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
        String l=local != null? Util.shortName(local) : "<null>";
        String r=remote != null? Util.shortName(remote) : "<null>";
        return String.format("%s:%s --> %s:%s (%d secs old) [%s]%s", l, tmp_sock.getLocalPort(), r, tmp_sock.getPort(),
                             SECONDS.convert(getTimestamp() - last_access, NANOSECONDS), status(),
                             use_lock_to_send? "" : " [lockless]");
    }

    @Override
    public String status() {
        if(sock == null)  return "n/a";
        if(isRejected())  return "rejected";
        if(isClosed())    return "closed";
        if(isConnected()) return "connected";
        return                   "open";
    }

    @Override public boolean isConnected() {
        return connected;
    }

    @Override public boolean isConnectionPending() {
        return false;
    }

    @Override public boolean isClosed() {
        return sock == null || sock.isClosed();
    }

    @Override public void close() throws IOException {
        if (!connected) {
            return;
        }
        try {
            this.out.flush();
        } catch (Exception e) {
            ; // do nothing
        }
        connected=false;
        Receiver r=receiver;
        if(r != null) {
            r.stop();
            receiver=null;
        }
        Util.close(out,in);
        Util.close(sock); // fix for https://issues.redhat.com/browse/JGRP-2350
        server.log.info("%s: TcpConnetion::close %s", server.local_addr, this);
    }
}

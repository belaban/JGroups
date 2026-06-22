package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.nio.Buffers;
import org.jgroups.nio.MessageReader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.Util;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.SelectionKey.*;

/**
 * An NIO based impl of {@link Connection}
 * @author Bela Ban
 * @since  3.6.5
 */
public class NioConnection extends Connection {
    protected final SocketChannel     channel;        // the channel to the peer
    protected SelectionKey            key;
    protected final Buffers           send_buf;       // send messages via gathering writes
    protected final MessageReader     message_reader;
    protected boolean                 copy_on_partial_write=true;
    protected int                     partial_writes; // number of partial writes (write which did not write all bytes)
    protected final PeerAddressReader peer_addr_reader;
    protected ByteBuffer              length_buf;     // reused: send the length of the next buf
    protected ByteBuffer              graceful_close_buf;
    protected ByteBuffer              cookie_buffer;  // for reception of the cookie (never called concurrently)


     /** Creates a connection stub and binds it, use {@link #connect(Address)} to connect */
    public NioConnection(Address peer_addr, NioBaseServer server) throws Exception {
        this.server=server;
        if(peer_addr == null)
            throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);
        this.peer_addr=peer_addr;
        send_buf=new Buffers(server.maxSendBuffers() *2); // space for actual bufs and length bufs!
        channel=server.socketFactory().createSocketChannel("jgroups.nio.client");
        channel.configureBlocking(false);
        setSocketParameters(channel.socket());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
        message_reader=new MessageReader(this, channel, 1024, server.useDirectMemory()).maxLength(server.getMaxLength());
        peer_addr_reader=new PeerAddressReader(server.useDirectMemory());
        createBuffers();
    }

    public NioConnection(SocketChannel channel, NioBaseServer server) throws Exception {
        this.channel=channel;
        this.server=server;
        setSocketParameters(this.channel.socket());
        channel.configureBlocking(false);
        send_buf=new Buffers(server.maxSendBuffers() *2); // space for actual bufs and length bufs!
        if(!server.usePeerConnections())
            peer_addr=new IpAddress((InetSocketAddress)channel.getRemoteAddress());
        peer_addr_reader=new PeerAddressReader(server.useDirectMemory());
        message_reader=new MessageReader(this, channel, 1024, server.useDirectMemory()).maxLength(server.getMaxLength());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
        createBuffers();
    }

    @Override
    public boolean isConnected() {return channel != null && channel.isConnected();}

    @Override
    public boolean isConnectionPending() {return channel != null && channel.isConnectionPending();}

    @Override
    public boolean isClosed() {return channel == null || !channel.isOpen();}

    @Override
    public Address localAddress() {
        InetSocketAddress local_addr=null;
        if(channel != null) {
            try {local_addr=(InetSocketAddress)channel.getLocalAddress();} catch(IOException e) {}
        }
        return local_addr != null? new IpAddress(local_addr) : null;
    }

    public SelectionKey  key()                         {return key;}
    public NioConnection key(SelectionKey k)           {this.key=k; return this;}
    public NioConnection copyOnPartialWrite(boolean b) {this.copy_on_partial_write=b; return this;}
    public boolean       copyOnPartialWrite()          {return copy_on_partial_write;}
    public int           numPartialWrites()            {return partial_writes;}

    public synchronized void registerSelectionKey(int interest_ops) {
        if(key != null && key.isValid())
            key.interestOpsOr(interest_ops);
    }

    public synchronized void clearSelectionKey(int interest_ops) {
        if(key != null && key.isValid())
            key.interestOps(key.interestOps() & ~interest_ops);
    }

    @Override
    public void connect(Address dest) throws Exception {
        connect(dest, server.usePeerConnections());
    }

    protected void connect(Address dest, boolean send_local_addr) throws Exception {
        SocketAddress destAddr=((IpAddress)dest).getSocketAddress();
        try {
            if(!server.deferClientBinding())
                this.channel.bind(new InetSocketAddress(server.clientBindAddress(), server.clientBindPort()));
            this.key=((NioBaseServer)server).register(channel, OP_CONNECT | OP_READ, this);
            boolean success=Util.connect(channel, destAddr);
            if(success || channel.finishConnect())
                clearSelectionKey(OP_CONNECT);
            if(this.channel.getLocalAddress() != null && this.channel.getLocalAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);
            if(send_local_addr)
                sendLocalAddress(server.localAddress());
        }
        catch(Exception t) {
            close();
            throw t;
        }
    }

    @Override
    public void start() throws Exception {
        ; // nothing to be done here
    }

    @Override
    public void send(byte[] buf, int offset, int length) throws Exception {
        send(ByteBuffer.wrap(buf, offset, length));
    }

    /**
     * Sends a message. If the previous write didn't complete, tries to complete it. If this still doesn't complete,
     * the message is dropped (needs to be retransmitted, e.g. by UNICAST3 or NAKACK2).
     */
    @Override
    public void send(ByteBuffer buf) throws Exception {
        send(buf, true);
    }

    protected void send(ByteBuffer buf, boolean send_length) throws Exception {
        send_lock.lock();
        try {
            // makeLengthBuffer() reuses the same pre-allocated buffer and copies it only if the write didn't complete
            if(send_length) {
                int length=buf.remaining();
                length_buf.clear().putInt(0, length);
                send_buf.add(length_buf, buf);
            }
            else
                send_buf.add(buf);
            boolean success=send_buf.write(channel);
            if(!success) {
                registerSelectionKey(OP_WRITE);
                if(copy_on_partial_write)
                    send_buf.copy(); // copy data on partial write as further writes might corrupt data (https://issues.redhat.com/browse/JGRP-1991)
                partial_writes++;
            }
        }
        catch(Exception ex) {
            if(!(ex instanceof SocketException || ex instanceof EOFException || ex instanceof ClosedChannelException))
                server.log.error("%s: failed sending message to %s: %s", server.localAddress(), peerAddress(), ex);
            throw ex;
        }
        finally {
            send_lock.unlock();
        }
    }

    /**
     * Sends the buffers currently present in send_buf
     * @return True if all buffers were sent successfully, false otherwise
     * @throws Exception If the send failed, e.g. because the channel was closed
     */
    public boolean send() throws Exception {
        send_lock.lock();
        try {
            boolean success=send_buf.write(channel);
            if(success) {
                clearSelectionKey(OP_WRITE);
                return true;
            }
            else {
                // copy data on partial write as further writes might corrupt data (https://issues.redhat.com/browse/JGRP-1991)
                if(copy_on_partial_write)
                    send_buf.copy();
                partial_writes++;
                return false;
            }
        }
        finally {
            send_lock.unlock();
        }
    }


    /** Read the length first, then the actual data. This method is not reentrant and access must be synchronized */
    public void read() throws Exception {
        for(;;) { // try to receive as many msgs as possible, until no more msgs are ready or the conn is closed
            try {
                if(!_read())
                    break;
                updateLastAccessed();
            }
            catch(Exception ex) {
                if(!(ex instanceof SocketException || ex instanceof EOFException || ex instanceof ClosedChannelException))
                    server.log.warn("%s: failed handling message from %s: %s", server.localAddress(), peerAddress(), ex);
                server.closeConnection(NioConnection.this);
                return;
            }
        }
    }

    protected boolean _read() throws Exception {
        ByteBuffer msg;
        Receiver   receiver=server.receiver();

        if(peer_addr == null) {
            if((peer_addr=peer_addr_reader.readPeerAddress(channel)) == null)
                return false;
            server.handleIncomingConnection(peer_addr, this);
            return true;
        }
        if((msg=message_reader.readMessage()) == null)
            return false;
        if(receiver != null)
            receiver.receive(peer_addr, msg);
        return true;
    }

    @Override
    public void close() throws IOException {
        close(true);
    }

    @Override
    public void close(boolean graceful) throws IOException {
        if(isClosed())
            return;
        if(graceful && !closed_gracefully)
            send_buf.add(graceful_close_buf);
        doClose(); // flushes send_buf
    }

    public void flush() {
        send_lock.lock();
        try {
            if(send_buf.remaining() > 0) { // try to flush send buffer if it still has pending data to send
                try {send();} catch(Throwable e) {}
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    public String toString() {
        InetSocketAddress local=null, remote=null;
        try {local=channel != null? (InetSocketAddress)channel.getLocalAddress() : null;} catch(Throwable t) {}
        try {remote=channel != null? (InetSocketAddress)channel.getRemoteAddress() : null;} catch(Throwable t) {}
        String loc=local == null ? "n/a" : local.getHostString() + ":" + local.getPort(),
          rem=remote == null? "n/a" : remote.getHostString() + ":" + remote.getPort();
        return String.format("<%s --> %s> (%d secs old) [%s] send_buf: %s, message_reader: %s",
                             loc, rem, TimeUnit.SECONDS.convert(getTimestamp() - last_access, TimeUnit.NANOSECONDS),
                             status(), send_buf, message_reader);
    }

    @Override
    public String status() {
        if(channel == null)       return "n/a";
        if(isClosed())            return closed_gracefully? "closed gracefully" : "closed";
        if(isConnected())         return "connected";
        if(isConnectionPending()) return "connection pending";
        return                           "open";
    }

    protected void doClose() {
        flush();
        Util.close(channel);
    }

    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            if(server.sendBufferSize() > 0)
                client_sock.setSendBufferSize(server.sendBufferSize());
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting send buffer to %d bytes: %s", server.localAddress(), server.sendBufferSize(), ex);
        }
        try {
            if(server.receiveBufferSize() > 0)
                client_sock.setReceiveBufferSize(server.receiveBufferSize());
        }
        catch(IllegalArgumentException ex) {
            server.log.error("%s: exception setting receive buffer to %d bytes: %s", server.localAddress(), server.receiveBufferSize(), ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(server.tcpNodelay());
        if(server.linger() >= 0)
            client_sock.setSoLinger(true, server.linger());
        else
            client_sock.setSoLinger(false, -1);
    }

    protected void sendLocalAddress(Address local_addr) throws Exception {
        try {
            int addr_size=local_addr.serializedSize();
            int expected_size=cookie.length + Global.SHORT_SIZE*2 + addr_size;
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(expected_size +2);
            out.write(cookie, 0, cookie.length);
            out.writeShort(Version.version);
            out.writeShort(addr_size); // address size
            local_addr.writeTo(out);
            ByteBuffer buf=ByteBuffer.wrap(out.buffer(), 0, out.position());
            send(buf, false);
        }
        catch(Exception ex) {
            close();
            throw ex;
        }
    }

    protected void createBuffers() {
        length_buf=createBuffer(Integer.BYTES); // reused: send the length of the next buf
        graceful_close_buf=createBuffer(Integer.BYTES).putInt(GRACEFUL_CLOSE).flip();
        cookie_buffer=createBuffer(Integer.BYTES);
    }

    protected ByteBuffer createBuffer(int size) {
        return ((NioBaseServer)server).useDirectMemory()? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }

    /**
     * Reads a peer address from a SocketChannel. Note that method {@link #readPeerAddress(SocketChannel)} is not reentrant
     */
    public static class PeerAddressReader {
        protected enum State {initial, reading_metadata, metadata_read}
        protected static final int METADATA_SIZE=9, IPV4_SIZE=15, IPv6_SIZE=31;
        protected final ByteBuffer buffer;
        // 0: initial state, 1: metadata read
        protected State            state=State.initial;
        protected final byte[]     cookie_buf=new byte[4];

        public PeerAddressReader() {
            this(true);
        }

        public PeerAddressReader(boolean use_direct_memory) {
            buffer=Util.createBuffer(IPv6_SIZE, use_direct_memory); // 15 for an IPv4 address; 31 for an IPv6 address
        }

        // Reads the peer address. If not enough bytes can be read -> return null
        // Format: [cookie (4)] [version (2)] [addr-size (2)] [addr-len (1)], followed by
        //   IPv4: [addr (4)]  [port (2)]: total=15 bytes
        //   IPv6: [addr (20)] [port (2)]: total=31 bytes
        // We read 15 bytes and check addr-len: if 4, we can parse the address right away (IPv4). If 16, we need to
        // read another 16 bytes, and then parse the address (IPv6).
        // This method might have to be called multiple times until a non-null address is returned
        public Address readPeerAddress(SocketChannel ch) throws IOException {
            while(ch.isOpen()) {
                switch(state) {
                    case initial:
                        buffer.clear().limit(IPV4_SIZE);
                        state=State.reading_metadata;
                        break;
                    case reading_metadata:
                        int num=ch.read(buffer);
                        if(num == -1)
                            throw new EOFException();
                        if(buffer.position() < METADATA_SIZE)
                            return null; // metadata hasn't been fully read yet
                        state=State.metadata_read;
                        // otherwise parse the metadata and get the length of the address (4: IPv4, 20: IPv6)
                        int len=parseMetadata(ch.getLocalAddress(), ch.getRemoteAddress());
                        if(len != 4 && len != 16)
                            throw new IllegalArgumentException(String.format("length (%d) has to be 4 or 20", len));
                        if(len == 16) { // IPv6 (default is IPv4)
                            if(buffer.limit() < IPv6_SIZE)
                                buffer.limit(IPv6_SIZE);
                        }
                        break;
                    case metadata_read: // metadata read, read address
                        if(buffer.hasRemaining()) {
                            num=ch.read(buffer);
                            if(num == -1)
                                throw new EOFException();
                            if(buffer.hasRemaining())
                                return null;
                        }
                        IpAddress addr=new IpAddress();
                        int addr_start=METADATA_SIZE-1, remaining=buffer.limit()-addr_start;
                        ByteBuffer addr_buf=buffer.slice(addr_start, remaining);
                        addr.readFrom(new ByteBufferInputStream(addr_buf));
                        reset();
                        return addr;
                }
            }
            return null;
        }

        public PeerAddressReader reset() {
            state=State.initial;
            buffer.clear().limit(IPV4_SIZE);
            return this;
        }

        protected int parseMetadata(SocketAddress local, SocketAddress remote) throws IOException {
            buffer.get(0, cookie_buf, 0, 4);
            if(!Arrays.equals(cookie, cookie_buf)) {
                String fmt=String.format("%s: readPeerAddress(): cookie %s sent by %s does not match own cookie",
                                         local, Util.byteArrayToHexString(cookie_buf), remote);
                throw new IOException(fmt);
            }
            short version=buffer.getShort(cookie_buf.length);
            if(!Version.isBinaryCompatible(version)) {
                String fmt=String.format("%s: readPeerAddress(): packet from %s has different version (%s) from ours (%s)",
                                         local, remote, Version.print(version), Version.printVersion());
                throw new IOException(fmt);
            }
            return buffer.get(METADATA_SIZE-1);
        }


    }


}

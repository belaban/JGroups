package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.nio.Buffers;
import org.jgroups.nio.MessageReader;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
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
    protected final SocketChannel channel;      // the channel to the peer
    protected SelectionKey        key;
    protected final Buffers       send_buf;     // send messages via gathering writes
    protected final MessageReader message_reader;
    protected final ByteBuffer    length_buf=ByteBuffer.allocate(Integer.BYTES); // reused: send the length of the next buf
    protected boolean             copy_on_partial_write=true;
    protected int                 partial_writes; // number of partial writes (write which did not write all bytes)
    protected Buffers             peer_addr_recv_buf;



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
        message_reader=new MessageReader(channel).maxLength(server.getMaxLength());
    }

    public NioConnection(SocketChannel channel, NioBaseServer server) throws Exception {
        this.channel=channel;
        this.server=server;
        setSocketParameters(this.channel.socket());
        channel.configureBlocking(false);
        send_buf=new Buffers(server.maxSendBuffers() *2); // space for actual bufs and length bufs!
        if (server.usePeerConnections()) {
            peer_addr_recv_buf=new Buffers(4).add(ByteBuffer.allocate(cookie.length));
        } else {
            peer_addr=new IpAddress((InetSocketAddress)channel.getRemoteAddress());
        }
        message_reader=new MessageReader(channel).maxLength(server.getMaxLength());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
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
            key.interestOps(key.interestOps() | interest_ops);
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
            if(send_length)
                send_buf.add(makeLengthBuffer(buf.remaining()), buf);
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

    public void send() throws Exception {
        send_lock.lock();
        try {
            boolean success=send_buf.write(channel);
            if(success)
                clearSelectionKey(OP_WRITE);
            else {
                // copy data on partial write as further writes might corrupt data (https://issues.redhat.com/browse/JGRP-1991)
                if(copy_on_partial_write)
                    send_buf.copy();
                partial_writes++;
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

        if(peer_addr == null && (peer_addr=readPeerAddress()) != null) {
            peer_addr_recv_buf=null;
            server.addConnection(peer_addr, this);
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
        send_lock.lock();
        try {
            if(send_buf.remaining() > 0) { // try to flush send buffer if it still has pending data to send
                try {send();} catch(Throwable e) {}
            }
            server.socketFactory().close(channel);
        }
        finally {
            send_lock.unlock();
        }
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
        if(isClosed())            return "closed";
        if(isConnected())         return "connected";
        if(isConnectionPending()) return "connection pending";
        return                           "open";
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

    protected Address readPeerAddress() throws Exception {
        while(peer_addr_recv_buf.read(channel)) {
            int current_position=peer_addr_recv_buf.position()-1;
            ByteBuffer buf=peer_addr_recv_buf.get(current_position);
            if(buf == null)
                return null;
            buf.flip();
            switch(current_position) {
                case 0:      // cookie
                    byte[] cookie_buf=getBuffer(buf);
                    if(!Arrays.equals(cookie, cookie_buf))
                        throw new IOException(String.format("%s: readPeerAddress(): cookie %s sent by %s does not match own cookie; terminating connection",
                                                            server.localAddress(), Util.byteArrayToHexString(cookie_buf), channel.getRemoteAddress()));
                    peer_addr_recv_buf.add(ByteBuffer.allocate(Global.SHORT_SIZE));
                    break;
                case 1:      // version
                    short version=buf.getShort();
                    if(!Version.isBinaryCompatible(version))
                        throw new IOException(String.format("%s: readPeerAddress(): packet from %s has different version (%s) from ours (%s); discarding it",
                                                            server.localAddress(), channel.getRemoteAddress(), Version.print(version), Version.printVersion()));
                    peer_addr_recv_buf.add(ByteBuffer.allocate(Global.SHORT_SIZE));
                    break;
                case 2:      // length of address
                    short addr_len=buf.getShort();
                    peer_addr_recv_buf.add(ByteBuffer.allocate(addr_len));
                    break;
                case 3:      // address
                    byte[] addr_buf=getBuffer(buf);
                    ByteArrayDataInputStream in=new ByteArrayDataInputStream(addr_buf);
                    IpAddress addr=new IpAddress();
                    addr.readFrom(in);
                    return addr;
                default:
                    throw new IllegalStateException(String.format("position %d is invalid", peer_addr_recv_buf.position()));
            }
        }
        return null;
    }

    protected static byte[] getBuffer(final ByteBuffer buf) {
        byte[] retval=new byte[buf.limit()];
        buf.get(retval, buf.position(), buf.limit());
        return retval;
    }


    protected ByteBuffer makeLengthBuffer(int length) {
        length_buf.clear(); // buf was used before to write, so reset it again
        length_buf.putInt(0, length); // absolute put; doesn't move position or limit
        return length_buf;
    }



}

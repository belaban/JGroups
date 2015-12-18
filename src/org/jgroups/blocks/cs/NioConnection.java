package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.nio.Buffers;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An NIO based impl of {@link Connection}
 * @author Bela Ban
 * @since  3.6.5
 */
public class NioConnection implements Connection {
    protected SocketChannel       channel;      // the channel to the peer
    protected SelectionKey        key;
    protected Address             peer_addr;    // address of the 'other end' of the connection
    protected long                last_access;  // timestamp of the last access to this connection (read or write)
    protected final NioBaseServer server;

    // {length, data} pair to send a message with a gathering write
    protected final Buffers       send_buf;
    protected boolean             write_interest_set; // set when a send() didn't manage to send all data
    protected final Lock          send_lock=new ReentrantLock(); // serialize send()

    // creates an array of 2: length buffer (for reading the length of the following data buffer) and data buffer
    protected Buffers             recv_buf=new Buffers(ByteBuffer.allocate(Global.INT_SIZE), null);
    protected boolean             copy_on_partial_write=true;
    protected int                 partial_writes; // number of partial writes (write which did not write all bytes)


     /** Creates a connection stub and binds it, use {@link #connect(Address)} to connect */
    public NioConnection(Address peer_addr, NioBaseServer server) throws Exception {
        this.server=server;
        if(peer_addr == null)
            throw new IllegalArgumentException("Invalid parameter peer_addr="+ peer_addr);
        this.peer_addr=peer_addr;
        send_buf=new Buffers(server.maxSendBuffers() *2); // space for actual bufs and length bufs!
        channel=SocketChannel.open();
        channel.configureBlocking(false);
        setSocketParameters(channel.socket());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
    }

    public NioConnection(SocketChannel channel, NioBaseServer server) throws Exception {
        this.channel=channel;
        this.server=server;
        setSocketParameters(this.channel.socket());
        channel.configureBlocking(false);
        send_buf=new Buffers(server.maxSendBuffers() *2); // space for actual bufs and length bufs!
        this.peer_addr=server.usePeerConnections()? null /* read by first receive() */
          : new IpAddress((InetSocketAddress)channel.getRemoteAddress());
        last_access=getTimestamp(); // last time a message was sent or received (ns)
    }




    @Override
    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return channel != null && channel.isConnected();
    }

    @Override
    public boolean isExpired(long now) {
        return server.connExpireTime() > 0 && now - last_access >= server.connExpireTime();
    }

    protected void updateLastAccessed() {
        if(server.connExpireTime() > 0)
            last_access=getTimestamp();
    }

    @Override
    public Address localAddress() {
        InetSocketAddress local_addr=null;
        if(channel != null) {
            try {local_addr=(InetSocketAddress)channel.getLocalAddress();} catch(IOException e) {}
        }
        return local_addr != null? new IpAddress(local_addr) : null;
    }

    public Address       peerAddress()                 {return peer_addr;}
    public SelectionKey  key()                         {return key;}
    public NioConnection key(SelectionKey k)           {this.key=k; return this;}
    public NioConnection copyOnPartialWrite(boolean b) {this.copy_on_partial_write=b; return this;}
    public boolean       copyOnPartialWrite()          {return copy_on_partial_write;}
    public int           numPartialWrites()            {return partial_writes;}

    public synchronized int registerSelectionKey(int interest_ops) {
        if(key == null)
            return 0;
        key.interestOps(key.interestOps() | interest_ops);
        return key.interestOps();
    }

    public synchronized int clearSelectionKey(int interest_ops) {
        if(key == null)
            return 0;
        key.interestOps(key.interestOps() & ~interest_ops);
        return key.interestOps();
    }


    @Override
    public void connect(Address dest) throws Exception {
        connect(dest, server.usePeerConnections());
    }

    protected void connect(Address dest, boolean send_local_addr) throws Exception {
        SocketAddress destAddr=new InetSocketAddress(((IpAddress)dest).getIpAddress(), ((IpAddress)dest).getPort());
        try {
            if(!server.deferClientBinding())
                this.channel.bind(new InetSocketAddress(server.clientBindAddress(), server.clientBindPort()));
            if(this.channel.getLocalAddress() != null && this.channel.getLocalAddress().equals(destAddr))
                throw new IllegalStateException("socket's bind and connect address are the same: " + destAddr);

            this.key=server.register(channel, SelectionKey.OP_CONNECT | SelectionKey.OP_READ, this);
            if(Util.connect(channel, destAddr)) {
                if(channel.finishConnect())
                    clearSelectionKey(SelectionKey.OP_CONNECT);
            }
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
     * @param buf
     * @throws Exception
     */
    @Override
    public void send(ByteBuffer buf) throws Exception {
        send_lock.lock();
        try {
            // makeLengthBuffer() reuses the same pre-allocated buffer and copies it only if the write didn't complete
            boolean success=send_buf.add(makeLengthBuffer(buf), buf).write(channel);
            writeInterest(!success);
            if(success)
                updateLastAccessed();
            if(!success) {
                if(copy_on_partial_write)
                    send_buf.copy(); // copy data on partial write as subsequent writes might corrupt data (https://issues.jboss.org/browse/JGRP-1991)
                partial_writes++;
            }
        }
        finally {
            send_lock.unlock();
        }
    }


    public void send() throws Exception {
        send_lock.lock();
        try {
            boolean success=send_buf.write(channel);
            writeInterest(!success);
            if(success)
                updateLastAccessed();
            if(!success) {
                if(copy_on_partial_write)
                    send_buf.copy(); // copy data on partial write as subsequent writes might corrupt data (https://issues.jboss.org/browse/JGRP-1991)
                partial_writes++;
            }
        }
        finally {
            send_lock.unlock();
        }
    }



    /** Read the length first, then the actual data. This method is not reentrant and access must be synchronized */
    public void receive() throws Exception {
        _receive(true);
    }


    /** Tries to receive up to max_msgs_to_receive messages in one go */
    public void receive(int max_msgs_to_receive) throws Exception {
        try {
            while(max_msgs_to_receive-- > 0) {
                if(!_receive(false))
                    return;
            }
        }
        finally {
            updateLastAccessed();
        }
    }


    protected boolean _receive(boolean update) throws Exception {
        ByteBuffer msg;
        Receiver   receiver=server.receiver();

        if((msg=recv_buf.readLengthAndData(channel)) == null)
            return false;
        if(peer_addr == null && server.usePeerConnections()) {
            peer_addr=readPeerAddress(msg);
            server.addConnection(peer_addr, this);
            return true;
        }
        if(receiver != null)
            receiver.receive(peer_addr, msg);
        if(update)
            updateLastAccessed();
        return true;
    }


    @Override
    public void close() throws IOException {
        send_lock.lock();
        try {
            if(send_buf.remaining() > 0) { // try to flush send buffer if it still has pending data to send
                try {send();} catch(Throwable e) {}
            }
            Util.close(channel);
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
        return String.format("<%s --> %s> (%d secs old) [%s] [send_buf: %s, recv_buf: %s] [ops=%d]",
                             loc, rem, TimeUnit.SECONDS.convert(getTimestamp() - last_access, TimeUnit.NANOSECONDS),
                             status(), send_buf, recv_buf, key != null? key.interestOps() : -1);
    }

    protected String status() {
        if(channel == null) return "n/a";
        if(isConnected())   return "connected";
        if(channel.isConnectionPending()) return "connection pending";
        if(isOpen())        return "open";
        return "closed";
    }

    protected long getTimestamp() {
        return server.timeService() != null? server.timeService().timestamp() : System.nanoTime();
    }

    protected void writeInterest(boolean register) {
        if(register) {
            if(!write_interest_set) {
                write_interest_set=true;
                registerSelectionKey(SelectionKey.OP_WRITE);
            }
        }
        else {
            if(write_interest_set) {
                write_interest_set=false;
                clearSelectionKey(SelectionKey.OP_WRITE);
            }
        }
    }

    protected void setSocketParameters(Socket client_sock) throws SocketException {
        try {
            client_sock.setSendBufferSize(server.sendBufferSize());
        }
        catch(IllegalArgumentException ex) {
            server.log().error("%s: exception setting send buffer to %d bytes: %s", server.localAddress(), server.sendBufferSize(), ex);
        }
        try {
            client_sock.setReceiveBufferSize(server.receiveBufferSize());
        }
        catch(IllegalArgumentException ex) {
            server.log().error("%s: exception setting receive buffer to %d bytes: %s", server.localAddress(), server.receiveBufferSize(), ex);
        }

        client_sock.setKeepAlive(true);
        client_sock.setTcpNoDelay(server.tcpNodelay());
        if(server.linger() > 0)
            client_sock.setSoLinger(true, server.linger());
        else
            client_sock.setSoLinger(false, -1);
    }

    protected void sendLocalAddress(Address local_addr) throws Exception {
        try {
            ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
            out.writeShort(Version.version);
            local_addr.writeTo(out);
            ByteBuffer buf=out.getByteBuffer();
            send(buf);
            updateLastAccessed();
        }
        catch(Exception ex) {
            close();
            throw ex;
        }
    }

    protected Address readPeerAddress(ByteBuffer buf) throws Exception {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);

        // then read the version
        short version=in.readShort();
        if(!Version.isBinaryCompatible(version))
            throw new IOException("packet from " + channel.getRemoteAddress() + " has different version (" + Version.print(version) +
                                    ") from ours (" + Version.printVersion() + "); discarding it");
        Address client_peer_addr=new IpAddress();
        client_peer_addr.readFrom(in);
        updateLastAccessed();
        return client_peer_addr;
    }

    protected static ByteBuffer makeLengthBuffer(ByteBuffer buf) {
        return (ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE).putInt(buf.remaining()).clear();
    }

}

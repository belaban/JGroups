package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents a connection to a peer
 */
public abstract class Connection implements Closeable {
    public static final byte[] MSG_ACK= { 'o', 'k' };
    public static final byte[] MSG_NACK= { 'k', 'o'};
    public static final int MSG_LENGTH=MSG_ACK.length;

    public static final byte[] cookie= { 'b', 'e', 'l', 'a' };
    public static final int    GRACEFUL_CLOSE=-5;
    protected BaseServer       server;
    protected Address          peer_addr;             // address of the 'other end' of the connection
    protected long             last_access;           // timestamp of the last access to this connection (read or write)
    protected volatile boolean closed_gracefully;     // set when a length of GRACEFUL_CLOSE has been received
    protected final Lock       send_lock=new ReentrantLock(); // serialize send()

    abstract public boolean    isConnected();
    abstract public boolean    isConnectionPending();
    abstract public boolean    isClosed();
    public   boolean           isClosedGracefully() {return closed_gracefully;}
    public Connection          setClosedGracefully(boolean b) {this.closed_gracefully=b; return this;}
    abstract public Address    localAddress();
    public Address             peerAddress() {return peer_addr;}
    abstract public void       flush(); // sends pending data
    abstract public void       connect(Address dest) throws Exception;
    abstract public void       start() throws Exception;
    abstract public void       send(byte[] buf, int offset, int length) throws Exception;
    abstract public void       send(ByteBuffer buf) throws Exception;
    abstract public String     status();
    abstract public void       close(boolean graceful) throws IOException;

    protected long getTimestamp() {
        return server.timeService() != null? server.timeService().timestamp() : System.nanoTime();
    }

    protected void updateLastAccessed() {
        if(server.connExpireTime() > 0)
            last_access=getTimestamp();
    }

    public boolean isExpired(long now) {
        return server.connExpireTime() > 0 && now - last_access >= server.connExpireTime();
    }

    abstract public void ack() throws Exception;
    abstract public void nack() throws Exception;
    abstract public void waitForAck() throws Exception;
}

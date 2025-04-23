package org.jgroups.blocks.cs;

import org.jgroups.PhysicalAddress;
import java.io.Closeable;
import java.nio.ByteBuffer;


/**
 * Represents a connection to a peer
 */
public abstract class Connection implements Closeable {
    public static final byte[]    cookie= { 'b', 'e', 'l', 'a' };
    protected BaseServer          server;
    protected PhysicalAddress     peer_addr;    // address of the 'other end' of the connection
    protected long                last_access;  // timestamp of the last access to this connection (read or write)

    abstract public boolean         isConnected();
    abstract public boolean         isConnectionPending();
    abstract public boolean         isClosed();
    abstract public PhysicalAddress localAddress();
    public PhysicalAddress          peerAddress() {return peer_addr;}
    abstract public void            flush(); // sends pending data
    abstract public void            connect(PhysicalAddress dest) throws Exception;
    abstract public void            start() throws Exception;
    abstract public void            send(byte[] buf, int offset, int length) throws Exception;
    abstract public void            send(ByteBuffer buf) throws Exception;
    abstract public String          status();

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
}

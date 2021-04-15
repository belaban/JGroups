package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.io.Closeable;
import java.nio.ByteBuffer;


/**
 * Represents a connection to a peer
 */
public abstract class Connection implements Closeable {
    public static final byte[]    cookie= { 'b', 'e', 'l', 'a' };
    protected Address             peer_addr;    // address of the 'other end' of the connection
    protected long                last_access;  // timestamp of the last access to this connection (read or write)

    abstract public boolean isOpen();
    abstract public boolean isConnected();
    abstract public boolean isConnectionPending();
    abstract public Address localAddress();
    abstract public Address peerAddress();
    abstract public boolean isExpired(long millis);
    abstract public void    flush(); // sends pending data
    abstract public void    connect(Address dest) throws Exception;
    abstract public void    start() throws Exception;
    abstract public void    send(byte[] buf, int offset, int length) throws Exception;
    abstract public void    send(ByteBuffer buf) throws Exception;
    abstract public String  status();
}

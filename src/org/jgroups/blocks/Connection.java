package org.jgroups.blocks;

import java.io.Closeable;
import java.nio.ByteBuffer;


/**
 * Represents a connection to a peer
 * @param <A> The type of the peer address
 */
public interface Connection<A> extends Closeable {
    boolean isOpen();
    boolean isConnected();
    boolean isExpired(long milis);
    void    connect(A dest) throws Exception;
    void    start() throws Exception;
    void    send(byte[] buf, int offset, int length) throws Exception;
    void    send(ByteBuffer buf) throws Exception;
}
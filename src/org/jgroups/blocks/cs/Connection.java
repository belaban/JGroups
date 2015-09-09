package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.io.Closeable;
import java.nio.ByteBuffer;


/**
 * Represents a connection to a peer
 */
public interface Connection extends Closeable {
    boolean isOpen();
    boolean isConnected();
    Address localAddress();
    Address peerAddress();
    boolean isExpired(long millis);
    void    connect(Address dest) throws Exception;
    void    start() throws Exception;
    void    send(byte[] buf, int offset, int length) throws Exception;
    void    send(ByteBuffer buf) throws Exception;
}
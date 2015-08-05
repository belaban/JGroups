package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.nio.ByteBuffer;

/**
 * Defines common operations for clients (TCP or NIO)
 * @author Bela Ban
 * @since  3.6.5
 */
public interface Client {
    Address localAddress();

    Address remoteAddress();

    boolean isOpen();

    boolean isConnected();

    /** Sends data to the remote server. The server's address must have been set before. */
    void send(byte[] data, int offset, int length) throws Exception;

    /** Sends data to the remote server.  The server's address must have been set before. */
    void send(ByteBuffer data) throws Exception;
}

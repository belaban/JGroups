package org.jgroups.nio;

import java.nio.ByteBuffer;

/**
 * NIO based server interface for sending byte[] buffers to a single or all members and for receiving byte[] buffers.
 * Implementations should be able to be used to provide light weight NIO servers.
 * @param <A> The type of the address, e.g. {@link org.jgroups.Address}
 * @author Bela Ban
 * @since  3.6.5
 */

public interface Server<A> {

    /**
     * Sets a receiver
     * @param receiver The receiver
     */
    Server<A> receiver(Receiver<A> receiver);

    /**
     * Starts the server. Implementations will typically create a socket and start a select loop. Note that start()
     * should return immediately, and work (e.g. select()) should be done in a separate thread
     */
    void start() throws Exception;

    /** Stops the server, e.g. by killing the thread calling select() */
    void stop();

    /**
     * Sends a message to a destination
     * @param dest The destination address. Must not be null
     * @param buf The buffer
     * @param offset The offset into the buffer
     * @param length The number of bytes to be sent
     */
    void send(A dest, byte[] buf, int offset, int length) throws Exception;

    /**
     * Sends a message to a destination
     * @param dest The destination address. Must not be null
     * @param buf The buffer to be sent
     */
    void send(A dest, ByteBuffer buf) throws Exception;
}

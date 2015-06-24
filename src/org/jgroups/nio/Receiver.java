package org.jgroups.nio;

import java.nio.ByteBuffer;

/**
 * Receiver interface to be used with {@link Server} implementations
 * @param <A> The type of the address, e.g. {@link org.jgroups.Address}
 * @author Bela Ban
 * @since  3.6.5
 */
public interface Receiver<A> {

    /**
     * Delivers a message from a given sender to the application
     * @param sender The sender of the message
     * @param buf The buffer. Invokers of receive() must ensure that the contents of buf are not overwritten, e.g. by
     *            copying it if a buffer is reused. The application can therefore hang on to buf as long as it needs to.
     *            When buf is not referenced any longer, it can get garbage collected.
     * @param offset The offset at which the received data starts
     * @param length The length of the received data
     */
    void receive(A sender, byte[] buf, int offset, int length);

    /**
     * Delivers a message from a given sender to the application
     * @param sender The sender of the message
     * @param buf The buffer. Invokers of receive() must ensure that the contents of buf are not overwritten, e.g. by
     *            copying it if a buffer is reused. The application can therefore hang on to buf as long as it needs to.
     *            When buf is not referenced any longer, it can get garbage collected.<p/>
     *            Note that buf could be a direct ByteBuffer.
     */
    void receive(A sender, ByteBuffer buf); // should be a default method in Java 8
}

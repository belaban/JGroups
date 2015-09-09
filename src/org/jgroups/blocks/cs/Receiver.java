package org.jgroups.blocks.cs;

import org.jgroups.Address;

import java.nio.ByteBuffer;

/**
 * Receiver interface to be used with {@link BaseServer} instances
 * @author Bela Ban
 * @since  3.6.5
 */
public interface Receiver {

    /**
     * Delivers a message from a given sender to the application
     * @param sender The sender of the message
     * @param buf The buffer. Invokers of receive() must ensure that the contents of buf are not overwritten, e.g. by
     *            copying it if a buffer is reused. The application can therefore hang on to buf as long as it needs to.
     *            When buf is not referenced any longer, it can get garbage collected.
     * @param offset The offset at which the received data starts
     * @param length The length of the received data
     */
    void receive(Address sender, byte[] buf, int offset, int length);

    /**
     * Delivers a message from a given sender to the application
     * @param sender The sender of the message
     * @param buf The buffer. Invokers of receive() must ensure that the contents of buf are not overwritten, e.g. by
     *            copying it if a buffer is reused. The application can therefore hang on to buf as long as it needs to.
     *            When buf is not referenced any longer, it can get garbage collected.<p/>
     *            Note that buf could be a direct ByteBuffer.
     */
    void receive(Address sender, ByteBuffer buf); // should be a default method in Java 8
}

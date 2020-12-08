package org.jgroups.blocks.cs;

import org.jgroups.Address;
import org.jgroups.util.Util;

import java.io.DataInput;
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
     * @param buf The buffer. An application typically de-serializes data from the buffer into objects used by the
     *            application. Note that when receive() returns, it is not safe to use the buffer any longer;
     *            if an application needs to use a buffer after this callback returns, it must make a copy.
     * @param offset The offset at which the received data starts
     * @param length The length of the received data
     */
    void receive(Address sender, byte[] buf, int offset, int length);

    /**
     * Delivers a message from a given sender to the application
     * @param sender The sender of the message
     * @param buf The buffer. An application typically de-serializes data from the buffer into objects used by the
     *            application. Note that when receive() returns, it is not safe to use the buffer any longer;
     *            if an application needs to use a buffer after this callback returns, it must make a copy.<p/>
     *            Note that buf could be a direct ByteBuffer.
     */
    default void receive(Address sender, ByteBuffer buf) {
        Util.bufferToArray(sender, buf, this);
    }

    void receive(Address sender, DataInput in) throws Exception;
}

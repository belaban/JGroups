package org.jgroups.tests.rt;

import java.nio.ByteBuffer;

/**
 * Receives messages from a {@link RtTransport} via a callback. Needs to be registered with {@link RtTransport}
 * @author Bela Ban
 * @since  4.0
 */
public interface RtReceiver {

    /**
     * Called when a message is received. Note that this method may be called by multiple threads concurrently
     * @param sender The address of the sender
     * @param buf The buffer
     * @param offset The offset of the data in the buffer
     * @param length The length (bytes) of the data
     */
    void receive(Object sender, byte[] buf, int offset, int length);

    default void receive(Object sender, ByteBuffer buf) {
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
        if(!buf.isDirect())
            receive(sender, buf.array(), offset, len);
        else {
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, tmp.length);
            receive(sender, tmp, 0, tmp.length);
        }
    }
}

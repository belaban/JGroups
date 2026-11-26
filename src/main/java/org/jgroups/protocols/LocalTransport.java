package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Util;

import java.nio.ByteBuffer;

/**
 * A local transport is used for sending messages only to single (or all) members of the same host.
 * @author Bela Ban
 * @since  5.2.0
 */
public interface LocalTransport {
    /**
     * Calls after the local transport has been created.
     * @param transport A reference to TP
     */
    LocalTransport init(TP transport) throws Exception;
    LocalTransport start() throws Exception;
    LocalTransport stop();
    LocalTransport destroy();
    LocalTransport resetStats();

    LocalTransport viewChange(View v);

    /** Returns true if addr is a local member, false otherwise */
    boolean isLocalMember(Address addr);

    /**
     * Sends a message to a given local member. The caller should check before whether dest is a local member;
     * an implementation is not required to do so, but may nevertheless perform the check (and throw an
     * exception if the destination is not local).
     * @param dest The address of the member to which to send the message. Must be non-null
     * @param buf The buffer to send
     * @param offset The offset at which the data starts
     * @param length The number of bytes to send
     * @exception Exception Thrown when the send failed, e.g. when dest isn't a local address.
     */
    void sendTo(Address dest, byte[] buf, int offset, int length) throws Exception;

    default void sendTo(Address dest, ByteBuffer buf) throws Exception {
        ByteArray ba=Util.bufferToByteArray(buf);
        if(ba != null)
            sendTo(dest, ba.array(), ba.offset(), ba.length());
    }

    /**
     * Sends a message to all local members.
     * @param buf The buffer to send
     * @param offset The offset at which the data starts
     * @param length The number of bytes to send
     * @exception Exception Thrown when the send failed.
     */
    void sendToAll(byte[] buf, int offset, int length) throws Exception;

    default void sendToAll(ByteBuffer buf) throws Exception {
        ByteArray ba=Util.bufferToByteArray(buf);
        if(ba != null)
            sendToAll(ba.array(), ba.offset(), ba.length());
    }
}

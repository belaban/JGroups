package org.jgroups.tests.rt;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Transport for the {@link org.jgroups.tests.RoundTrip} test
 * @author Bela Ban
 * @since  4.0
 */
public interface RtTransport {

    /**
     * Prints the accepted options, e.g. [-host host] [-port port] [-server host]
     * @return
     */
    String[] options();

    /**
     * Sets options on this transport. Usually done after creation and before {@link #start(String...)} is called,
     * but may also be called at runtime.
     * @param options The options
     * @throws Exception
     */
    void options(String ... options) throws Exception;

    /**
     * Sets the receiver whose {@link RtReceiver#receive(Object,byte[],int,int)} callback will be invoked whenever a
     * message is received
     * @param receiver
     */
    void receiver(RtReceiver receiver);

    /**
     * Returns the local addres of this member.
     * @return The local address. Implementations without cluster membership may return null
     */
    Object localAddress();

    /**
     * Returns the addresses of all cluster members. May return null if not implemented
     * @return The list of all members in the cluster
     */
    List<? extends Object> clusterMembers();

    /**
     * Starts the transport, e.g. connecting to a server socket
     * @param options Options passed to the transport at startup time. May be null
     * @throws Exception
     */
    void start(String... options) throws Exception;

    /**
     * Stops the transport, e.g. stopping the accept() loop in a TCP-based server
     */
    void stop();

    /**
     * Sends a message
     * @param dest The destination address
     * @param buf The buffer
     * @param offset The offset at which the data starts
     * @param length The length (in bytes) of the data to send
     * @throws Exception
     */
    void send(Object dest, byte[] buf, int offset, int length) throws Exception;

    default void send(Object dest, ByteBuffer buf) throws Exception {
        if(buf == null)
            return;
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
        if(!buf.isDirect())
            send(dest, buf.array(), offset, len);
        else {
            byte[] tmp=new byte[len];
            buf.get(tmp, 0, tmp.length);
            send(dest, tmp, 0, len);
        }
    }
}

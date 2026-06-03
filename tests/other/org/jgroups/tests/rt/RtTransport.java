package org.jgroups.tests.rt;

import org.jgroups.tests.RoundTrip;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Transport for the {@link org.jgroups.tests.RoundTrip} test
 * @author Bela Ban
 * @since  4.0
 */
public abstract class RtTransport {
    protected RoundTrip round_trip;
    protected boolean   vthreads=true;
    protected boolean   direct_memory=true; // use direct memory for ByteBuffers where supported

    public RtTransport roundTrip(RoundTrip rt) {
        this.round_trip=rt;
        return this;
    }

    public boolean     vthreads() {return vthreads;}
    public RtTransport vthreads(boolean b) {
        this.vthreads=b;
        return this;
    }

    public boolean     directMemory() {return direct_memory;}
    public RtTransport directMemory(boolean b) {
        direct_memory=b;
        return this;
    }

    /**
     * Prints the accepted options, e.g. [-host host] [-port port] [-server host]
     * @return
     */
    public abstract String[] options();

    /**
     * Sets options on this transport. Usually done after creation and before {@link #start(String...)} is called,
     * but may also be called at runtime.
     * @param options The options
     * @throws Exception
     */
    public abstract RtTransport options(String ... options) throws Exception;

    /**
     * Sets the receiver whose {@link RtReceiver#receive(Object,byte[],int,int)} callback will be invoked whenever a
     * message is received
     * @param receiver
     */
    public abstract RtTransport receiver(RtReceiver receiver);

    /**
     * Returns the local addres of this member.
     * @return The local address. Implementations without cluster membership may return null
     */
    public abstract Object localAddress();

    /**
     * Returns the addresses of all cluster members. May return null if not implemented
     * @return The list of all members in the cluster
     */
    public abstract List<? extends Object> clusterMembers();

    /**
     * Starts the transport, e.g. connecting to a server socket
     * @param options Options passed to the transport at startup time. May be null
     * @throws Exception
     */
    public abstract void start(String... options) throws Exception;

    /**
     * Stops the transport, e.g. stopping the accept() loop in a TCP-based server
     */
    public abstract void stop();

    /**
     * Sends a message
     * @param dest The destination address
     * @param buf The buffer
     * @param offset The offset at which the data starts
     * @param length The length (in bytes) of the data to send
     * @throws Exception
     */
    public abstract void send(Object dest, byte[] buf, int offset, int length) throws Exception;

    public void send(Object dest, ByteBuffer buf) throws Exception {
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

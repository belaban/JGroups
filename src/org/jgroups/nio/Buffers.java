package org.jgroups.nio;

import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;

/**
 * Use to write or read {length, data} pairs to or from a channel using gathering writes or scattering reads. This
 * class is not synchronized.
 * @author Bela Ban
 * @since  3.6.5
 */
public class Buffers {
    // bufs[0] contains the number of bytes to write or read (length)
    // bufs[1] contains the data to be written or read
    protected final ByteBuffer[] bufs;

    public Buffers() {
        this(null);
    }

    public Buffers(int capacity) {
        bufs=new ByteBuffer[capacity *2];
    }

    public Buffers(ByteBuffer data) {
        bufs=new ByteBuffer[]{ByteBuffer.allocate(Global.INT_SIZE), data};
    }

    public int remaining() {
        return bufs[0].remaining() + (bufs[1] != null? bufs[1].remaining() : 0);
    }

    /**
     * Writes the length and data with a gathering write
     * @param ch The channel to write to
     * @param buf The data buffer. Note that {@link ByteBuffer#position} needs to be at the start of the data to be written
     * @return True if all the bytes of the buffer were written successfully, false otherwise.
     * @throws Exception Thrown if the write failed
     */
    public boolean write(SocketChannel ch, ByteBuffer buf) throws Exception {
        if(bufs[1] != null && !write(ch))
            return false; // didn't manage to write all bytes

        bufs[0].clear();
        bufs[0].putInt(buf.remaining()).flip();
        bufs[1]=buf;
        return write(ch);
    }

    /**
     * Tries to complete a previous (unfinished) write
     * @param ch The channel to write to
     * @return True of all remaining bytes could be written, false otherwise
     * @throws Exception Thrown if the write failed
     */
    public boolean write(SocketChannel ch) throws Exception {
        if(bufs[1] == null) return true;
        if(ch != null /* && ch.isConnected() */) {
            try {
                ch.write(bufs); // send the (unfinished) buffer from the previous write
            }
            catch(ClosedChannelException closed_ex) {
                throw closed_ex;
            }
            catch(NotYetConnectedException | IOException others) {
                ; // ignore, we'll queue 1 write
            }
        }
        return nullData(remaining() == 0);
    }


    /**
     * Reads length, then allocates a data buffer and reads all data into it. Returns the data buffer when complete, or
     * null when not all data has been read
     * @param ch The channel to read from
     * @return The complete data buffer, or null when more data needs to be read
     * @throws Exception Thrown when the read failed
     */
    public ByteBuffer read(SocketChannel ch) throws Exception {
        if(bufs[0].hasRemaining() && ch.read(bufs[0]) < 0)
            throw new EOFException();

        if(bufs[0].hasRemaining())
            return null;

        if(bufs[1] == null) {
            bufs[0].flip();
            int len=bufs[0].getInt();
            bufs[1]=ByteBuffer.allocate(len);
        }
        if(bufs[1].hasRemaining() && ch.read(bufs[1]) < 0)
            throw new EOFException();

        if(!bufs[1].hasRemaining()) {
            try {
                bufs[1].flip();
                return bufs[1];
            }
            finally {
                bufs[0].clear();
                bufs[1]=null;
            }
        }
        return null; // has remaining; not all data read yet
    }


    public String toString() {
        StringBuilder sb=new StringBuilder(Util.print(bufs[0]));
        if(bufs[1] != null)
            sb.append(", ").append(Util.print(bufs[1]));
        sb.append(", remaining ").append(remaining());
        return sb.toString();
    }

    protected boolean nullData(boolean all_data_sent) {
        if(all_data_sent)
            bufs[1]=null;
        return all_data_sent;
    }

}

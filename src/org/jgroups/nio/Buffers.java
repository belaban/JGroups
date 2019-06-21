package org.jgroups.nio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Class to do scattering reads or gathering writes on a sequence of {@link ByteBuffer} instances. The buffers are
 * kept in an array with fixed capacity (max Short.MAX_VALUE).
 * Buffers can be added and removed dynamically (they're dropped when the capacity is exceeded).<p/>
 * A read is successful when all non-null buffers from left to right are filled, ie. all {@link ByteBuffer#remaining()}
 * methods return 0.<p/>
 * Same for writes: when all non-null buffers (from left to right) have been written ({@link ByteBuffer#remaining()} == 0),
 * a write is considered successful; otherwise it is partial.<p/>
 * Individual buffers can be accessed; e.g. for reading its value after a read. It is also possible to add buffers
 * dynamically, e.g. after reading a 'length' buffer, a user may want to add a new buffer allocated for reading
 * 'length' bytes.<p/>
 * This class is not synchronized.
 * @author Bela Ban
 * @since  3.6.5
 */
public class Buffers implements Iterable<ByteBuffer> {
    protected final ByteBuffer[] bufs; // the buffers to be written or read
    protected short position;          // points to the next buffer in bufs to be read or written
    protected short limit;             // points beyond the last buffer in bufs that was read or written
    protected short next_to_copy;      // index of the next buffer to copy, set by copy(): position <= last_copied <= limit

    /**
     * Creates a new instance with an array of capacity buffers
     * @param capacity Must be an unsigned positive short [1 .. Short.MAX_VALUE]
     */
    public Buffers(int capacity) {
        bufs=new ByteBuffer[toPositiveUnsignedShort(capacity)];
    }

    public Buffers(ByteBuffer ... data) {
        if(data == null)
            throw new IllegalArgumentException("null buffer array");
        assertPositiveUnsignedShort(data.length);
        this.bufs=data;
        for(ByteBuffer b: this.bufs) {
            if(b == null)
                break;
            limit++;
        }
    }

    public int     position()              {return position;}
    public Buffers position(int new_pos)   {this.position=toPositiveUnsignedShort(new_pos); nextToCopy(new_pos); return this;}
    public int     limit()                 {return limit;}
    public Buffers limit(int new_limit)    {this.limit=toPositiveUnsignedShort(new_limit); return this;}
    public int     nextToCopy()            {return next_to_copy;}
    public Buffers nextToCopy(int next)    {next_to_copy=toPositiveUnsignedShort(next); return this;}


    public int remaining() {
        int remaining=0;
        for(int i=position; i < limit; i++) {
            ByteBuffer buf=bufs[i];
            if(buf != null)
                remaining+=buf.remaining();
        }
        return remaining;
    }

    public boolean hasRemaining() {
        for(int i=position; i < limit; i++) {
            ByteBuffer buf=bufs[i];
            if(buf != null && buf.hasRemaining())
                return true;
        }
        return false;
    }


    public Buffers add(ByteBuffer ... buffers) {
        if(buffers == null)
            return this;
        assertPositiveUnsignedShort(buffers.length);
        int len=buffers.length;
        if(spaceAvailable(len) || (makeSpace() && spaceAvailable(len))) {
            for(ByteBuffer buf: buffers)
                bufs[limit++]=buf;
        }
        return this;
    }

    public Buffers add(ByteBuffer buf) {
        if(buf == null)
            return this;
        if(spaceAvailable(1) || (makeSpace() && spaceAvailable(1)))
            bufs[limit++]=buf;
        return this;
    }


    public ByteBuffer get(int index) {
        return this.bufs[index];
    }

    public Buffers set(int index, ByteBuffer buf) {
        this.bufs[index]=buf; return this;
    }

    /** Nulls the buffer at index */
    public Buffers remove(int index) {
        return set(index, null);
    }



    /**
     * Reads length and then length bytes into the data buffer, which is grown if needed.
     * @param ch The channel to read data from
     * @return The data buffer (position is 0 and limit is length), or null if not all data could be read.
     */
    public ByteBuffer readLengthAndData(SocketChannel ch) throws Exception {
        if(bufs[0].hasRemaining() && ch.read(bufs[0]) < 0)
            throw new EOFException();

        if(bufs[0].hasRemaining())
            return null;

        int len=bufs[0].getInt(0);
        if(bufs[1] == null || len > bufs[1].capacity())
            bufs[1]=ByteBuffer.allocate(len);
        // Workaround for JDK8 compatibility
        // limit() returns java.nio.Buffer in JDK8, but java.nio.ByteBuffer since JDK9.
        ((java.nio.Buffer) bufs[1]).limit(len);

        if(bufs[1].hasRemaining() && ch.read(bufs[1]) < 0)
            throw new EOFException();

        if(bufs[1].hasRemaining())
            return null;

        try {
            // Workaround for JDK8 compatibility
            // flip() returns java.nio.Buffer in JDK8, but java.nio.ByteBuffer since JDK9.
            return (ByteBuffer) ((java.nio.Buffer) bufs[1].duplicate()).flip();
        }
        finally {
            bufs[0].clear();
            bufs[1].clear();
        }
    }

    /**
     * Performs a scattering read into all (contiguous) non-null buffers in range [position .. limit]. Returns true if
     * the scattering read was successful, else false. Note that to read the contents of the individual buffers,
     * {@link ByteBuffer#clear()} has to be called (all buffers have their position == limit on a successful read).
     */
    public boolean read(SocketChannel ch) throws Exception {
        long bytes=ch.read(bufs, position, limit-position);
        if(bytes == -1)
            throw new EOFException();
        return adjustPosition(false);
    }

    /** Helper method which adds the buffers passed as arguments and then calls write() */
    public boolean write(GatheringByteChannel ch, ByteBuffer ... buffers) throws Exception {
        return add(buffers).write(ch);
    }

    /**
     * Writes the buffers from position to limit to the given channel. Note that all buffers need to have
     * their {@link ByteBuffer#position} at the start of the data to be written
     * be at the start of the data to be written.
     * @param ch The channel to write to
     * @return True if all the bytes of the buffer were written successfully, false otherwise (partial write).
     * @throws Exception Thrown if the write failed
     */
    public boolean write(GatheringByteChannel ch) throws Exception {
        int num_buffers_to_write=size();
        if(num_buffers_to_write == 0)
            return true;

        if(ch != null) {
            try {
                ch.write(bufs, position, num_buffers_to_write);
            }
            catch(ClosedChannelException closed_ex) {
                throw closed_ex;
            }
            catch(NotYetConnectedException | IOException others) {
                ; // ignore, we'll queue 1 write
            }
        }

        return nullData();
    }

    /** Copies the data that has not yet been written and moves last_copied. Typically done after an unsuccessful
     * write, if copying is required. This is typically needed if the output buffer is reused. Note that
     * direct buffers will be converted to heap-based buffers */
    public Buffers copy() {
        for(int i=Math.max(position, next_to_copy); i < limit; i++) {
            this.bufs[i]=copyBuffer(this.bufs[i]);
            next_to_copy=(short)(i+1);
        }
        return this;
    }

    /** Returns the number of elements that have not yet been read or written */
    public int size() {
        return limit - position;
    }

    public String toString() {
        return String.format("[%d bufs pos=%d lim=%d cap=%d rem=%d]", size(), position, limit, bufs.length, remaining());
    }

    protected boolean spaceAvailable(int num_buffers) {
        return bufs.length - limit >= num_buffers;
    }

    protected boolean makeSpace() {
        if(position == limit) { // easy case: no pending writes, but pos and limit are not at the head of the buffer
            position=limit=next_to_copy=0;   // redundant if position == 0, but who cares
            return true;
        }
        // limit > position: copy to head of buffer if position > 0
        if(position == 0) // cannot copy to head as position is already at head
            return false;

        // move data to the head of the buffer
        int buffers_to_move=size();
        for(int dest_index=0, src_index=position; dest_index < buffers_to_move; dest_index++, src_index++) {
            bufs[dest_index]=bufs[src_index];
        }
        // null buffers
        for(int i=buffers_to_move; i < limit; i++)
            bufs[i]=null;
        next_to_copy-=position;
        limit=(short)buffers_to_move; // same as limit-=position
        position=0;
        next_to_copy=(short)Math.max(next_to_copy, position);
        return true;
    }

     /** Looks at all buffers in range [position .. limit-1] and nulls buffers that have no remaining data.
     * Returns true if all buffers could be nulled, and false otherwise */
    protected boolean nullData() {
        if(!adjustPosition(true))
            return false;
        if(position >= bufs.length)
            makeSpace();
        return true;
    }

    protected boolean adjustPosition(boolean null_complete_data) {
        while(position < limit) {
            ByteBuffer buf=bufs[position];
            if(buf.remaining() > 0)
                return false;
            if(null_complete_data)
                bufs[position]=null;
            position++;
            if(next_to_copy < position)
                next_to_copy=position;
        }
        return true;
    }

    protected static short toPositiveUnsignedShort(int num) {
        assertPositiveUnsignedShort(num);
        return (short)num;
    }

    protected static void assertPositiveUnsignedShort(int num) {
        if(num < 1 || num > Short.MAX_VALUE) {
            short tmp=(short)num;
            throw new IllegalArgumentException(String.format("number %d must be a positive unsigned short", tmp));
        }
    }

    /*protected void assertNextToCopy() {
        boolean condition=position <= next_to_copy && next_to_copy <= limit;
        assert condition
          : String.format("position=%d next_to_copy=%d limit=%d\n", position, next_to_copy, limit);
    }*/

    /** Copies a ByteBuffer by copying and wrapping the underlying array of a heap-based buffer. Direct buffers
        are converted to heap-based buffers */
    public static ByteBuffer copyBuffer(final ByteBuffer buf) {
        if(buf == null)
            return null;
        int offset=buf.hasArray()? buf.arrayOffset() + buf.position() : buf.position(), len=buf.remaining();
        byte[] tmp=new byte[len];
        if(!buf.isDirect())
            System.arraycopy(buf.array(), offset, tmp, 0, len);
        else {
         //   buf.get(tmp, 0, len);
            for(int i=0; i < len; i++)
                tmp[i]=buf.get(i+offset);
        }
        return ByteBuffer.wrap(tmp);
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new BuffersIterator();
    }

    /** Iterates over the non-null buffers */
    protected class BuffersIterator implements Iterator<ByteBuffer> {
        protected int index=-1;

        @Override
        public boolean hasNext() {
            for(int i=index+1; i < bufs.length; i++)
                if(bufs[i] != null)
                    return true;
            return false;
        }

       // @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
        @Override
        public ByteBuffer next() {
            while(true) {
                if(++index >= bufs.length)
                    throw new NoSuchElementException(String.format("index %d is out of range (%d buffers)", index, bufs.length));
                if(bufs[index] != null)
                    return bufs[index];
            }
        }

        @Override
        public void remove() { // todo: remove when baselining on Java 8
            ;
        }
    }
}

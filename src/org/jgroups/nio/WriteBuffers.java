package org.jgroups.nio;

import org.jgroups.Global;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;

/**
 * Whereas {@link Buffers} only store 1 pending write, WriteBuffers store N pending writes. When the bounded buffer is
 * full, additional writes will be discarded. <p/>
 * All pending writes are stored in the bufs array, and position and limit point to the current start and end of
 * writable buffers. <p/>
 * Completed writes are nulled, and when the end of the array is reached, position and limit wrap around, possibly
 * copying pending writes to the head of the array. Position and limit only advance when an entire buffer has been
 * written successfully; partial writes don't advance them. <p/>
 * Like Buffers, WriteBuffers is not synchronized. <p/>
 * Invariant: 0 <= position <= limit
 * @author Bela Ban
 * @since  3.6.5
 */
public class WriteBuffers extends Buffers {
    protected int position; // points to the next buffer in bufs
    protected int limit;    // points 1 beyond the last buffer in bufs

    public WriteBuffers() {
    }

    /** Creates a WriteBuffers of capacity * 2 elements (capacity {length/data} pairs) */
    public WriteBuffers(int capacity) {
        super(capacity);
    }

    public WriteBuffers(ByteBuffer data) {
        super(data);
        if(data == null)
            throw new IllegalArgumentException("buffer must not be null");
        limit=2;
    }


    public int position() {return position;}
    public int limit()    {return limit;}


    //@Override
    public boolean write(SocketChannel ch, ByteBuffer buf) throws Exception {
        if(spaceAvailable() || makeSpace())
            add(buf);
        return write(ch);
    }

    //@Override
    public boolean write(SocketChannel ch) throws Exception {
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


    @Override
    public int remaining() {
        int remaining=0;
        for(int i=position; i < limit; i++) {
            ByteBuffer buf=bufs[i];
            remaining+=buf.remaining();
        }
        return remaining;
    }

    /** Returns the number of elements that have not yet been written */
    public int size() {
        return limit - position;
    }

    public String print() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(int i=position; i < limit; i++) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(Util.print(bufs[i]));
        }
        sb.append(" (").append(size()).append(" elements, remaining=").append(remaining()).append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("[%d bufs pos=%d lim=%d cap=%d rem=%d]", size(), position, limit, bufs.length, remaining());
    }

    protected boolean spaceAvailable() {
        return bufs.length - limit >= 2;
    }

    protected boolean makeSpace() {
        if(position == limit) { // easy case: no pending writes, but pos and limit are not at the head of the buffer
            position=limit=0;   // redundant if position == 0, but who cares
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
        position=0;
        limit=buffers_to_move;
        return spaceAvailable();
    }

    protected void add(ByteBuffer buf) {
        if(buf == null)
            return;
        bufs[limit++]=makeLengthBuffer(buf);
        bufs[limit++]=buf;
    }

    /** Looks at all buffers in range [position .. limit-1] and nulls buffers that have no remaining data.
     * Returns true if all buffers could be nulled, and false otherwise */
    protected boolean nullData() {
        while(position < limit) {
            ByteBuffer buf=bufs[position];
            if(buf.remaining() > 0)
                return false;
            bufs[position++]=null;
        }
        if(position >= bufs.length)
            makeSpace();
        return true;
    }

    protected static ByteBuffer makeLengthBuffer(ByteBuffer buf) {
        return (ByteBuffer)ByteBuffer.allocate(Global.INT_SIZE).putInt(buf.remaining()).flip();
    }

}

package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded input stream. A consumer reads bytes until the end of the stream is reached, or the input stream is closed.
 * The producer writes bytes to the tail and blocks if the capacity has been reached (until the consumer reads more bytes).
 * <p/>
 * This class is for only 1 producer and 1 consumer; multiple producers/consumers will most likely yield incorrect
 * results !
 * <p/>
 * Note that the implementation of this class is optimized for reading and adding a few K at a time; performance will
 * be suboptimal if single bytes are added and read.
 * @author Bela Ban
 * @since 2.12.2
 */
public class BlockingInputStream extends InputStream {
    /** Set to true when {@link #close()} is called */
    protected boolean         closed=false;

    /** The transfer buffer */
    protected final byte[]    buf;

    /** Index into buf at which the next bytes will be read. Has to be between 0 and buf.length-1 */
    protected int             read_pos=0;

    /** Index into buf at which bytes will be written. Has to be between 0 and buf.length-1 */
    protected int             write_pos=0;

    protected final Lock      lock=new ReentrantLock();

    /** Signalled when the buf becomes 'writeable'; ie. is not full anymore */
    protected final Condition not_full=lock.newCondition();

    /** Signalled when the buf becomes 'readable'; ie. is not empty anymore */
    protected final Condition not_empty=lock.newCondition();


    public BlockingInputStream() {
        this(100000);
    }

    public BlockingInputStream(int capacity) {
        buf=new byte[capacity];
    }




    public int read() throws IOException {
        lock.lock();
        try {
            while(true) {
                if(read_pos < write_pos) {
                    int retval=buf[read_pos++] & 0xff;
                    not_full.signalAll();
                    return retval;
                }
                if(closed)
                    return -1; // EOF
                try {
                    not_empty.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        sanityCheck(b, off, len);

        int bytes_read=0;
        int bytes_to_be_read=Math.min(b.length, len);

        lock.lock();
        try {
            while(bytes_read < bytes_to_be_read) {
                if(read_pos < write_pos) {
                    int bytes_to_read=Math.min(len-bytes_read, size());
                    System.arraycopy(buf, read_pos, b, bytes_read + off, bytes_to_read);
                    read_pos+=bytes_to_read;
                    bytes_read+=bytes_to_read;
                    not_full.signalAll();
                    continue;
                }
                if(closed)
                    return bytes_read > 0? bytes_read : -1;
                try {
                    not_empty.await();
                }
                catch(InterruptedException e) {
                }
            }
            return bytes_read > 0? bytes_read : -1;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Appends bytes to the end of the stream
     * @param buf
     * @throws IOException
     * @see #write(byte[],int,int) 
     */
    public void write(byte[] buf) throws IOException {
        if(buf != null)
            write(buf, 0, buf.length);
    }

    /**
     * Appends bytes to the end of the stream. If the number of bytes to be written is greater than the remaining
     * capacity, write() will block until the bytes can be added, or the stream is closed.
     * <p/>
     * This method will try to append partial buffers to the stream, e.g. if the remaining capacity is 2K, but the
     * length of the buffer is 5K, 2K will be written and then write() will block until the remaining 3K can be
     * added.
     * @param buf The buffer to be added to the end of the stream
     * @param offset The offset within buf at which bytes are read
     * @param length The number of bytes to be added
     * @throws IOException
     */
    public void write(byte[] buf, int offset, int length) throws IOException {
        if(buf == null) return;
        sanityCheck(buf, offset, length);

        lock.lock();
        try {
            if(closed) return;
            int bytes_written=0;
            while(bytes_written < length && !closed) {

                if(read_pos > 0 && length - bytes_written > remaining())
                    compact();

                if(remaining() > 0) {
                    int bytes_to_write=Math.min(length-bytes_written, remaining());
                    System.arraycopy(buf, offset+bytes_written, this.buf, write_pos, bytes_to_write);
                    write_pos+=bytes_to_write;
                    bytes_written+=bytes_to_write;
                    not_empty.signalAll();
                }
                else {
                    try {
                        not_full.await();
                    }
                    catch(InterruptedException e) {
                    }
                }
            }
        }
        finally {
            lock.unlock();
        }
    }




    public long skip(long n) throws IOException {
        throw new IOException("skip() not supported");
    }

    public int available() throws IOException {
        lock.lock();
        try {
            return size();
        }
        finally {
            lock.unlock();
        }
    }


    public int capacity() {
        return buf.length;
    }

    /**
     * Closes the stream. Writes to a closed stream will fail, reads will successfully read the bytes that are already
     * in the buffer and then return -1 (EOF)
     * @throws IOException
     */
    public void close() throws IOException {
        lock.lock();
        try {
            if(closed) return;
            closed=true;
            not_empty.signalAll();
            not_full.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {return closed;} finally {lock.unlock();}
    }

    public String toString() {
        return size() + "/" + capacity() + ", size=" + size() + ", remaining=" + remaining() + (closed? " (closed)" : "");
    }

    @GuardedBy("lock")
    protected int size() {
        return write_pos-read_pos;
    }

    @GuardedBy("lock")
    protected int remaining() {
        return buf.length - write_pos;
    }



    /**
     *  Moves the bytes between [read_pos and write_pos] read_pos bytes to the left, such that the new read_pos is 0
     *  and the write_pos is write_pos - read_pos. Lock must be held.
     */
    @GuardedBy("lock")
    protected void compact() {
        if(read_pos == 0)
            return;
        if(size() == 0) {
            read_pos=write_pos=0;
            return;
        }
        int length=write_pos - read_pos;
        System.arraycopy(buf, read_pos, buf, 0, length);
        write_pos-=read_pos;
        read_pos=0;
        not_full.signalAll();
    }


    /**
     * Verifies that length doesn't exceed a buffer's length
     * @param buf
     * @param offset
     * @param length
     */
    protected static void sanityCheck(byte[] buf, int offset, int length) {
        if(buf == null) throw new NullPointerException("buffer is null");
        if(offset + length > buf.length)
            throw new ArrayIndexOutOfBoundsException("length (" + length + ") + offset (" + offset +
                                                       ") > buf.length (" + buf.length + ")");
    }


}

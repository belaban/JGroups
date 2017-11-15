package org.jgroups.util;

import java.util.Arrays;

/**
 * Implements {@link java.io.DataOutput} over a byte[] buffer. The byte[] buffer expands when needed; however, it
 * doesn't double but only expands minimally, to accommodate the additional data.
 * It is therefore recommended to always size the buffer to the actual number of bytes needed.
 * This class is not thread safe.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteArrayDataOutputStream extends BaseDataOutputStream {
    protected byte[]  buf;
    protected boolean grow_exponentially; // if true, the buffer will double every time

    public ByteArrayDataOutputStream() {
        this(32, false);
    }

    public ByteArrayDataOutputStream(int capacity) {
        this(capacity, false);
    }

    public ByteArrayDataOutputStream(int capacity, boolean grow_exponentially) {
        this.buf=new byte[capacity];
        this.grow_exponentially=grow_exponentially;
    }

    public byte[]                    buffer()                     {return buf;}
    public ByteArray                 getBuffer()                  {return new ByteArray(buf, 0, pos);}
    public boolean                   growExponentially()          {return grow_exponentially;}
    public ByteArrayDataOutputStream growExponentially(boolean b) {grow_exponentially=b; return this;}


    public void write(int b) {
        ensureCapacity(1);
        buf[pos++]=(byte)b;
    }

    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0))
            throw new IndexOutOfBoundsException(String.format("off=%d, len=%d, b.length=%d", off, len, b.length));
        ensureCapacity(len);
        System.arraycopy(b, off, buf, pos, len);
        pos+=len;
    }

    public String toString() {
        return super.toString() + String.format(", lim=%d", buf.length);
    }

    protected int checkBounds(int pos) {
        super.checkBounds(pos);
        if(pos > buf.length)
            throw new IndexOutOfBoundsException("length=" + buf.length);
        return pos;
    }

    /** Grows the buffer; whether it grow linearly or exponentially depends on grow_exponentially */
    protected void ensureCapacity(int bytes) {
        int minCapacity=pos+bytes;

        if(minCapacity - buf.length > 0) {
            int newCapacity=this.grow_exponentially? buf.length << 1 : pos + bytes + 32;
            if(newCapacity - minCapacity < 0)
                newCapacity=minCapacity;
            if(newCapacity < 0) {
                if(minCapacity < 0) // overflow
                    throw new OutOfMemoryError();
                newCapacity=Integer.MAX_VALUE;
            }
            buf=Arrays.copyOf(buf, newCapacity);
        }
    }
}

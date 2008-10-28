package org.jgroups.util;

import java.io.ByteArrayInputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedByteArrayInputStream.java,v 1.1.14.1 2008/10/28 09:39:01 belaban Exp $
 */
public class ExposedByteArrayInputStream extends ByteArrayInputStream {


    /**
     * Creates a <code>ByteArrayInputStream</code>
     * so that it  uses <code>buf</code> as its
     * buffer array.
     * The buffer array is not copied.
     * The initial value of <code>pos</code>
     * is <code>0</code> and the initial value
     * of  <code>count</code> is the length of
     * <code>buf</code>.
     * @param buf the input buffer.
     */
    public ExposedByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Creates <code>ByteArrayInputStream</code>
     * that uses <code>buf</code> as its
     * buffer array. The initial value of <code>pos</code>
     * is <code>offset</code> and the initial value
     * of <code>count</code> is the minimum of <code>offset+length</code>
     * and <code>buf.length</code>.
     * The buffer array is not copied. The buffer's mark is
     * set to the specified offset.
     * @param buf    the input buffer.
     * @param offset the offset in the buffer of the first byte to read.
     * @param length the maximum number of bytes to read from the buffer.
     */
    public ExposedByteArrayInputStream(byte[] buf, int offset, int length) {
        super(buf, offset, length);
    }

    public void setData(byte[] buf, int offset, int length) {
        this.buf=buf;
        this.pos=offset;
        this.count=Math.min(offset + length, buf.length);
        this.mark=offset;
    }


    public  int read() {
        return (pos < count)? (buf[pos++] & 0xff) : -1;
    }

    public  int read(byte b[], int off, int len) {
        if(b == null) {
            throw new NullPointerException();
        }
        else if(off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if(pos >= count) {
            return -1;
        }
        if(pos + len > count) {
            len=count - pos;
        }
        if(len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos+=len;
        return len;
    }


    public  long skip(long n) {
        if(pos + n > count) {
            n=count - pos;
        }
        if(n < 0) {
            return 0;
        }
        pos+=n;
        return n;
    }

    public  int available() {
        return count - pos;
    }

    public  void reset() {
        pos=mark;
    }


}

package org.jgroups.util;

/**
 * Buffer with an offset and length. Will be replaced with NIO equivalent once JDK 1.4 becomes baseline. This class is
 * immutable. Note that the underlying byte[] buffer must <em>not</em> be changed as long as this Buffer instance is in use !
 * @author Bela Ban
 */
public class Buffer {
    private final byte[] buf;
    private final int offset;
    private final int length;

    public Buffer(byte[] buf, int offset, int length) {
        this.buf=buf;
        this.offset=offset;
        this.length=length;
    }

    public Buffer(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public byte[] getBuf() {
        return buf;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public Buffer copy() {
        byte[] new_buf=buf != null? new byte[length] : null;
        int new_length=new_buf != null? new_buf.length : 0;
        if(new_buf != null)
            System.arraycopy(buf, offset, new_buf, 0, length);
        return new Buffer(new_buf, 0, new_length);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder(length + " bytes");
        if(offset > 0)
            sb.append(" (offset=").append(offset).append(")");
        return sb.toString();
    }

}

package org.jgroups.util;

/**
 * Buffer with an offset and length. Will be replaced with NIO equivalent once JDK 1.4 becomes baseline
 * @author Bela Ban
 * @version $Id: Buffer.java,v 1.2 2005/04/11 12:54:08 belaban Exp $
 */
public class Buffer {
    byte[] buf;
    int offset;
    int length;

    public Buffer(byte[] buf, int offset, int length) {
        this.buf=buf;
        this.offset=offset;
        this.length=length;
    }

    public byte[] getBuf() {
        return buf;
    }

    public void setBuf(byte[] buf) {
        this.buf=buf;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset=offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length=length;
    }

    public String toString() {
        StringBuffer sb=new StringBuffer();
        sb.append(buf != null? buf.length : '0').append(" bytes");
        if(offset > 0)
            sb.append("offset=").append(offset).append(", len=").append(length).append(")");
        return sb.toString();
    }
}

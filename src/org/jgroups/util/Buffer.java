package org.jgroups.util;

/**
 * Buffer with an offset and length. Will be replaced with NIO equivalent once JDK 1.4 becomes baseline
 * @author Bela Ban
 * @version $Id: Buffer.java,v 1.1 2004/09/26 11:10:56 belaban Exp $
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
}

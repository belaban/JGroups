package org.jgroups.util;

import java.io.ByteArrayOutputStream;

/**
 * Extends ByteArrayOutputStream, but exposes the internal buffer. This way we don't need to call
 * toByteArray() which copies the internal buffer
 * @author Bela Ban
 * @version $Id: ExposedByteArrayOutputStream.java,v 1.2.14.1 2008/03/27 16:33:38 belaban Exp $
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
        super();
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    /** Resets count and creates a new buf if the current buf is > max_size. This method is not synchronized */
    public void reset(int max_size) {
        super.reset();
        if(buf.length > max_size) {
            buf=new byte[max_size];
        }
    }

    public byte[] getRawBuffer() {
        return buf;
    }

    public int getCapacity() {
        return buf.length;
    }
}

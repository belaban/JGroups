package org.jgroups.util;

import java.io.ByteArrayOutputStream;

/**
 * Extends ByteArrayOutputStream, but exposes the internal buffer. This way we don't need to call
 * toByteArray() which copies the internal buffer
 * @author Bela Ban
 * @version $Id: ExposedByteArrayOutputStream.java,v 1.1 2004/09/26 11:08:25 belaban Exp $
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    public byte[] getRawBuffer() {
        return buf;
    }
}

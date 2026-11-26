package org.jgroups.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Extends {@link OutputStream} but only counts bytes written, doesn't actually write anything. Used to measure size
 * of a marshalled object. Not synchronized.
 * @author Bela Ban
 * @since  5.5.2
 */
public class SizeCountingOutputStream extends OutputStream {
    protected int size; // in bytes

    public int                      size() {return size;}
    public SizeCountingOutputStream reset() {size=0; return this;}

    public void write(int b) throws IOException {
        size++;
    }

    public void write(byte[] b) throws IOException {
        if(b != null)
            size+=b.length;
    }

    public void write(byte[] b, int off, int len) throws IOException {
        size+=len;
    }

    @Override
    public String toString() {
        return String.format("%,d bytes", size);
    }
}

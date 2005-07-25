package org.jgroups.util;

import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedBufferedInputStream.java,v 1.1 2005/07/25 15:53:36 belaban Exp $
 */
public class ExposedBufferedInputStream extends BufferedInputStream {
    /**
     * Creates a <code>BufferedInputStream</code>
     * and saves its  argument, the input stream
     * <code>in</code>, for later use. An internal
     * buffer array is created and  stored in <code>buf</code>.
     *
     * @param in the underlying input stream.
     */
    public ExposedBufferedInputStream(InputStream in) {
        super(in);
    }

    /**
     * Creates a <code>BufferedInputStream</code>
     * with the specified buffer size,
     * and saves its  argument, the input stream
     * <code>in</code>, for later use.  An internal
     * buffer array of length  <code>size</code>
     * is created and stored in <code>buf</code>.
     *
     * @param in   the underlying input stream.
     * @param size the buffer size.
     * @throws IllegalArgumentException if size <= 0.
     */
    public ExposedBufferedInputStream(InputStream in, int size) {
        super(in, size);
    }

    public void reset(int size) {
        count=pos=marklimit=0;
        markpos=-1;
        if(size > buf.length) {
            buf=new byte[size];
        }
    }
}

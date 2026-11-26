package org.jgroups.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * Wraps another input stream and counts the bytes it reads from it. Contrary to {@link SizeCountingOutputStream}, this
 * class delegates reads to the wrapped inputstream.
 * @author Bela Ban
 * @since  5.5.3
 */
public class SizeCountingInputStream extends InputStream {
    protected final InputStream in;
    protected int               position;

    public SizeCountingInputStream(InputStream in) {
        this.in=Objects.requireNonNull(in);
    }

    public int                     position()        {return position;}
    public SizeCountingInputStream position(int pos) {this.position=pos; return this;}

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void close() throws IOException {
        Util.close(in);
    }

    @Override
    public int read() throws IOException {
        int b=in.read();
        if(b == -1)
            return b;
        position++;
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read=in.read(b, off, len);
        if(read > 0)
            position+=read;
        return read;
    }

    public String toString() {
        return String.format("pos=%,d", position);
    }
}

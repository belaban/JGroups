package org.jgroups.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Extends {@link InputStream} from a {@link ByteArrayDataInputStream}. Methods like flush() and close() are no-ops.
 * @author Bela Ban
 * @since  3.5
 */
public class InputStreamAdapter extends InputStream {
    protected final ByteArrayDataInputStream input;

    public InputStreamAdapter(ByteArrayDataInputStream input) {
        this.input=input;
    }


    public int read() throws IOException {
        return input.read();
    }


    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return input.read(b, off, len);
    }


    public long skip(long n) throws IOException {
        return input.skipBytes((int)n);
    }


    public int available() throws IOException {
        return input.limit() - input.position();
    }


    public void close() throws IOException {
        ;
    }


    public void reset() throws IOException {
        ;
    }

    public boolean markSupported() {
        return false;
    }


}

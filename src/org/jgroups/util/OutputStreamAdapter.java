package org.jgroups.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Extends {@link OutputStream} from a {@link ByteArrayDataOutputStream}. Some methods are no-ops.
 * @author Bela Ban
 * @since  3.5
 */
public class OutputStreamAdapter extends OutputStream {
    protected final ByteArrayDataOutputStream output;

    public OutputStreamAdapter(ByteArrayDataOutputStream output) {
        this.output=output;
    }

    public void write(int b) throws IOException {
        output.write(b);
    }

    public void write(byte b[]) throws IOException {
        output.write(b);
    }

    public void write(byte b[], int off, int len) throws IOException {
        output.write(b, off, len);
    }

    public void flush() throws IOException {
        ;
    }

    public void close() throws IOException {
        ;
    }

}

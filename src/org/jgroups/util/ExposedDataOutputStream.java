package org.jgroups.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @version $Id: ExposedDataOutputStream.java,v 1.3 2008/10/28 08:50:15 belaban Exp $
 */
public class ExposedDataOutputStream extends DataOutputStream {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see java.io.FilterOutputStream#out
     */
    public ExposedDataOutputStream(OutputStream out) {
        super(out);
    }

    public void reset() {
        written=0;
    }

    public OutputStream getOutputStream() {
        return out;
    }


    public void write(int b) throws IOException {
        out.write(b);
        incCount(1);
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to the underlying output stream.
     * If no exception is thrown, the counter <code>written</code> is
     * incremented by <code>len</code>.
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream#out
     */
    public void write(byte b[], int off, int len)
            throws IOException {
        out.write(b, off, len);
        incCount(len);
    }


    private void incCount(int value) {
        int temp=written + value;
        if(temp < 0) {
            temp=Integer.MAX_VALUE;
        }
        written=temp;
    }
}

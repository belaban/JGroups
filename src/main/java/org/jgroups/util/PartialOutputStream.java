package org.jgroups.util;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Decorates an output stream ({@link java.io.DataOutput}) and writes only a subset [start..end] (both indices
 * inclusive) of the original data to the underlying output stream.<br/>
 * Example: <pre>start=5, length=5: range=[5..9], bytes are written for positions 5,6,7,8,9 (5 bytes)</pre>
 * @author Bela Ban
 * @since  5.0
 */
public class PartialOutputStream extends BaseDataOutputStream {
    protected final DataOutput  out;
    protected final int offset, end;

    public PartialOutputStream(DataOutput out, int offset, int length) {
        this.out=out;
        this.offset=offset;
        this.end=offset+length-1;
    }

    public void write(int b) {
        if(pos > end)     // greater than range
            return;
        if(pos < offset)   // smaller than range
            pos++;
        else              // in range
            _write(b);
    }

    public void write(byte[] b, int off, int len) {
        if(pos > end)      // greater than range
            return;
        if(pos < offset) { // smaller than range
            int bytess_to_skip=offset-pos;
            if(bytess_to_skip >= len)
                pos+=len;
            else {
                pos+=bytess_to_skip;
                write(b, off + bytess_to_skip, len - bytess_to_skip);
            }
        }
        else {             // in range
            int bytes_to_be_written=Math.min(len, remaining());
            _write(b, off, bytes_to_be_written);
        }
    }

    /** Number of bytes left to be written */
    public int remaining() {
        int start=Math.max(offset, pos);
        return Math.max(0, end-start+1);
    }

    public String toString() {
        return String.format("%s, range=[%d..%d] (remaining=%d) (%s)",
                             super.toString(), offset, end, remaining(), inRange()? "in range" : "outside range");
    }

    protected void ensureCapacity(int bytes) {
    }

    protected void _write(int b) {
        try {
            out.write(b);
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            pos++;
        }
    }

    protected void _write(byte[] b, int off, int len) {
        try {
            out.write(b, off, len);
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            pos+=len;
        }
    }

    /** Checks if pos is in range [offset..end] */
    protected boolean inRange() {
        return pos >= offset && pos <= end;
    }
}

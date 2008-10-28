package org.jgroups.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Extends ByteArrayOutputStream, but exposes the internal buffer. This way we don't need to call
 * toByteArray() which copies the internal buffer
 * @author Bela Ban
 * @version $Id: ExposedByteArrayOutputStream.java,v 1.4 2008/10/28 08:50:15 belaban Exp $
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {
        super();
    }

    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * Resets count and creates a new buf if the current buf is > max_size. This method is not synchronized
     */
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


    public void write(int b) {
        int newcount=count + 1;
        if(newcount > buf.length) {
            buf=Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        buf[count]=(byte)b;
        count=newcount;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this byte array output stream.
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    public void write(byte b[], int off, int len) {
        if((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        else if(len == 0) {
            return;
        }
        int newcount=count + len;
        if(newcount > buf.length) {
            buf=Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        System.arraycopy(b, off, buf, count, len);
        count=newcount;
    }

    /**
     * Writes the complete contents of this byte array output stream to
     * the specified output stream argument, as if by calling the output
     * stream's write method using <code>out.write(buf, 0, count)</code>.
     * @param out the output stream to which to write the data.
     * @throws java.io.IOException if an I/O error occurs.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }

    /**
     * Resets the <code>count</code> field of this byte array output
     * stream to zero, so that all currently accumulated output in the
     * output stream is discarded. The output stream can be used again,
     * reusing the already allocated buffer space.
     * @see java.io.ByteArrayInputStream#count
     */
    public void reset() {
        count=0;
    }

    /**
     * Creates a newly allocated byte array. Its size is the current
     * size of this output stream and the valid contents of the buffer
     * have been copied into it.
     * @return the current contents of this output stream, as a byte array.
     * @see java.io.ByteArrayOutputStream#size()
     */
    public byte toByteArray()[] {
        return Arrays.copyOf(buf, count);
    }

    /**
     * Returns the current size of the buffer.
     * @return the value of the <code>count</code> field, which is the number
     *         of valid bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return count;
    }

    /**
     * Converts the buffer's contents into a string decoding bytes using the
     * platform's default character set. The length of the new <tt>String</tt>
     * is a function of the character set, and hence may not be equal to the
     * size of the buffer.
     * <p/>
     * <p> This method always replaces malformed-input and unmappable-character
     * sequences with the default replacement string for the platform's
     * default character set. The {@linkplain java.nio.charset.CharsetDecoder}
     * class should be used when more control over the decoding process is
     * required.
     * @return String decoded from the buffer's contents.
     * @since JDK1.1
     */
    public String toString() {
        return new String(buf, 0, count);
    }

    /**
     * Converts the buffer's contents into a string by decoding the bytes using
     * the specified {@link java.nio.charset.Charset charsetName}. The length of
     * the new <tt>String</tt> is a function of the charset, and hence may not be
     * equal to the length of the byte array.
     * <p/>
     * <p> This method always replaces malformed-input and unmappable-character
     * sequences with this charset's default replacement string. The {@link
     * java.nio.charset.CharsetDecoder} class should be used when more control
     * over the decoding process is required.
     * @param charsetName the name of a supported
     *                    {@linkplain java.nio.charset.Charset </code>charset<code>}
     * @return String decoded from the buffer's contents.
     * @throws java.io.UnsupportedEncodingException
     *          If the named charset is not supported
     * @since JDK1.1
     */
    public String toString(String charsetName)
            throws UnsupportedEncodingException {
        return new String(buf, 0, count, charsetName);
    }

    /**
     * Creates a newly allocated string. Its size is the current size of
     * the output stream and the valid contents of the buffer have been
     * copied into it. Each character <i>c</i> in the resulting string is
     * constructed from the corresponding element <i>b</i> in the byte
     * array such that:
     * <blockquote><pre>
     *     c == (char)(((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
     * </pre></blockquote>
     * @param hibyte the high byte of each resulting Unicode character.
     * @return the current contents of the output stream, as a string.
     * @see java.io.ByteArrayOutputStream#size()
     * @see java.io.ByteArrayOutputStream#toString(String)
     * @see java.io.ByteArrayOutputStream#toString()
     * @deprecated This method does not properly convert bytes into characters.
     *             As of JDK&nbsp;1.1, the preferred way to do this is via the
     *             <code>toString(String enc)</code> method, which takes an encoding-name
     *             argument, or the <code>toString()</code> method, which uses the
     *             platform's default character encoding.
     */
    @Deprecated
    public String toString(int hibyte) {
        return new String(buf, hibyte, 0, count);
    }


}

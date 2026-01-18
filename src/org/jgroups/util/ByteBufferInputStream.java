package org.jgroups.util;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Class using {@link ByteBuffer} and implementing {@link DataInput}.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteBufferInputStream extends InputStream implements DataInput {
    protected final ByteBuffer buf;

    public ByteBufferInputStream(ByteBuffer buf) {
        this.buf=buf;
    }

    public ByteBuffer buf() {return buf;}

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        try {
            buf.get(b, off, len);
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int skip=Math.min(n, buf.limit() - buf.position());
        buf.position(buf.position() + skip);
        return skip;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() == 1;
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return buf.get();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public int read() {
        return !buf.hasRemaining() ? -1 : (buf.get() & 0xff);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(b, off, len);
        return len;
    }

    @Override
    public int available() {
        return buf.remaining();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        buf.mark();
    }

    @Override
    public synchronized void reset() {
        try {
            buf.reset();
        } catch (java.nio.InvalidMarkException e) {}
    }

    @Override
    public int readUnsignedByte() throws IOException {
        try {
            return buf.get() & 0xff;
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return buf.getShort();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        try {
            return buf.getShort() & 0xffff;
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public char readChar() throws IOException {
        try {
            return buf.getChar();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return buf.getInt();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return buf.getLong();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public float readFloat() throws IOException {
        try {
            return buf.getFloat();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public double readDouble() throws IOException {
        try {
            return buf.getDouble();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    @Override
    public String readLine() throws IOException {
        if(!buf.hasRemaining())
            return null;

        char[] lineBuffer=new char[128];
        char[] buffer= lineBuffer;

        int room = buffer.length;
        int offset = 0;
        int c;

        loop:	while (true) {
            if (!buf.hasRemaining()) {
                c = -1;
                break;
            }
            
            c = buf.get() & 0xff;
            
            switch (c) {
                case -1:
                case '\n':
                    break loop;

                case '\r':
                    if (buf.hasRemaining()) {
                        buf.mark();
                        int c2 = buf.get() & 0xff;
                        if (c2 != '\n') {
                            // Not a line feed, reset to after the CR
                            buf.reset();
                        }
                    }
                    break loop;

                default:
                    if (--room < 0) {
                        buffer = new char[offset + 128];
                        room = buffer.length - offset - 1;
                        System.arraycopy(lineBuffer, 0, buffer, 0, offset);
                        lineBuffer = buffer;
                    }
                    buffer[offset++] = (char) c;
                    break;
            }
        }
        if ((c == -1) && (offset == 0)) {
            return null;
        }
        return String.copyValueOf(buffer, 0, offset);
    }

    @Override
    public String readUTF() throws IOException {
        int utflen = readUnsignedShort();
        byte[] bytearr = new byte[utflen];
        char[] chararr = new char[utflen];

        int c, char2, char3;
        int count = 0;
        int chararr_count=0;

        readFully(bytearr,0,utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) break;
            count++;
            chararr[chararr_count++]=(char)c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++]=(char)c;
                    break;
                case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                          "malformed input: partial character at end");
                    char2 =bytearr[count-1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                          "malformed input around byte " + count);
                    chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
                      (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                          "malformed input: partial character at end");
                    char2 =bytearr[count-2];
                    char3 =bytearr[count-1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                          "malformed input around byte " + (count-1));
                    chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                      ((char2 & 0x3F) << 6)  |
                      ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException(
                      "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
    }

    @Override
    public String toString() {
        return buf.toString();
    }
}
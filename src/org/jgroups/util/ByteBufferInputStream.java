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

    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        try {
            buf.get(b, off, len);
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }


    public int skipBytes(int n) throws IOException {
        int skip=Math.min(n, buf.limit() - buf.position());
        buf.position(buf.position() + skip);
        return skip;
    }

    public boolean readBoolean() throws IOException {
        return readByte() == 1;
    }

    public byte readByte() throws IOException {
        try {
            return buf.get();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public int read() {
        return !buf.hasRemaining() ? -1 : (buf.get() & 0xff);
    }

    public int readUnsignedByte() throws IOException {
        int ch=readByte() & 0xff;
        if(ch < 0)
            throw new EOFException();
        return ch;
    }

    public short readShort() throws IOException {
        try {
            return buf.getShort();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public int readUnsignedShort() throws IOException {
        int retval=readShort() & 0xffff;
        if(retval < 0)
            throw new EOFException();
        return retval;
    }

    public char readChar() throws IOException {
        try {
            return buf.getChar();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public int readInt() throws IOException {
        try {
            return buf.getInt();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public long readLong() throws IOException {
        try {
            return buf.getLong();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public float readFloat() throws IOException {
        try {
            return buf.getFloat();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public double readDouble() throws IOException {
        try {
            return buf.getDouble();
        } catch (BufferUnderflowException e) {
            throw new EOFException();
        }
    }

    public String readLine() throws IOException {
        char[] lineBuffer=new char[128];
        char buffer[] = lineBuffer;

        if (buffer == null) {
            buffer = lineBuffer = new char[128];
        }

        int room = buffer.length;
        int offset = 0;
        int c;

        loop:	while (true) {
            switch (c = readByte()) {
                case -1:
                case '\n':
                    break loop;

                case '\r':
                    int c2 = readByte();
                    if ((c2 != '\n') && (c2 != -1))
                        ;
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
                    char2 = (int) bytearr[count-1];
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
                    char2 = (int) bytearr[count-2];
                    char3 = (int) bytearr[count-1];
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

    public String toString() {
        return buf.toString();
    }
}

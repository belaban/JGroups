package org.jgroups.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implements {@link java.io.DataInput} over a byte[] buffer. This class is not thread safe.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteArrayDataInputStream extends InputStream implements DataInput {
    protected final byte[] buf;
    protected int          pos;   // current position to read next byte from buf

    // index of last byte to be read, reading beyond will return -1 or throw EOFException. Limit has to be <= buf.length
    protected final int    limit;

    public ByteArrayDataInputStream(byte[] buf) {
        this(buf, 0, buf != null? buf.length : 0);
    }

    public ByteArrayDataInputStream(byte[] buf, int offset, int length) {
        this.buf=buf;
        this.limit=Math.min(buf.length, offset+length);
        this.pos=checkBounds(offset);
    }

    public ByteArrayDataInputStream(ByteBuffer buffer) {
        int offset=buffer.hasArray()? buffer.arrayOffset() + buffer.position() : buffer.position(),
          len=buffer.remaining();
        if(!buffer.isDirect()) {
            this.buf=buffer.array();
            this.pos=offset;
            this.limit=offset+len;
        }
        else { // by default use a copy; but of course implementers of Receiver can override this
            byte[] tmp=new byte[len];
            buffer.get(tmp, 0, len);
            this.buf=tmp;
            this.pos=0;
            this.limit=len;
        }
    }

    public ByteArrayDataInputStream position(int pos) {
        this.pos=checkBounds(pos); return this;
    }

    public int position() {return pos;}
    public int limit()    {return limit;}
    public int capacity() {return buf.length;}



    /**
     * Reads the next byte of data from buf. The value byte is returned as an {@code int} in the range {@code 0} to
     * {@code 255}. If no byte is available because the end of the buffer has been reached, the value {@code -1} is
     * returned.
     * @return  the next byte of data, or {@code -1} if the end of the stream has been reached.
     */
    public int read() {
        return (pos < limit) ? (buf[pos++] & 0xff) : -1;
    }

    public int read(byte[] b, int off, int len) {
        Objects.requireNonNull(b);

        if(off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if(pos >= limit)
            return -1;

        int avail=limit - pos;
        if(len > avail)
            len=avail;
        if(len <= 0)
            return 0;

        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count=read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    public int skipBytes(int n) {
        int k = limit - pos;
        if (n < k)
            k=Math.max(n, 0);
        pos += k;
        return k;
    }

    public boolean readBoolean() throws IOException {
        int ch=read();
        if(ch < 0)
            throw new EOFException();
        return ch != 0;
    }

    public byte readByte() throws IOException {
        int ch=read();
        if (ch < 0)
            throw new EOFException();
        return (byte)(ch);
    }

    public int readUnsignedByte() throws IOException {
        int ch=read();
        if (ch < 0)
            throw new EOFException();
        return ch;
    }

    public short readShort() throws IOException {
        int ch1=read();
        int ch2=read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    public int readUnsignedShort() throws IOException {
        int ch1=read();
        int ch2=read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    public char readChar() throws IOException {
        int ch1=read();
        int ch2=read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    public int readInt() throws IOException {
        int ch1=read();
        int ch2=read();
        int ch3=read();
        int ch4=read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public long readLong() throws IOException {
        return (((long)read() << 56) +
          ((long)(read() & 0xff) << 48) +
          ((long)(read() & 0xff) << 40) +
          ((long)(read() & 0xff) << 32) +
          ((long)(read() & 0xff) << 24) +
          ((read() & 0xff) << 16) +
          ((read() & 0xff) <<  8) +
          ((read() & 0xff) <<  0));
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        StringBuilder sb=new StringBuilder(35);
        int ch;

        while(true) {
            ch=read();
            if(ch == -1)
                return sb.length() == 0? null : sb.toString();
            if(ch == '\r')
                ;
            else {
                if(ch == '\n')
                    break;
                sb.append((char)ch);
            }
        }
        return sb.toString();
    }

    public String readUTF() throws IOException {
        int utflen=readUnsignedShort();
        byte[] bytearr=new byte[utflen];
        char[] chararr=new char[utflen];

        if(((short)utflen) == -1)
            return null;

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
                    char2=bytearr[count-1];
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
                    char2=bytearr[count-2];
                    char3=bytearr[count-1];
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
        return "pos=" + pos + " lim=" + limit + " cap=" + buf.length;
    }

    protected int checkBounds(int pos) {
        if(pos < 0 || pos >= limit)
            throw new IndexOutOfBoundsException("pos=" + pos + ", limit=" + limit);
        return pos;
    }
}

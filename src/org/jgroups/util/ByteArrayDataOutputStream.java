package org.jgroups.util;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Implements {@link java.io.DataOutput} over a byte[] buffer. The byte[] buffer expands when needed; however, it
 * doesn't double but only expands minimally, to accommodate the additional data.
 * It is therefore recommended to always size the buffer to the actual number of bytes needed.
 * This class is not thread safe.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteArrayDataOutputStream implements DataOutput {
    protected byte[]  buf;
    protected int     pos;
    protected boolean grow_exponentially; // if true, the buffer will double every time

    public ByteArrayDataOutputStream() {
        this(32, false);
    }

    public ByteArrayDataOutputStream(int capacity) {
        this(capacity, false);
    }

    public ByteArrayDataOutputStream(int capacity, boolean grow_exponentially) {
        this.buf=new byte[capacity];
        this.grow_exponentially=grow_exponentially;
    }

    public ByteArrayDataOutputStream position(int pos)            {this.pos=checkBounds(pos); return this;}
    public int                       position()                   {return pos;}
    public byte[]                    buffer()                     {return buf;}
    public Buffer                    getBuffer()                  {return new Buffer(buf, 0, pos);}
    public ByteBuffer                getByteBuffer()              {return ByteBuffer.wrap(buf, 0, pos);}
    public boolean                   growExponentially()          {return grow_exponentially;}
    public ByteArrayDataOutputStream growExponentially(boolean b) {grow_exponentially=b; return this;}


    public void write(int b) {
        ensureCapacity(1);
        buf[pos++]=(byte)b;
    }

    public void write(byte[] b) {
        write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) {
        if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0))
            throw new IndexOutOfBoundsException(String.format("off=%d, len=%d, b.length=%d", off, len, b.length));
        ensureCapacity(len);

        System.arraycopy(b, off, buf, pos, len);
        pos+=len;
    }

    public void writeBoolean(boolean v) {
        write(v ? 1 : 0);
    }

    public void writeByte(int v) {
        write(v);
    }

    public void writeShort(int v) {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeChar(int v) {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeInt(int v) {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>>  8) & 0xFF);
        write((v >>>  0) & 0xFF);
    }

    public void writeLong(long v) {
        write((byte)(v >>> 56));
        write((byte)(v >>> 48));
        write((byte)(v >>> 40));
        write((byte)(v >>> 32));
        write((byte)(v >>> 24));
        write((byte)(v >>> 16));
        write((byte)(v >>>  8));
        write((byte)(v >>>  0));
    }

    public void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) {
        int len=s != null? s.length() : 0;
        if(len > 0)
            ensureCapacity(len);
        for(int i = 0 ; i < len ; i++)
            write((byte)s.charAt(i));
    }

    public void writeChars(String s) {
        int len=s != null? s.length() : 0;
        if(len > 0)
            ensureCapacity(len *2); // 2 bytes per char

        for(int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            writeChar(v);
        }
    }

    public void writeUTF(String str) {
        int strlen=str != null? str.length() : 0;
        if(strlen > 0)
            ensureCapacity(strlen *2 + 2);

        int utflen = 0;
        int c, count = 0;

        if(str == null) {
            writeShort(-1);
            return;
        }

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }

        if (utflen > 65535)
            throw new IllegalArgumentException("encoded string too long: " + utflen + " bytes");

        byte[] bytearr=new byte[utflen+2];

        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

        int i=0;
        for (i=0; i<strlen; i++) {
            c = str.charAt(i);
            if (!((c >= 0x0001) && (c <= 0x007F))) break;
            bytearr[count++] = (byte) c;
        }

        for (;i < strlen; i++){
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        write(bytearr, 0, utflen+2);
    }

    public String toString() {
        return "pos=" + pos + " lim=" + buf.length;
    }

    protected int checkBounds(int pos) {
        if(pos < 0 || pos > buf.length)
            throw new IndexOutOfBoundsException("pos=" + pos + ", length=" + buf.length);
        return pos;
    }

    /** Grows the buffer; whether it grow linearly or exponentially depends on grow_exponentially */
    protected void ensureCapacity(int bytes) {
        int minCapacity=pos+bytes;

        if(minCapacity - buf.length > 0) {
            int newCapacity=this.grow_exponentially? buf.length << 1 : pos + bytes + 32;
            if(newCapacity - minCapacity < 0)
                newCapacity=minCapacity;
            if(newCapacity < 0) {
                if(minCapacity < 0) // overflow
                    throw new OutOfMemoryError();
                newCapacity=Integer.MAX_VALUE;
            }
            // System.out.printf("growing buffer from %d -> %d (pos=%d, bytes=%d)\n", buf.length, newCapacity, pos, bytes);
            buf=Arrays.copyOf(buf, newCapacity);
        }
    }
}

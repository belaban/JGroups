package org.jgroups.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Class using {@link ByteBuffer} and implementing {@link DataOutput}. The underlying ByteBuffer will be grown when
 * the capacity is too small.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteBufferOutputStream implements DataOutput {
    protected ByteBuffer buf;
    protected boolean    grow_exponentially; // if true, the buffer will double up to a certain size
    protected boolean    use_direct_memory;  // direct or heap memory

    public ByteBufferOutputStream(ByteBuffer buf) {
        this.buf=Objects.requireNonNull(buf);
        this.use_direct_memory=buf.isDirect();
    }

    public ByteBufferOutputStream() {
        this(32, false, false);
    }

    public ByteBufferOutputStream(int capacity) {
        this(capacity, false, false);
    }

    public ByteBufferOutputStream(int capacity, boolean grow_exponentially, boolean use_direct_memory) {
        this.grow_exponentially=grow_exponentially;
        this.use_direct_memory=use_direct_memory;
        this.buf=use_direct_memory? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    public ByteBuffer             buf()                        {return buf;}
    public int                    position()                   {return buf.position();}
    public int                    limit()                      {return buf.limit();}
    public int                    capacity()                   {return buf.capacity();}
    public boolean                growExponentially()          {return grow_exponentially;}
    public ByteBufferOutputStream growExponentially(boolean b) {grow_exponentially=b; return this;}
    public boolean                useDirectMemory()            {return use_direct_memory;}

    public void write(int b) throws IOException {
        ensureCapacity(1);
        buf.put((byte)b);
    }

    public void write(byte[] b) throws IOException {
        write(b,0,b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        ensureCapacity(len);
        buf.put(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        ensureCapacity(1);
        write(v? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        ensureCapacity(2);
        buf.putShort((short)v);
    }

    public void writeChar(int v) throws IOException {
        ensureCapacity(2);
        buf.putChar((char)v);
    }

    public void writeInt(int v) throws IOException {
        ensureCapacity(Integer.BYTES);
        buf.putInt(v);
    }

    public void writeLong(long v) throws IOException {
        ensureCapacity(Long.BYTES);
        buf.putLong(v);
    }

    public void writeFloat(float v) throws IOException {
        ensureCapacity(Integer.BYTES);
        buf.putFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        ensureCapacity(Long.BYTES);
        buf.putDouble(v);
    }

    public void writeBytes(String s) throws IOException {
        int len=s != null? s.length() : 0;
        if(len > 0) {
            ensureCapacity(len);
            for(int i=0; i < len; i++)
                write((byte)s.charAt(i));
        }
    }

    public void writeChars(String s) throws IOException {
        int len=s != null? s.length() : 0;
        if(len > 0) {
            ensureCapacity(len*2); // 2 bytes per char is possible
            for(int i=0; i < len; i++) {
                int v=s.charAt(i);
                write((v >>> 8) & 0xFF);
                write((v >>> 0) & 0xFF);
            }
        }
    }

    public void writeUTF(String str) throws IOException {
        if(str == null) {
            writeShort(-1);
            return;
        }

        int strlen=str != null? str.length() : 0;

        if(strlen > 0)
            ensureCapacity(strlen);

        int utflen=0, c=0, count = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");

        writeShort(utflen);

        byte[] bytearr=new byte[utflen];

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
        write(bytearr,0,utflen);
    }

    public String toString() {
        return buf.toString();
    }

    /** Grows the buffer; whether linearly or exponentially depends on grow_exponentially */
    protected void ensureCapacity(int bytes) {
        int minCapacity=buf.position()+bytes;
        if(minCapacity - buf.capacity() > 0) {
            int newCapacity=this.grow_exponentially? buf.capacity() << 1 : buf.position() + bytes + 32;
            if(newCapacity - minCapacity < 0)
                newCapacity=minCapacity;
            if(newCapacity < 0) {
                if(minCapacity < 0) // overflow
                    throw new OutOfMemoryError();
                newCapacity=Integer.MAX_VALUE;
            }
            ByteBuffer tmp=use_direct_memory? ByteBuffer.allocateDirect(newCapacity) : ByteBuffer.allocate(newCapacity);
            buf.flip();
            tmp.put(buf);
            buf=tmp;
        }
    }
}

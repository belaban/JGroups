package org.jgroups.util;

import java.io.DataOutput;

/**
 * Implements {@link DataOutput} in a generic manner, so that subclasses can implement specific behavior. Some write()
 * methods are abstract and need to be implemennted in subclasses. Filed {@link #pos} points to the current
 * write-position.
 * @author Bela Ban
 * @since  5.0
 */
public abstract class BaseDataOutputStream implements DataOutput {
    protected int     pos;

    public BaseDataOutputStream  position(int pos) {this.pos=checkBounds(pos); return this;}
    public int                   position()        {return pos;}

    public abstract void  write(int b);
    public void           write(byte[] b) {write(b, 0, b.length);}
    public abstract void  write(byte[] b, int off, int len);

    public void writeBoolean(boolean v) {
        ensureCapacity(1);
        write(v ? 1 : 0);
    }

    public void writeByte(int v) {
        ensureCapacity(1);
        write(v);
    }

    public void writeShort(int v) {
        ensureCapacity(2);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeChar(int v) {
        ensureCapacity(2);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeInt(int v) {
        ensureCapacity(4);
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>>  8) & 0xFF);
        write((v >>>  0) & 0xFF);
    }

    public void writeLong(long v) {
        ensureCapacity(8);
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
        if(len > 0) {
            ensureCapacity(len);
            for(int i=0; i < len; i++)
                write((byte)s.charAt(i));
        }
    }

    public void writeChars(String s) {
        int len=s != null? s.length() : 0;
        if(len > 0) {
            ensureCapacity(len * 2); // 2 bytes per char
            for(int i=0; i < len; i++) {
                int v=s.charAt(i);
                writeChar(v);
            }
        }
    }

    public void writeUTF(String str) {
        int strlen=str != null? str.length() : 0;
        if(strlen > 0)
            ensureCapacity(strlen);

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

    public String toString() {return String.format("pos=%d", pos);}

    protected int checkBounds(int pos) {
        if(pos < 0)
            throw new IndexOutOfBoundsException("pos=" + pos);
        return pos;
    }

    /** Makes sure that bytes can be written to the output stream (e.g. a byte array has enough space left) */
    protected abstract void ensureCapacity(int bytes);
}

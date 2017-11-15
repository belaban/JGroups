package org.jgroups.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

/**
 * Class using {@link ByteBuffer} and implementing {@link DataOutput}. Note that all instances need to pass the
 * exact size of the marshalled object as a ByteBuffer cannot be expanded.
 * @author Bela Ban
 * @since  3.5
 */
public class ByteBufferOutputStream implements DataOutput {
    protected final ByteBuffer buf;


    public ByteBufferOutputStream(ByteBuffer buf) {
        this.buf=buf;
    }

    public void reset() {buf.clear();}

    public ByteBuffer getBuffer() {return buf;}

    public ByteArray getBufferAsBuffer() {
        return new ByteArray(buf.array(), buf.arrayOffset(), buf.position());
    }


    public void write(int b) throws IOException {
        buf.put((byte)b);
    }

    public void write(byte[] b) throws IOException {
        write(b,0,b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        buf.put(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        write(v? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        write(v);
    }

    public void writeShort(int v) throws IOException {
        buf.putShort((short)v);
    }

    public void writeChar(int v) throws IOException {
        buf.putChar((char)v);
    }

    public void writeInt(int v) throws IOException {
        buf.putInt(v);
    }

    public void writeLong(long v) throws IOException {
        buf.putLong(v);
    }

    public void writeFloat(float v) throws IOException {
        buf.putFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        buf.putDouble(v);
    }

    public void writeBytes(String s) throws IOException {
        int len=s.length();
        for(int i = 0 ; i < len ; i++)
            write((byte)s.charAt(i));
    }

    public void writeChars(String s) throws IOException {
        int len=s.length();
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v >>> 0) & 0xFF);
        }
    }

    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c, count = 0;

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
            throw new UTFDataFormatException(
              "encoded string too long: " + utflen + " bytes");

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
        write(bytearr,0,utflen + 2);
    }

    public String toString() {
        return buf.toString();
    }
}

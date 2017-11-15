package org.jgroups.util;

import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class (similar to (and partly copied from) java.nio.Bits) containing helper methods to encode variables
 * (e.g. ints, long, List&lt;Address&gt; etc) to memory (byte buffer) or output streams and read variables
 * from memory or input streams.<p/>
 * The write methods write a type (e.g. an int or a char) to a buffer ({@link ByteBuffer} or output stream, using
 * <a href="https://developers.google.com/protocol-buffers/docs/encoding">variable-length encoding</a>. If
 * there are not enough byte in the buffer to write a type, a {@link java.nio.BufferOverflowException} is thrown.
 * If the variable cannot be written to the output stream, an IOException is thrown.
 * <p/>
 * The read methods read a variable-length encoded type from a buffer or input stream. If there are fewer bytes in
 * the buffer than needed to read the type, a {@link java.nio.BufferUnderflowException} is thrown. If the read fails,
 * an IOException is thrown.
 * <p/>
 * The size() methods return the number of bytes used to encode the given type with variable-length encoding.
 * <p/>
 * There are additional helper methods to write/read custom JGroups types, e.g. address lists, Views etc
 * <p/>
 * Note that methods to read/write atomic types (char, int etc) should only be used if variable-length encoding is
 * desired; otherwise {@link DataOutput#writeInt(int)} or {@link ByteBuffer#putInt(int)} should be used instead.
 * <p/>
 * At the time of writing this (Feb 2014), most methods have not yet been implemented.
 * @author Bela Ban
 * @author Sanne Grinovero
 * @since  3.5
 */
public final class Bits {

	private Bits() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    // -------------------- char ------------------------ //

    // No compression of chars as they only use 2 bytes
    public static char makeChar(byte[] buf, int offset) {
        return (char) ((buf[offset + 1] & 0xFF) + (buf[offset] << 8));
    }

    static private char makeChar(byte b1, byte b0) {
        return (char)((b1 << 8) | (b0 & 0xff));
    }

    public static void writeChar(char c, byte[] buf, int offset) {
        buf[offset+1]=(byte)c;
        buf[offset]=(byte)(c >>> 8);
    }

    public static char readChar(byte[] buf, int offset) {
        return makeChar(buf, offset);
    }

    public static char readChar(ByteBuffer buf) {
	    byte a=buf.get(), b=buf.get();
	    return makeChar(b, a);
    }

    // -------------------- short ----------------------- //

    // No implementations as encoding a char doesn't use much space

    public static short makeShort(byte a, byte b) {
        return (short)((a << 8) | (b & 0xff));
    }

    public static short makeShort(byte a) {
        return (short) (a & 0xff);
    }

    public static void writeShort(short s, byte[] buf, int offset) {
        buf[offset+1]=(byte)s;
        buf[offset]=(byte)(s >>> 8);
    }

    public static short readShort(byte[] buf, int offset) {
        return (short)((buf[offset+1] & 0xFF) + (buf[offset] << 8));
    }

    public static short readShort(ByteBuffer buf) {
	    return makeShort(buf.get(), buf.get());
    }

    // --------------------- int ------------------------ //


    /**
     * Writes an int to a ByteBuffer
     * @param num the int to be written
     * @param buf the buffer
     */
    public static void writeInt(int num, ByteBuffer buf) {
        if(num == 0) {
            buf.put((byte)0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        buf.put(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            buf.put(getByteAt(num, i));
    }

    /**
     * Writes an int to an output stream
     * @param num the int to be written
     * @param out the output stream
     */
    public static void writeInt(int num, DataOutput out) throws IOException {
        if(num == 0) {
            out.write(0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        out.write(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            out.write(getByteAt(num, i));
    }

    public static void writeInt(int num, byte[] buf, int offset) {
        buf[offset+3]=(byte)num;
        buf[offset+2]=(byte)(num >>>  8);
        buf[offset+1]=(byte)(num >>> 16);
        buf[offset]=(byte)(num >>> 24);
    }

    public static void writeIntCompressed(int num, byte[] buf, int offset) {
        if(num == 0) {
            buf[offset]=0;
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        buf[offset++]=bytes_needed;
        for(int i=0; i < bytes_needed; i++)
            buf[offset++]=getByteAt(num, i);
    }


    /**
     * Reads an int from a buffer.
     * @param buf the buffer
     * @return the int read from the buffer
     */
    public static int readInt(ByteBuffer buf) {
        byte len=buf.get();
        if(len == 0)
            return 0;
        return makeInt(buf, len);
    }

    /**
     * Reads an int from an input stream
     * @param in the input stream
     * @return the int read from the input stream
     */
    public static int readInt(DataInput in) throws IOException {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        return makeInt(in, len);
    }

    public static int readInt(byte[] buf, int offset) {
        return ((buf[offset+3] & 0xFF)) +
          ((buf[offset+2] & 0xFF) <<  8) +
          ((buf[offset+1] & 0xFF) << 16) +
          ((buf[offset]) << 24);
    }

    public static int readIntCompressed(byte[] buf, int offset) {
        byte len=buf[offset++];
        if(len == 0)
            return 0;
        return makeInt(buf, offset, len);
    }


    /**
     * Computes the size of a variable-length encoded int
     * @param num the int
     * @return the number of bytes needed to variable-length encode num
     */
    public static int size(int num) {
        return (byte)(num == 0? 1 : bytesRequiredFor(num) +1);
    }




    // -------------------- long ------------------------ //

    /**
     * Writes a long to a ByteBuffer
     * @param num the long to be written
     * @param buf the buffer
     */
    public static void writeLong(long num, ByteBuffer buf) {
        if(num == 0) {
            buf.put((byte)0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        buf.put(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            buf.put(getByteAt(num, i));
    }

    /**
     * Writes a long to out in variable-length encoding. Note that currently variable-length encoding is <em>not</em>
     * used (a similar mechanism is used); this will be implemented later.
     * @param num the long
     * @param out the output stream to write num to
     * @throws IOException
     */
    public static void writeLong(final long num, final DataOutput out) throws IOException {
        if(num == 0) {
            out.write(0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        out.write(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            out.write(getByteAt(num, i));
    }

    public static void writeLong(long num, byte[] buf, int offset) {
        buf[offset+7]=(byte)num;
        buf[offset+6]=(byte)(num >>>  8);
        buf[offset+5]=(byte)(num >>> 16);
        buf[offset+4]=(byte)(num >>> 24);
        buf[offset+3]=(byte)(num >>> 32);
        buf[offset+2]=(byte)(num >>> 40);
        buf[offset+1]=(byte)(num >>> 48);
        buf[offset]=(byte)(num >>> 56);
    }

    public static void writeLongCompressed(long num, byte[] buf, int offset) {
        if(num == 0) {
            buf[offset]=0;
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        buf[offset++]=bytes_needed;
        for(int i=0; i < bytes_needed; i++)
            buf[offset++]=getByteAt(num, i);
    }



    /**
     * Reads a long from a buffer.
     * @param buf the buffer
     * @return the long read from the buffer
     */
    public static long readLong(ByteBuffer buf) {
        byte len=buf.get();
        if(len == 0)
            return 0;
        return makeLong(buf, len);
    }

    /**
     * Reads a variable-length encoded long from an input stream.  Note that currently variable-length encoding is <em>not</em>
     * used (a similar mechanism is used); this will be implemented later.
     * @param in the input stream
     * @return the long read from the input stream
     * @throws IOException
     */
    public static long readLong(DataInput in) throws IOException {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        return makeLong(in, len);
    }

    public static long readLong(byte[] buf, int offset) {
        return ((buf[offset+7] & 0xFFL)) +
          ((buf[offset+6] & 0xFFL) <<  8) +
          ((buf[offset+5] & 0xFFL) << 16) +
          ((buf[offset+4] & 0xFFL) << 24) +
          ((buf[offset+3] & 0xFFL) << 32) +
          ((buf[offset+2] & 0xFFL) << 40) +
          ((buf[offset+1] & 0xFFL) << 48) +
          (((long) buf[offset])    << 56);
    }

    public static long readLongCompressed(byte[] buf, int offset) {
        byte len=buf[offset++];
        if(len == 0)
            return 0;
        return makeLong(buf, offset, len);
    }

    /**
     * Computes the size of a variable-length encoded long.  Note that this is <em>not</em> currently using
     * variable-length encoding (will be implemented later).
     * @param num the long
     * @return the number of bytes needed to variable-length encode num
     */
    public static int size(long num) {
        return (byte)(num == 0? 1 : bytesRequiredFor(num) +1);
    }





    // ------------------ long seq ---------------------- //

    /**
     * Writes 2 sequence numbers (seqnos) in compressed format to buf.
     * The seqnos are non-negative and hr is guaranteed to be &gt;= hd.
     * <p/>
     * Once variable-length encoding has been implemented, this method will probably get dropped as we can simply
     * write the 2 longs individually.
     * @param hd the highest delivered seqno. Guaranteed to be a positive number
     * @param hr the highest received seqno. Guaranteed to be a positive number. Greater than or equal to hd
     * @param buf the buffer to write to
     */
    public static void writeLongSequence(long hd, long hr, ByteBuffer buf) {
        if(hr < hd)
            throw new IllegalArgumentException("hr (" + hr + ") has to be >= hd (" + hd + ")");

        if(hd == 0 && hr == 0) {
            buf.put((byte)0);
            return;
        }

        long delta=hr - hd;

        // encode highest_delivered followed by delta
        byte bytes_for_hd=bytesRequiredFor(hd), bytes_for_delta=bytesRequiredFor(delta);
        byte bytes_needed=encodeLength(bytes_for_hd, bytes_for_delta);
        buf.put(bytes_needed);

        for(int i=0; i < bytes_for_hd; i++)
            buf.put(getByteAt(hd, i));

        for(int i=0; i < bytes_for_delta; i++)
            buf.put(getByteAt(delta, i));
    }

    /**
     * Writes 2 sequence numbers (seqnos) in compressed format to an output stream.
     * The seqnos are non-negative and hr is guaranteed to be &gt;= hd.
     * <p/>
     * Once variable-length encoding has been implemented, this method will probably get dropped as we can simply
     * write the 2 longs individually.
     * @param hd the highest delivered seqno. Guaranteed to be a positive number
     * @param hr the highest received seqno. Guaranteed to be a positive number. Greater than or equal to hd
     * @param out the output stream to write to
     */
    public static void writeLongSequence(long hd, long hr, DataOutput out) throws IOException {
        if(hr < hd)
            throw new IllegalArgumentException("hr (" + hr + ") has to be >= hd (" + hd + ")");

        if(hd == 0 && hr == 0) {
            out.write(0);
            return;
        }

        long delta=hr - hd;

        // encode highest_delivered followed by delta
        byte bytes_for_hd=bytesRequiredFor(hd), bytes_for_delta=bytesRequiredFor(delta);
        byte bytes_needed=encodeLength(bytes_for_hd, bytes_for_delta);
        out.write(bytes_needed);

        for(int i=0; i < bytes_for_hd; i++)
            out.write(getByteAt(hd, i));

        for(int i=0; i < bytes_for_delta; i++)
            out.write(getByteAt(delta, i));
    }

    /**
     * Reads 2 compressed longs from buf into seqnos
     * <p/>
     * Once variable-length encoding has been implemented, this method will probably get dropped as we can simply
     * read the 2 longs individually.
     * @param buf the buffer to read from
     * @param seqnos the array to read the seqnos into, needs to have a length of 2
     */
    public static void readLongSequence(ByteBuffer buf, long seqnos[]) {
        byte len=buf.get();
        if(len == 0) {
            seqnos[0]=seqnos[1]=0;
            return;
        }
        byte len1=firstNibble(len), len2=secondNibble(len);
        seqnos[0]=makeLong(buf, len1);
        seqnos[1]=makeLong(buf, len2) + seqnos[0];
    }

    /**
     * Reads 2 compressed longs into an array of 2 longs.
     * <p/>
     * Once variable-length encoding has been implemented, this method will probably get dropped as we can simply
     * read the 2 longs individually.
     * @param in the input stream to read from
     * @param seqnos the array to read the seqnos into, needs to have a length of 2
     * @param index the index of the first element to be written; the seqnos are written to seqnos[index] and seqnos[index+1]
     */
    public static void readLongSequence(DataInput in, long[] seqnos, int index) throws IOException {
        byte len=in.readByte();
        if(len == 0) {
            seqnos[index]=seqnos[index+1]=0;
            return;
        }
        byte len1=firstNibble(len), len2=secondNibble(len);
        seqnos[index]=makeLong(in, len1);
        seqnos[index+1]=makeLong(in, len2) + seqnos[index];
    }



    public static byte size(long hd, long hr) {
        if(hd == 0 && hr == 0)
            return 1;

        byte num_bytes_for_hd=bytesRequiredFor(hd), num_bytes_for_delta=bytesRequiredFor(hr - hd);
        return (byte)(num_bytes_for_hd + num_bytes_for_delta + 1);
    }


    public static int makeInt(byte[] buf, int offset, int bytes_to_read) {
        int retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=buf[offset + i];
            retval |= ((int)b & 0xff) << (i * 8);
        }
        return retval;
    }

    public static int makeInt(ByteBuffer buffer, int bytes_to_read) {
        int retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=buffer.get();
            retval |= ((int)b & 0xff) << (i * 8);
        }
        return retval;
    }

    public static int makeInt(DataInput in, int bytes_to_read) throws IOException {
        int retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=in.readByte();
            retval |= ((int)b & 0xff) << (i * 8);
        }
        return retval;
    }

    public static long makeLong(byte[] buf, int offset, int bytes_to_read) {
        long retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=buf[offset + i];
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
    }


    public static long makeLong(ByteBuffer buffer, int bytes_to_read) {
        long retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=buffer.get();
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
    }

    public static long makeLong(DataInput in, int bytes_to_read) throws IOException {
        long retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=in.readByte();
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
    }

    // -------------------- float ----------------------- //

    /**
     * Writes a float to a ByteBuffer
     * @param num the float to be written
     * @param buf the buffer
     */
    public static void writeFloat(float num, ByteBuffer buf) {
        writeInt(Float.floatToIntBits(num), buf);
    }

    /**
     * Writes a float to an output stream
     * @param num the float to be written
     * @param out the output stream
     */
    public static void writeFloat(float num, DataOutput out) throws IOException {
        writeInt(Float.floatToIntBits(num), out);
    }

    public static void writeFloat(float num, byte[] buf, int offset) {
        writeInt(Float.floatToIntBits(num), buf, offset);
    }

    /**
     * Reads a a float from a buffer.
     * @param buf the buffer
     * @return the float read from the buffer
     */
    public static float readFloat(ByteBuffer buf) {
        return Float.intBitsToFloat(readInt(buf));
    }

    /**
     * Reads a a float from an input stream.
     * @param in the input stream
     * @return the float read from the input stream
     */
    public static float readFloat(DataInput in) throws IOException {
        return Float.intBitsToFloat(readInt(in));
    }

    public static float readFloat(byte[] buf, int offset) {
        return Float.intBitsToFloat(readInt(buf, offset));
    }


    /**
     * Computes the size of a variable-length encoded float
     * @param num the float
     * @return the number of bytes needed to variable-length encode num
     */
    public static int size(float num) {
        return size(Float.floatToIntBits(num));
    }



    // -------------------- double ---------------------- //

    /**
     * Writes a double to a ByteBuffer
     * @param num the double to be written
     * @param buf the buffer
     */
    public static void writeDouble(double num, ByteBuffer buf) {
        writeLong(Double.doubleToLongBits(num), buf);
    }

    /**
     * Writes a double to an output stream
     * @param num the double to be written
     * @param out the output stream
     */
    public static void writeDouble(double num, DataOutput out) throws IOException {
        writeLong(Double.doubleToLongBits(num), out);
    }

    public static void writeDouble(double num, byte[] buf, int offset) {
        writeLong(Double.doubleToLongBits(num), buf, offset);
    }


    /**
     * Reads a double from a buffer.
     * @param buf the buffer
     * @return the double read from the buffer
     */
    public static double readDouble(ByteBuffer buf) {
        return Double.longBitsToDouble(readLong(buf));
    }

    /**
     * Reads a double from an input stream
     * @param in the input stream
     * @return the double read from the input stream
     */
    public static double readDouble(DataInput in) throws IOException {
        return Double.longBitsToDouble(readLong(in));
    }

    public static double readDouble(byte[] buf, int offset) {
        return Double.longBitsToDouble(readLong(buf, offset));
    }

    /**
     * Computes the size of a variable-length encoded double
     * @param num the double
     * @return the number of bytes needed to variable-length encode num
     */
    public static int size(double num) {
        return size(Double.doubleToLongBits(num));
    }




    // -------------------- String ---------------------- //

    /**
     * Writes a string to buf. The length of the string is written first, followed by the chars (as single-byte values).
     * Multi-byte values are truncated: only the lower byte of each multi-byte char is written, similar to
     * {@link DataOutput#writeChars(String)}.
     * @param s the string
     * @param buf the buffer
     */
    public static void writeString(String s, ByteBuffer buf) {
        buf.put((byte)(s != null? 1 : 0));
        if(s != null) {
            byte[] bytes=s.getBytes();
            writeInt(bytes.length, buf);
            buf.put(bytes);
        }
    }

    /**
     * Writes a string to buf. The length of the string is written first, followed by the chars (as single-byte values).
     * Multi-byte values are truncated: only the lower byte of each multi-byte char is written, similar to
     * {@link DataOutput#writeChars(String)}.
     * @param s the string
     * @param out the output stream
     */
    public static void writeString(String s, DataOutput out) throws IOException {
        if(s != null) {
            out.write(1);
            out.writeUTF(s);
        }
        else
            out.write(0);
    }

    /**
     * Reads a string from buf. The length is read first, followed by the chars. Each char is a single byte
     * @param buf the buffer
     * @return the string read from buf
     */
    public static String readString(ByteBuffer buf) {
        if(buf.get() == 0)
            return null;
        int len=readInt(buf);
        if(buf.isDirect()) {
            byte[] bytes=new byte[len];
            buf.get(bytes);
            return new String(bytes);
        }
        else {
            byte[] bytes=buf.array();
            return new String(bytes, buf.arrayOffset() + buf.position(), len);
        }
    }

    /**
     * Reads a string from buf. The length is read first, followed by the chars. Each char is a single byte
     * @param in the input stream
     * @return the string read from buf
     */
    public static String readString(DataInput in) throws IOException {
        int b=in.readByte();
        if(b == 1)
            return in.readUTF();
        return null;
    }


    /**
     * Measures the number of bytes required to encode a string, taking multibyte characters into account. Measures
     * strings written by {@link DataOutput#writeUTF(String)}.
     * @param str the string
     * @return the number of bytes required for encoding str
     */
    public static int sizeUTF(String str) {
        int len=str != null? str.length() : 0, utflen=2;
        if(len == 0)
            return utflen;
        for(int i = 0; i < len; i++) {
           int  c=str.charAt(i);
            if((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }

    public static int size(String str) {
        if(str == null)
            return Global.BYTE_SIZE;
        byte[] bytes=str.getBytes();
        return Global.BYTE_SIZE + size(bytes.length) + bytes.length;
    }



    // ------------------ AsciiString ------------------- //
    /**
     * Writes an AsciiString to buf. The length of the string is written first, followed by the chars (as single-byte values).
     * @param s the string
     * @param buf the buffer
     */
    public static void writeAsciiString(AsciiString s, ByteBuffer buf) {
        short length=(short)(s != null? s.length() : -1);
        buf.putShort(length);
        if(s != null)
            buf.put(s.chars());
    }

    /**
     * Writes an AsciiString to buf. The length of the string is written first, followed by the chars (as single-byte values).
     * @param s the string
     * @param out the output stream
     */
    public static void writeAsciiString(AsciiString s, DataOutput out) throws IOException {
        short length=(short)(s != null? s.length() : -1);
        out.writeShort(length);
        if(s != null)
            out.write(s.chars());
    }

    /**
     * Reads an AsciiString from buf. The length is read first, followed by the chars. Each char is a single byte
     * @param buf the buffer
     * @return the string read from buf
     */
    public static AsciiString readAsciiString(ByteBuffer buf) {
        short len=buf.getShort();
        if(len < 0)
            return null;
        AsciiString retval=new AsciiString(len);
        buf.get(retval.chars());
        return retval;
    }

    /**
     * Reads an AsciiString from buf. The length is read first, followed by the chars. Each char is a single byte
     * @param in the input stream
     * @return the string read from buf
     */
    public static AsciiString readAsciiString(DataInput in) throws IOException {
        short len=in.readShort();
        if(len < 0)
            return null;
        AsciiString retval=new AsciiString(len);
        in.readFully(retval.chars());
        return retval;
    }


    /**
     * Measures the number of bytes required to encode an AsciiSring.
     * @param str the string
     * @return the number of bytes required for encoding str
     */
    public static int size(AsciiString str) {
        return str == null? Global.SHORT_SIZE : Global.SHORT_SIZE + str.length();
    }



    /**
     * Encodes the number of bytes needed into a single byte. The first number is encoded in the first nibble (the
     * first 4 bits), the second number in the second nibble
     * @param len1 The number of bytes needed to store a long. Must be between 0 and 8
     * @param len2 The number of bytes needed to store a long. Must be between 0 and 8
     * @return The byte storing the 2 numbers len1 and len2
     */
    protected static byte encodeLength(byte len1, byte len2) {
        byte retval=len2;
        retval |= (len1 << 4);
        return retval;
    }

/*    protected static byte[] decodeLength(byte len) {
        return new byte[]{(byte)((len & 0xff) >> 4),(byte)(len & ~0xf0)}; // 0xf0 is the first nibble set (11110000)
    }*/

    protected static byte firstNibble(byte len) {
        return (byte)((len & 0xff) >> 4); // 0xf0 is the first nibble set (11110000)
    }

    protected static byte secondNibble(byte len) {
        return (byte)(len & ~0xf0); // 0xf0 is the first nibble set (11110000)
    }

    protected static byte bytesRequiredFor(long number) {
        if(number >> 56 != 0) return 8;
        if(number >> 48 != 0) return 7;
        if(number >> 40 != 0) return 6;
        if(number >> 32 != 0) return 5;
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        return 1;
    }

    protected static byte bytesRequiredFor(int number) {
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        return 1;
    }


    static protected byte getByteAt(long num, int index) {
        return (byte)((num >> (index * 8)));
    }


}

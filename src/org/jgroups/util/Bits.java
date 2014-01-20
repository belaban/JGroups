package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Class (similar to java.io.Bits) which encodes numbers to memory (byte array or byte buffer) and reads numbers
 * from memory. Will later be extended to read/write all sorts of objects to/from memory, e.g. booleans, signed and
 * unsigned numbers and strings.<p/>
 * The initial methods were extracted from Util.
 * @author Bela Ban
 * @author Sanne Grinovero
 * @since  3.5
 */
public class Bits {


    public static void writeLong(final long num, final DataOutput out) throws IOException {
        if(num == 0) {
            out.write(0);
            return;
        }
        final byte bytes_needed=bytesRequiredForLong(num);
        out.write(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            out.write(getByteAt(num, i));
    }

    public static long readLong(DataInput in) throws Exception {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        byte[] buf=new byte[len];
        in.readFully(buf, 0, len);
        return makeLong(buf, 0, len);
    }


    public static void writeLongSequence(long highest_delivered, long highest_received, DataOutput out) throws Exception {
        if(highest_received < highest_delivered)
            throw new IllegalArgumentException("highest_received (" + highest_received +
                                                 ") has to be >= highest_delivered (" + highest_delivered + ")");

        if(highest_delivered == 0 && highest_received == 0) {
            out.write(0);
            return;
        }

        long delta=highest_received - highest_delivered;

        // encode highest_delivered followed by delta
        byte bytes_for_hd=bytesRequiredForLong(highest_delivered), bytes_for_delta=bytesRequiredForLong(delta);
        byte bytes_needed=encodeLength(bytes_for_hd, bytes_for_delta);
        out.write(bytes_needed);

        for(int i=0; i < bytes_for_hd; i++)
            out.write(getByteAt(highest_delivered, i));

        for(int i=0; i < bytes_for_delta; i++)
            out.write(getByteAt(delta, i));
    }


    public static long[] readLongSequence(DataInput in) throws Exception {
        byte len=in.readByte();
        if(len == 0)
            return new long[]{0,0};

        byte[] lengths=decodeLength(len);
        long[] seqnos=new long[2];
        byte[] buf=new byte[lengths[0] + lengths[1]];
        in.readFully(buf, 0, buf.length);
        seqnos[0]=makeLong(buf, 0, lengths[0]);
        seqnos[1]=makeLong(buf, lengths[0], lengths[1]) + seqnos[0];
        return seqnos;
    }

    public static byte size(long number) {
        return (byte)(number == 0? 1 : bytesRequiredForLong(number) +1);
    }

    /**
     * Writes 2 longs, where the second long needs to be >= the first (we only write the delta !)
     * @param hd
     * @param hr
     * @return
     */
    public static byte size(long hd, long hr) {
        if(hd == 0 && hr == 0)
            return 1;

        byte num_bytes_for_hd=bytesRequiredForLong(hd), num_bytes_for_delta=bytesRequiredForLong(hr - hd);
        return (byte)(num_bytes_for_hd + num_bytes_for_delta + 1);
    }


    public static short makeShort(byte[] buf, int offset) {
        byte c1=buf[offset], c2=buf[offset+1];
        return  (short)((c1 << 8) + (c2 << 0));
    }

    public static long makeLong(byte[] buf, int offset, int len) {
        long retval=0;
        for(int i=0; i < len; i++) {
            byte b=buf[offset + i];
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
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

    protected static byte[] decodeLength(byte len) {
        return new byte[]{(byte)((len & 0xff) >> 4),(byte)(len & ~0xf0)}; // 0xf0 is the first nibble set (11110000)
    }

    protected static byte bytesRequiredForLong(long number) {
        if(number >> 56 != 0) return 8;
        if(number >> 48 != 0) return 7;
        if(number >> 40 != 0) return 6;
        if(number >> 32 != 0) return 5;
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        if(number != 0) return 1;
        return 1;
    }


    static protected byte getByteAt(long num, int index) {
        return (byte)((num >> (index * 8)));
    }

}

package org.jgroups.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberEncoding {

   public static void writeLong(final long num, final DataOutput out) throws IOException {
      if(num == 0) {
         out.write(0);
         return;
      }
      final byte bytes_needed=numberOfBytesRequiredForLong(num);
      //Current implementation encodes the size as headers
      out.write(bytes_needed);
      for(int i=0; i < bytes_needed; i++)
         out.write(getByteAt(num, i));
   }

   public static long decode(final byte[] buf) {
      if(buf[0] == 0)
          return 0;

      byte length=buf[0];
      return makeLong(buf, 1, length);
  }
   
   public static long readLong(final DataInput in) throws Exception {
      byte len=in.readByte();
      if(len == 0)
          return 0;
      byte[] buf=new byte[len];
      in.readFully(buf, 0, len);
      return makeLong(buf, 0, len);
  }

   public static long makeLong(byte[] buf, int offset, int len) {
      long retval=0;
      for(int i=0; i < len; i++) {
          byte b=buf[offset + i];
          retval |= ((long)b & 0xff) << (i * 8);
      }
      return retval;
  }

   @Deprecated
   public static byte[] encode(final long num) {
      if(num == 0)
          return new byte[]{0};

      byte bytes_needed=numberOfBytesRequiredForLong(num);
      byte[] buf=new byte[bytes_needed + 1];
      buf[0]=bytes_needed;

      int index=1;
      for(int i=0; i < bytes_needed; i++)
          buf[index++]=getByteAt(num, i);
      return buf;
  }

  public static void writeLongSequence(final long highest_delivered, final long highest_received, DataOutput out) throws Exception {
      byte[] buf=encodeLongSequence(highest_delivered, highest_received);
      out.write(buf, 0, buf.length);
  }

  public static long[] readLongSequence(final DataInput in) throws Exception {
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

   /**
    * Encode the highest delivered and received seqnos. The assumption is that the latter is always >= the former, and
    * highest received is not much higher than highest delivered.
    * @param highest_delivered
    * @param highest_received
    * @return
    */
   @Deprecated
   public static byte[] encodeLongSequence(long highest_delivered, long highest_received) {
      if(highest_received < highest_delivered)
         throw new IllegalArgumentException("highest_received (" + highest_received +
                                              ") has to be >= highest_delivered (" + highest_delivered + ")");

      if(highest_delivered == 0 &&highest_received == 0)
         return new byte[]{0};

      long delta=highest_received - highest_delivered;

      // encode highest_delivered followed by delta
      byte num_bytes_for_hd=numberOfBytesRequiredForLong(highest_delivered),
      num_bytes_for_delta=numberOfBytesRequiredForLong(delta);

      byte[] buf=new byte[num_bytes_for_hd + num_bytes_for_delta + 1];

      buf[0]=encodeLength(num_bytes_for_hd, num_bytes_for_delta);

      int index=1;
      for(int i=0; i < num_bytes_for_hd; i++)
         buf[index++]=getByteAt(highest_delivered, i);

      for(int i=0; i < num_bytes_for_delta; i++)
         buf[index++]=getByteAt(delta, i);

      return buf;
   }

   @Deprecated
   public static byte[] decodeLength(final byte len) {
      byte[] retval={(byte)0,(byte)0};
      retval[0]=(byte)((len & 0xff) >> 4);
      retval[1]=(byte)(len & ~0xf0); // 0xff is the first nibble set (11110000)
      // retval[1]=(byte)(len << 4);
      // retval[1]=(byte)((retval[1] & 0xff) >> 4);
      return retval;
   }

   static protected byte getByteAt(final long num, final int index) {
      return (byte)((num >> (index * 8)));
   }

   /**
    * Encodes the number of bytes needed into a single byte. The first number is encoded in the first nibble (the
    * first 4 bits), the second number in the second nibble
    * @param len1 The number of bytes needed to store a long. Must be between 0 and 8
    * @param len2 The number of bytes needed to store a long. Must be between 0 and 8
    * @return The byte storing the 2 numbers len1 and len2
    */
   public static byte encodeLength(final byte len1, final byte len2) {
      byte retval=len2;
      retval |= (len1 << 4);
      return retval;
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

      byte num_bytes_for_hd=numberOfBytesRequiredForLong(hd),
         num_bytes_for_delta=numberOfBytesRequiredForLong(hr - hd);

      return (byte)(num_bytes_for_hd + num_bytes_for_delta + 1);
   }

   public static byte size(long number) {
      return (byte)(number == 0? 1 : numberOfBytesRequiredForLong(number) +1);
   }

   public static long[] decodeLongSequence(final byte[] buf) {
      if(buf[0] == 0)
         return new long[]{0,0};

      byte[] lengths=decodeLength(buf[0]);
      long[] seqnos=new long[2];
      seqnos[0]=makeLong(buf, 1, lengths[0]);
      seqnos[1]=makeLong(buf, 1 + lengths[0], lengths[1]) + seqnos[0];

      return seqnos;
   }

   static byte numberOfBytesRequiredForLong(final long number) {
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

}


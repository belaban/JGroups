package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Constructable;
import org.jgroups.Global;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.function.Supplier;

/**
 * Logical address which is unique over space and time. <br/>
 * Copied from java.util.UUID, but unneeded fields from the latter have been removed. UUIDs needs to
 * have a small memory footprint.
 * 
 * @author Bela Ban
 */
public class UUID implements Address, Constructable<UUID> {
    protected long   mostSigBits;
    protected long   leastSigBits;

    /** The random number generator used by this class to create random based UUIDs */
    protected static volatile SecureRandom numberGenerator=null;

    protected static final int SIZE=Global.LONG_SIZE * 2;



    public UUID() {
    }


    public UUID(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
    }

    /** Private constructor which uses a byte array to construct the new UUID */
    protected UUID(byte[] data) {
        long msb = 0;
        long lsb = 0;
        if(data.length != 16)
            throw new RuntimeException("UUID needs a 16-byte array");
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        this.mostSigBits = msb;
        this.leastSigBits = lsb;
    }

    public Supplier<? extends UUID> create() {
        return UUID::new;
    }


    /**
     * Static factory to retrieve a type 4 (pseudo randomly generated) UUID.
     * The {@code UUID} is generated using a cryptographically strong pseudo random number generator.
     * @return  A randomly generated {@code UUID}
     */
    public static UUID randomUUID() {
        return new UUID(generateRandomBytes(16));
    }


    public long getLeastSignificantBits() {
        return leastSigBits;
    }

    /**
     * Returns the most significant 64 bits of this UUID's 128 bit value.
     * @return  The most significant 64 bits of this UUID's 128 bit value
     */
    public long getMostSignificantBits() {
        return mostSigBits;
    }




    public String toString() {
        String val=NameCache.get(this);
        return val != null? val : toStringLong();
    }

     /**
     * Returns a {@code String} object representing this {@code UUID}.
     *
     * <p> The UUID string representation is as described by this BNF:
     * <blockquote><pre>
     * {@code
     * UUID                   = <time_low> "-" <time_mid> "-"
     *                          <time_high_and_version> "-"
     *                          <variant_and_sequence> "-"
     *                          <node>
     * time_low               = 4*<hexOctet>
     * time_mid               = 2*<hexOctet>
     * time_high_and_version  = 2*<hexOctet>
     * variant_and_sequence   = 2*<hexOctet>
     * node                   = 6*<hexOctet>
     * hexOctet               = <hexDigit><hexDigit>
     * hexDigit               =
     *       "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
     *       | "a" | "b" | "c" | "d" | "e" | "f"
     *       | "A" | "B" | "C" | "D" | "E" | "F"
     * }</pre></blockquote>
     *
     * @return  A string representation of this {@code UUID}
     */
    public String toStringLong() {
        return (digits(mostSigBits >> 32, 8) + "-" +
                digits(mostSigBits >> 16, 4) + "-" +
                digits(mostSigBits, 4) + "-" +
                digits(leastSigBits >> 48, 4) + "-" +
                digits(leastSigBits, 12));
    }


    /**
      * Creates a {@code UUID} from the string standard representation as
      * described in the {@link #toString} method.
      *
      * @param  name
      *         A string that specifies a {@code UUID}
      *
      * @return  A {@code UUID} with the specified value
      *
      * @throws  IllegalArgumentException
      *          If name does not conform to the string representation as
      *          described in {@link #toString}
     *
     */
    public static UUID fromString(String name) {
        String[] components = name.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: "+name);
        for (int i=0; i<5; i++)
            components[i] = "0x"+components[i];

        long mostSigBits =Long.decode(components[0]);
        mostSigBits <<= 16;
        mostSigBits |=Long.decode(components[1]);
        mostSigBits <<= 16;
        mostSigBits |=Long.decode(components[2]);

        long leastSigBits =Long.decode(components[3]);
        leastSigBits <<= 48;
        leastSigBits |=Long.decode(components[4]);

        return new UUID(mostSigBits, leastSigBits);
    }

    /** Returns val represented by the specified number of hex digits. */
    protected static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    /**
     * Returns a hash code for this {@code UUID}.
     * @return  A hash code value for this {@code UUID}
     */
    public int hashCode() {
        return (int)((mostSigBits >> 32) ^
                mostSigBits ^
                (leastSigBits >> 32) ^
                leastSigBits);
    }

    /**
     * Compares this object to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null}, is a {@code UUID}
     * object, has the same variant, and contains the same value, bit for bit,
     * as this {@code UUID}.
     * @param  obj The object to be compared
     * @return  {@code true} if the objects are the same; {@code false} otherwise
     */
    public boolean equals(Object obj) {
        if (!(obj instanceof UUID))
            return false;
        UUID id = (UUID)obj;
        return this == id || (mostSigBits == id.mostSigBits && leastSigBits == id.leastSigBits);
    }


    /**
     * Compares this UUID with the specified UUID.
     * <p> The first of two UUIDs is greater than the second if the most
     * significant field in which the UUIDs differ is greater for the first UUID.
     * @param  other {@code UUID} to which this {@code UUID} is to be compared
     * @return  -1, 0 or 1 as this {@code UUID} is less than, equal to, or greater than {@code val}
     */
    public int compareTo(Address other) {
        UUID val=(UUID)other;
        if(this == val)
            return 0;
        return (this.mostSigBits < val.mostSigBits ? -1 :
                (this.mostSigBits > val.mostSigBits ? 1 :
                        (this.leastSigBits < val.leastSigBits ? -1 :
                                (this.leastSigBits > val.leastSigBits ? 1 :
                                        0))));
    }


    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(leastSigBits);
        out.writeLong(mostSigBits);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        leastSigBits=in.readLong();
        mostSigBits=in.readLong();
    }

    @Override
    public int serializedSize() {
        return SIZE;
    }

    public UUID copy() {
        return new UUID(mostSigBits, leastSigBits);
    }


    public static byte[] generateRandomBytes() {
        return generateRandomBytes(16);
    }
    public static byte[] generateRandomBytes(int size) {
        SecureRandom ng=numberGenerator;
        if(ng == null)
            numberGenerator=ng=new SecureRandom();

        byte[] randomBytes=new byte[size];
        ng.nextBytes(randomBytes);
        return randomBytes;
    }


}

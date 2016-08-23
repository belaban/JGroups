package org.jgroups.util;

import java.io.*;
import java.util.function.Supplier;

/**
 * Subclass of {@link UUID} accommodating additional data such as a flag. There may be many instances
 * in memory, and as they are serialized a lot and sent across the wire, I tried to make this as compact as possible.
 * @author Bela Ban
 * @since  3.5
 */
public class FlagsUUID extends UUID {
    protected int             flags;


    public FlagsUUID() {
        super();
    }

    protected FlagsUUID(byte[] data) {
        super(data);
    }

    public FlagsUUID(long mostSigBits, long leastSigBits) {
        super(mostSigBits,leastSigBits);
    }

    public <T extends FlagsUUID> FlagsUUID(T other) {
        super(other.mostSigBits, other.leastSigBits);
        flags=other.flags;
    }

    public Supplier<? extends UUID> create() {
        return FlagsUUID::new;
    }

    public  static FlagsUUID randomUUID() {return new FlagsUUID(generateRandomBytes());}

    public static FlagsUUID randomUUID(String name) {
        FlagsUUID retval=new FlagsUUID(generateRandomBytes());
        if(name != null)
            UUID.add(retval, name);
        return retval;
    }

    public <T extends FlagsUUID> T setFlag(short flag) {
        flags |= flag; return (T)this;
    }

    public <T extends FlagsUUID> T clearFlag(short flag) {
        flags &= ~flag; return (T)this;
    }

    public boolean isFlagSet(short flag) {
        return (flags & flag) == flag;
    }


    public <T extends FlagsUUID> T addContents(T other) {
        flags|=other.flags;
        return (T)this;
    }


    public void writeTo(DataOutput out) throws Exception {
        super.writeTo(out);
        Bits.writeInt(flags, out);
    }


    public void readFrom(DataInput in) throws Exception {
        super.readFrom(in);
        flags=Bits.readInt(in);
    }

    /** The number of bytes required to serialize this instance */
    public int    size()     {return super.size() + Bits.size(flags);}
    public String toString() {return String.format("%s (flags=%d)", super.toString(), flags);}



}

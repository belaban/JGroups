package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Constructable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * Address with current time as key. Used by testing only!
 * @author Bela Ban
 * @since  5.5.0
 */
public class MillisAddress implements Address, Constructable<MillisAddress> {
    protected long millis;

    public MillisAddress() {
    }

    public MillisAddress(long m) {
        millis=m;
    }

    public MillisAddress(String s) {
        this(Long.parseLong(s));
    }

    public long millis() {return millis;}

    @Override
    public Supplier<? extends MillisAddress> create() {
        return MillisAddress::new;
    }

    public int compareTo(Address other) {
        MillisAddress val=(MillisAddress)other;
        if(this == val)
            return 0;
        return Long.compare(this.millis, val.millis);
    }

    public int compareTo(MillisAddress val) {
        if(this == val)
            return 0;
        return Long.compare(this.millis, val.millis);
    }

    @Override
    public int hashCode() {
        return (int)millis;
    }

    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(!(obj instanceof MillisAddress))
            return false;
        MillisAddress m=(MillisAddress)obj;
        return millis == m.millis;
    }

    @Override
    public int serializedSize() {
        return Long.BYTES;
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(millis);
    }

    @Override
    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        millis=in.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(millis);
    }

}


package org.jgroups.util;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Range implements SizeStreamable, Comparable<Range> {
    public long low=-1;  // first msg to be retransmitted
    public long high=-1; // last msg to be retransmitted



    /** For externalization */
    public Range() {
    }

    public Range(long low, long high) {
        this.low=low; this.high=high;
    }


    public String toString() {
        return "[" + low + " : " + high + ']';
    }


    public int compareTo(Range other) {
        if(low == other.low && high == other.high)
            return 0;
        return low < other.low? -1 : 1;
    }

    public int hashCode() {
        return (int)low;
    }

    public boolean equals(Object obj) {
        Range other=(Range)obj;
        return compareTo(other) == 0;
    }


    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeLongSequence(low, high, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        long[] seqnos={0,0};
        Bits.readLongSequence(in, seqnos, 0);
        low=seqnos[0];
        high=seqnos[1];
    }

    @Override
    public int serializedSize() {
        return Bits.size(low, high);
    }


}

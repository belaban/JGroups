
package org.jgroups.util;


import java.io.DataInput;
import java.io.DataOutput;


public class Range implements Streamable, Comparable<Range> {
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
        if (obj == null) {
            return false;
        }

        if (this.getClass() != obj.getClass()) {
            return false;
        }

        Range other=(Range)obj;
        return compareTo(other) == 0;
    }



    public void writeTo(DataOutput out) throws Exception {
        Bits.writeLongSequence(low, high, out);
    }

    public void readFrom(DataInput in) throws Exception {
        long[] seqnos=Bits.readLongSequence(in);
        low=seqnos[0];
        high=seqnos[1];
    }

    public int serializedSize() {
        return Bits.size(low, high);
    }


}

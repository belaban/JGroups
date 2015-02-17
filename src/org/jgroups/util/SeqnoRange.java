package org.jgroups.util;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Keeps track of a range of messages to be retransmitted. A bit set is used to represent missing messages.
 * Every non-received message has a corresponding bit set to 0, every received message is 1.
 * @author Bela Ban
 */
public class SeqnoRange extends Seqno {
    final long high;
    final FixedSizeBitSet bits;

    public SeqnoRange(long low, long high) {
        super(low);
        this.high=high;
        if(low > high)
            throw new IllegalArgumentException("low (" + low + ") must be <= high (" + high + ")");
        if((high - low) >= Integer.MAX_VALUE)
            throw new IllegalArgumentException("range (" + low + "-" + high + ") size is too big ");
        int size=(int)((high - low) + 1);
        bits=new FixedSizeBitSet(size);  // starts out with all bits set to 0 (false)
    }


    public long getHigh() {
        return high;
    }

    public boolean contains(long num) {
        return num >= low && num <= high;
    }

    public boolean get(long num) {
        return bits.get(getIndex(num));
    }

    public void set(long num) {
        bits.set(getIndex(num));
    }

    public void set(long ... nums) {
        if(nums != null)
            for(long num: nums)
                set(num);
    }

    public void clear(long num) {
        bits.clear(getIndex((int)num));
    }

    public void clear(long ... nums) {
        if(nums != null)
            for(long num: nums)
                clear(num);
    }

    public int getNumberOfReceivedMessages() {
        return bits.cardinality();
    }

    public int getNumberOfMissingMessages() {
        return size() - getNumberOfReceivedMessages();
    }

    public int size() {
        return (int)((high - low) + 1);
    }

    public Collection<Range> getMessagesToRetransmit() {
        return getBits(false);
    }


    public String toString() {
        return low + "-" + high;
    }

    public String print() {
        return low + "-" + high + ", set=" + printBits(true) + ", cleared=" + printBits(false);
    }

    protected int getIndex(long num) {
        if(num < low || num > high)
            throw new IllegalArgumentException(num + " is outside the range " + toString());
        return (int)(num - low);
    }

    public String printBits(boolean value) {
        Collection<Range> ranges=getBits(value);
        StringBuilder sb=new StringBuilder();
        if(ranges != null && !ranges.isEmpty()) {
            boolean first=true;
            for(Range range: ranges) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                if(range.low == range.high)
                    sb.append(range.low);
                else
                    sb.append(range.low).append("-").append(range.high);
            }
        }
        return sb.toString();
    }

    /**
     * Returns ranges of all bit set to value
     * @param value If true, returns all bits set to 1, else 0
     * @return
     */
    public Collection<Range> getBits(boolean value) {
        int index=0;
        int start_range=0, end_range=0;
        int size=(int)((high - low) + 1);
        final Collection<Range> retval=new ArrayList<>(size);

        while(index < size) {
            start_range=value? bits.nextSetBit(index) : bits.nextClearBit(index);
            if(start_range < 0 || start_range >= size)
                break;
            end_range=value? bits.nextClearBit(start_range) : bits.nextSetBit(start_range);
            if(end_range < 0 || end_range >= size) {
                retval.add(new Range(start_range + low, size-1+low));
                break;
            }
            retval.add(new Range(start_range + low, end_range-1+low));
            index=end_range;
        }

        return retval;
    }
    
  
}

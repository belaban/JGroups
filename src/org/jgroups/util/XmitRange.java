package org.jgroups.util;

import org.jgroups.ChannelException;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;


/**
 * Keeps track of a range of messages to be retransmitted. A bit set is used to represent missing messages.
 * Every non-received message has a corresponding bit set to 0, every received message is 1.
 * @author Bela Ban
 * @version $Id: XmitRange.java,v 1.5 2009/11/25 08:51:54 belaban Exp $
 */
public class XmitRange implements Comparable<XmitRange> {
    final long low;
    final long high;
    final boolean dummy;
    final FixedSizeBitSet bits;

    public XmitRange(long low, long high) {
        this.low=low;
        this.high=high;
        this.dummy=false;
        if(low > high)
            throw new IllegalArgumentException("low (" + low + ") must be <= high (" + high + ")");
        int size=(int)((high - low) + 1);
        bits=new FixedSizeBitSet(size);  // starts out with all bits set to 0 (false)
    }

    /**
     * Only used to compare a long against a range in a TreeSet / TreeMap. Used to find a range given a seqno
     * @param num
     * @param dummy
     */
    public XmitRange(long num, boolean dummy) {
        this.dummy=dummy;
        low=high=num;
        bits=null;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    public boolean contains(long num) {
        return num >= low && num <= high;
    }

    public boolean get(long num) {
        int index=getIndex((int)num);
        synchronized(this) {
            return bits.get(index);
        }
    }

    public void set(long num) {
        int index=getIndex((int)num);
        synchronized(this) {
            bits.set(index);
        }
    }

    public void set(long ... nums) {
        if(nums == null)
            return;
        synchronized(this) {
            for(long num: nums)
                set(num);
        }
    }

    public void clear(long num) {
        int index=getIndex((int)num);
        synchronized(this) {
            bits.clear(index);
        }
    }

    public void clear(long ... nums) {
        if(nums == null)
            return;
        synchronized(this) {
            for(long num: nums)
                clear(num);
        }
    }

    public synchronized int getNumberOfReceivedMessages() {
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

    /**
     * This method is key to add a range to a sorted set (or map). The key to compare on is 'low'. When we want to
     * lookup a range given a seqno, we'll create a dummy Range and then the comparison checks whether the seqno
     * is in the range
     * @param range
     * @return
     */
    public int compareTo(XmitRange range) {
        if(range == null)
            throw new NullPointerException();
        if(!dummy)
            return low > range.low ? 1 : low < range.low? -1 : 0;

        if(low >= range.low && high <= range.high)
            return 0;
        if(low < range.low)
            return -1;
        else
            return 1;
    }


    public boolean equals(Object obj) {
        return obj != null && compareTo((XmitRange)obj) == 0;
    }

    public int hashCode() {
        return (int)low;
    }

    public String toString() {
        if(dummy)
            return Long.toString(low);
        return low + " - " + high;
    }

    public String print() {
        if(dummy)
            return Long.toString(low);
        return low + " - " + high + ", set=" + printBits(true) + ", cleared=" + printBits(false);
    }

    protected int getIndex(int num) {
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
    public synchronized Collection<Range> getBits(boolean value) {
        int index=0;
        int start_range=0, end_range=0;
        int size=(int)((high - low) + 1);
        final Collection<Range> retval=new LinkedList<Range>();

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
    

    public static void main(String[] args) throws IOException, ChannelException {
        XmitRange range=new XmitRange(10,20);
        System.out.println("range = " + range.print());

        range.set(12);
        range.set(17);
        range.set(10);
        range.set(11);
        System.out.println("range = " + range.print());

        boolean set=range.get(12);
        System.out.println("set = " + set);

        set=range.get(17);
        System.out.println("set = " + set);

        System.out.println("msgs to retransmit: " + range.printBits(false));



       /* TreeMap<MyRange,MyRange> map=new TreeMap<MyRange,MyRange>();

        MyRange[] ranges=new MyRange[]{new MyRange(23,200), new MyRange(222,222), new MyRange(700,800), new MyRange(900,905)};

        for(MyRange range: ranges)
            map.put(range, range);


        System.out.println("map = " + map.keySet());


        for(long num: new long[]{0, 1, 23, 100, 200, 201, 202, 222, 223, 750, 899, 905, 1000}) {
            MyRange range=get(num, map);
            if(range != null && range.contains(num))
                System.out.println("range for " + num + ": " + range);
            else
                System.out.println("range for " + num + ": " + range);
        }*/
    }


    public static XmitRange get(long num, Map<XmitRange, XmitRange> map) {
        XmitRange range=map.get(new XmitRange(num, true));
        if(range != null && range.contains(num))
            return range;
        return null;
    }
}

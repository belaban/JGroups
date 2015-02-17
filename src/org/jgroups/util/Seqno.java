package org.jgroups.util;

import java.util.ArrayList;
import java.util.Collection;


/**
 * Keeps track of a single message to retransmit
 * @author Bela Ban
 */
public class Seqno {
    final long low;
    byte flags=0;

    public static final byte DUMMY    = 1 << 0;
    public static final byte RECEIVED = 1 << 1;
    

    public Seqno(long low) {
        this.low=low;
    }

    /**
     * Only used to compare a long against a range in a TreeSet / TreeMap. Used to find a range given a seqno
     * @param num
     * @param dummy
     */
    public Seqno(long num, boolean dummy) {
        low=num;
        if(dummy)
            flags=Util.setFlag(flags, DUMMY);
    }

    public boolean isDummy() {
        return Util.isFlagSet(flags, DUMMY);
    }

    public long getLow() {
        return low;
    }

    public boolean contains(long num) {
        return low == num;
    }

    public boolean get(long num) {
        return low == num && received();
    }

    public void set(long num) {
        if(low == num)
            flags=Util.setFlag(flags, RECEIVED);
    }

    public void clear(long num) {
        if(low == num)
            flags=Util.clearFlags(flags, RECEIVED);
    }

    public int getNumberOfReceivedMessages() {
        return received()? 1 : 0;
    }

    public int getNumberOfMissingMessages() {
        return received()? 0 : 1;
    }

    public int size() {
        return 1;
    }

    public Collection<Range> getMessagesToRetransmit() {
        final Collection<Range> retval=new ArrayList<>(1);
        if(!received())
            retval.add(new Range(low, low));
        return retval;
    }

    public int hashCode() {
        return (int)low;
    }

    public boolean equals(Object obj) {
        return obj instanceof Seqno && low == ((Seqno)obj).low;
    }

    public String toString() {
        if(isDummy())
            return low + " (dummy)";
        return Long.toString(low);
    }

    public String print() {
        if(isDummy())
            return Long.toString(low);
        return Long.toString(low);
    }



    protected boolean received() {
        return Util.isFlagSet(flags, RECEIVED);
    }
    
  
}

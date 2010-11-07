
package org.jgroups.stack;

import org.jgroups.annotations.GuardedBy;


/**
 * Manages retransmission timeouts. Always returns the next timeout, until the last timeout in the
 * array is reached. Returns the last timeout from then on. Note that this class is <em?not</em> immutable,
 * so it shouldn't be shared between instances, as {@link #next()} will modify the state.
 * @author John Giorgiadis
 * @author Bela Ban
 * @version $Id: StaticInterval.java,v 1.3 2007/08/10 12:47:38 belaban Exp $
 */
public class StaticInterval implements Interval {
    private int          next=0;
    private final long[] values;

    public StaticInterval(long ... vals) {
        if (vals.length == 0)
            throw new IllegalArgumentException("zero length array passed as argument");
        this.values=vals;
    }

    public Interval copy() {
        return new StaticInterval(values);
    }

    /** @return the next interval */
    @GuardedBy("interval")
    public long next() {
        // we don't need to synchronize because this method won't be called concurrently; each entry has its own copy
        // of StaticInterval
        // synchronized(values) {
            if (next >= values.length)
                return(values[values.length-1]);
            else
                return(values[next++]);
        // }
    }
    

}




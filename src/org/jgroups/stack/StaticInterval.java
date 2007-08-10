
package org.jgroups.stack;

import org.jgroups.annotations.GuardedBy;


/**
 * Manages retransmission timeouts. Always returns the next timeout, until the last timeout in the
 * array is reached. Returns the last timeout from then on. Note that this class is <em?not</em> immutable,
 * so it shouldn't be shared between instances, as {@link #next()} will modify the state.
 * @author John Giorgiadis
 * @author Bela Ban
 * @version $Id: StaticInterval.java,v 1.1 2007/08/10 11:58:37 belaban Exp $
 */
public class StaticInterval implements Interval {
    private int          next=0;
    private final long[] interval;

    public StaticInterval(long ... vals) {
        if (vals.length == 0)
            throw new IllegalArgumentException("zero length array passed as argument");
        this.interval=vals;
    }


    /** @return the next interval */
    @GuardedBy("interval")
    public long next() {
        synchronized(interval) {
            if (next >= interval.length)
                return(interval[interval.length-1]);
            else
                return(interval[next++]);
        }
    }
    

}


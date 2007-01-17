// $Id: Interval.java,v 1.2 2007/01/17 14:50:59 belaban Exp $

package org.jgroups.stack;

import org.jgroups.annotations.Immutable;
import org.jgroups.annotations.GuardedBy;


/**
 * Manages retransmission timeouts. Always returns the next timeout, until the last timeout in the
 * array is reached. Returns the last timeout from then on.
 * @author John Giorgiadis
 * @author Bela Ban
 */
@Immutable
public class Interval {
    private int          next=0;
    private final long[] interval;

    public Interval(long[] interval) {
        if (interval.length == 0)
            throw new IllegalArgumentException("Interval()");
        this.interval=interval;
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


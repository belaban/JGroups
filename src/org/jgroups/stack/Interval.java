
package org.jgroups.stack;

import org.jgroups.annotations.Immutable;
import org.jgroups.annotations.GuardedBy;


/**
 * Interface which returns a time series, one value at a time calling next()
 * @author Bela Ban
 */
public interface Interval {
    /** @return the next interval */
    public long next() ;

    /** Returns a copy of the state. If there is no state, this method may return a ref to itself */
    Interval copy();
}


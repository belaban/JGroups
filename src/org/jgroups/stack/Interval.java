
package org.jgroups.stack;

import org.jgroups.annotations.Immutable;
import org.jgroups.annotations.GuardedBy;


/**
 * Interface which returns a time series, one value at a time calling next()
 * @author Bela Ban
 * @version $Id: Interval.java,v 1.3 2007/08/10 12:32:17 belaban Exp $
 */
public interface Interval {
    /** @return the next interval */
    public long next() ;

    /** Returns a copy of the state. If there is no state, this method may return a ref to itself */
    Interval copy();
}


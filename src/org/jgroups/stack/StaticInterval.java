
package org.jgroups.stack;

/**
 * Manages retransmission timeouts. Always returns the next timeout, until the last timeout in the
 * array is reached. Returns the last timeout from then on. Note that this class is <em>not</em> immutable,
 * so it shouldn't be shared between instances, as {@link #next()} will modify the state.
 * @author John Giorgiadis
 * @author Bela Ban
 */
public class StaticInterval implements Interval {
    private int          next=0;
    private final int [] values;

    public StaticInterval(int ... vals) {
        if(vals.length == 0)
            throw new IllegalArgumentException("zero length array passed as argument");
        values=vals;
    }

    public Interval copy() {
        return new StaticInterval(values);
    }

    /** @return the next interval */
    public long next() {
        if (next >= values.length)
            return(values[values.length-1]);
        else
            return(values[next++]);
    }
    

}




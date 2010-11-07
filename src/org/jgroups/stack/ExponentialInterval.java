package org.jgroups.stack;

/**
 * @author Bela Ban
 * @version $Id: ExponentialInterval.java,v 1.1 2007/08/14 07:51:12 belaban Exp $
 */
public class ExponentialInterval implements Interval {
    private long value=30;
    private static final long MAX=15000;

    public ExponentialInterval() {
        
    }

    public ExponentialInterval(long value) {
        this.value=value;
    }

    public long next() {
        long retval=value;
        value=Math.min(MAX, value * 2);
        return retval;
    }

    /** We don't need to copy as we don't have any state */
    public final Interval copy() {
        return new ExponentialInterval(value);
    }

    public String toString() {
        return String.valueOf(value);
    }
}

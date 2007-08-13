package org.jgroups.stack;

/**
 * @author Bela Ban
 * @version $Id: DynamicInterval.java,v 1.2 2007/08/13 12:29:45 belaban Exp $
 */
public class DynamicInterval implements Interval {
    private long value=30;
    private static final long MAX=5000;

    public DynamicInterval() {
        
    }

    public DynamicInterval(long value) {
        this.value=value;
    }

    public long next() {
        long retval=value;
        value=Math.min(MAX, value * 2);
        return retval;
    }

    /** We don't need to copy as we don't have any state */
    public final Interval copy() {
        return new DynamicInterval(value);
    }

    public String toString() {
        return String.valueOf(value);
    }
}

package org.jgroups.stack;

/**
 * @author Bela Ban
 * @version $Id: DynamicInterval.java,v 1.1 2007/08/10 13:01:37 belaban Exp $
 */
public class DynamicInterval implements Interval {

    public long next() {
        return 30;
    }

    /** We don't need to copy as we don't have any state */
    public final Interval copy() {
        return this;
    }
}

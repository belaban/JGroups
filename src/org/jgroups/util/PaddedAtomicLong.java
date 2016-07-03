package org.jgroups.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Copied from http://mechanical-sympathy.blogspot.ch/2011/08/false-sharing-java-7.html. Switch to @Contended once
 * it is available.
 * @author Bela Ban
 * @since  4.0
 */
@SuppressWarnings("serial")
public class PaddedAtomicLong extends AtomicLong {
    public volatile long p1=1,p2=2,p3=3,p4=4,p5=5,p6=6,p7=7;

    public long sum() { // prevents optimizing away the fields above
        return p1+p2+p3+p4+p5+p6+p7;
    }

    public PaddedAtomicLong(long initialValue) {
        super(initialValue);
    }
}

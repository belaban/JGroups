package org.jgroups.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 * @since  4.0
 */
@SuppressWarnings("serial")
public class PaddedAtomicInteger extends AtomicInteger {
    protected volatile int i1=1,i2=2,i3=3,i4=4,i5=5,i6=6;
    protected volatile int i7=7,i8=8,i9=9,i10=10,i11=11,i12=12,i13=13,i14=14;

    public PaddedAtomicInteger(int initialValue) {
        super(initialValue);
    }

    public int sum() {
        return i1+i2+i3+i4+i5+i6+i7+i8+i9+i10+i11+i12+i13+i14;
    }
}

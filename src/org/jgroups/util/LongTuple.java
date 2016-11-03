package org.jgroups.util;

/**
 * A tuple with a long (primitive) first value
 * @author Bela Ban
 */
public class LongTuple<V> {
    private final long val1;
    private final V    val2;

    public LongTuple(long val1, V val2) {
        this.val1=val1;
        this.val2=val2;
    }

    public long getVal1() {return val1;}
    public V    getVal2() {return val2;}

    public String toString() {
        return val1 + " : " + val2;
    }
}

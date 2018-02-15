package org.jgroups.util;

/**
 * Holds 2 values, useful when we have a map with a key, but more than 1 value and we don't want to create a separate
 * holder object for the values, and don't want to pass the values as a list or array.
 * @author Bela Ban
 */
public class Tuple<V1,V2> {
    private final V1 val1;
    private final V2 val2;

    public Tuple(V1 val1, V2 val2) {
        this.val1=val1;
        this.val2=val2;
    }

    public V1 getVal1() {return val1;}
    public V2 getVal2() {return val2;}

    public String toString() {
        return String.format("%s: %s", val1, val2);
    }
}

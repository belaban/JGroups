package org.jgroups.util;

/**
 * Holds 3 values, useful when we have a map with a key, but more than 1 value and we don't want to create a separate
 * holder object for the values, and don't want to pass the values as a list or array.
 * @author Bela Ban
 */
public class Triple<V1,V2,V3> {
    private V1 val1;
    private V2 val2;
    private V3 val3;

    public Triple(V1 val1, V2 val2, V3 val3) {
        this.val1=val1;
        this.val2=val2;
        this.val3=val3;
    }

    public V1 getVal1() {
        return val1;
    }

    public void setVal1(V1 val1) {
        this.val1=val1;
    }

    public V2 getVal2() {
        return val2;
    }

    public void setVal2(V2 val2) {
        this.val2=val2;
    }

    public V3 getVal3() {
        return val3;
    }

    public void setVal3(V3 val3) {
        this.val3=val3;
    }

    public String toString() {
        return val1 + " : " + val2 + " : " + val3;
    }
}

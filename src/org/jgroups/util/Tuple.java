package org.jgroups.util;

/**
 * Holds 2 values, useful when we have a map with a key, but more than 1 value and we don't want to create a separate
 * holder object for the values, and don't want to pass the values as a list or array.
 * @author Bela Ban
 * @version $Id: Tuple.java,v 1.1 2007/11/23 12:50:48 belaban Exp $
 */
public class Tuple<V1,V2> {
    private V1 val1;
    private V2 val2;

    public Tuple(V1 val1, V2 val2) {
        this.val1=val1;
        this.val2=val2;
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

    public String toString() {
        return val1 + " : " + val2;
    }
}

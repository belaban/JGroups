package org.jgroups.util;

/**
 * Holds 2 values, useful when we have a map with a key, but more than 1 value and we don't want to create a separate
 * holder object for the values, and don't want to pass the values as a list or array.
 * @param <V1> V1
 * @param <V2> V2
 * @param val1 val1
 * @param val2 val2
 * @author Bela Ban
 */
public record Tuple<V1, V2>(V1 val1, V2 val2) {
    public String toString() {
        return String.format("%s: %s", val1, val2);
    }
}

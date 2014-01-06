package org.jgroups.util;

/**
 * @author Bela Ban
 * @since  3.5
 */
public interface Filter<T> {
    boolean accept(T element);
}

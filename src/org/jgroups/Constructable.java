package org.jgroups;

import java.util.function.Supplier;

/**
 * Interface returning a supplier which can be called to create an instance
 * @param <T> The type parameter
 * @author Bela Ban
 * @since  4.0
 */
public interface Constructable<T> {
    /** Creates an instance of the class implementing this interface
     * @return Supplier
     */
    Supplier<? extends T> create();
}

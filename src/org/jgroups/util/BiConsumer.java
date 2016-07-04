package org.jgroups.util;

/**
 * Required by RingBuffer.
 * @author Bela Ban
 * @since 3.6
 */
public interface BiConsumer<T, U> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     */
    void accept(T t, U u);
}

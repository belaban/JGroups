package org.jgroups.blocks.atomic;

import org.jgroups.annotations.Experimental;
import org.jgroups.util.LongSizeStreamable;
import org.jgroups.util.Streamable;

/**
 * A synchronous counter interface
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface SyncCounter {

    String getName();

    /**
     * Gets the current value of the counter
     *
     * @return The current value
     */
    long get();

    /**
     * Sets the counter to a new value
     *
     * @param new_value The new value
     */
    void set(long new_value);


    /**
     * Atomically updates the counter using a CAS operation
     *
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return True if the counter could be updated, false otherwise
     */
    default boolean compareAndSet(long expect, long update) {
        return compareAndSwap(expect, update) == expect;
    }

    /**
     * Atomically updates the counter using a compare-and-swap operation.
     *
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return The previous value of the counter
     */
    long compareAndSwap(long expect, long update);

    /**
     * Atomically increments the counter and returns the new value
     *
     * @return The new value
     */
    default long incrementAndGet() {
        return addAndGet(1L);
    }

    /**
     * Atomically decrements the counter and returns the new value
     *
     * @return The new value
     */
    default long decrementAndGet() {
        return addAndGet(-1L);
    }


    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    long addAndGet(long delta);

    /**
     * Atomically updates the counter's value.
     * <p>
     * Both {@link CounterFunction} and return value must implement {@link Streamable} to be sent over the network. The function should not block thread since it can cause deadlocks neither invoke any operation over the {@link SyncCounter}.
     * <p>
     * The {@link CounterView} is a copy of the counter's value and the last {@link CounterView#set(long)} will be applied to the counter.
     *
     * @param updateFunction The update {@link CounterFunction}.
     * @param <T>            The return value type.
     * @return The function's return value.
     * @see CounterFunction
     * @see CounterView
     * @see LongSizeStreamable
     */
    @Experimental
    default <T extends Streamable> T update(CounterFunction<T> updateFunction) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return an asynchronous wrapper around this instance.
     */
    AsyncCounter async();

}

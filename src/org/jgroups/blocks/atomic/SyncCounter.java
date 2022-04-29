package org.jgroups.blocks.atomic;

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
     * @return an asynchronous wrapper around this instance.
     */
    AsyncCounter async();

}

package org.jgroups.blocks.atomic;

/**
 * @author Bela Ban
 * @since 3.0.0
 */
public interface Counter {

    String getName();

    /**
     * Gets the current value of the counter
     * @return The current value
     */
    long get();

    /**
     * Sets the counter to a new value
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
    boolean compareAndSet(long expect, long update);

    /**
     * Atomically increments the counter and returns the new value
     * @return The new value
     */
    long incrementAndGet();

    /**
     * Atomically decrements the counter and returns the new value
     * @return The new value
     */
    long decrementAndGet();


    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    long addAndGet(long delta);
}


package org.jgroups.blocks.atomic;

/**
 * @author Bela Ban
 * @since 3.0.0
 */
public interface Counter {
    /**
     * Get the current value of the counter
     * @return The current value
     */
    public int get();

    /**
     * Sets the counter to a new value
     * @param new_value The new value
     */
    public void set(int new_value);

    /**
     * Atomically updates the counter using a CAS operation
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return True if the counter could be updated, false otherwise
     */
    public boolean compareAndSet(int expect, int update);

    /**
     * Atomically increments the counter and returns the new value
     * @return The new value
     */
    public int incrementAndGet();

    /**
     * Atomically decrements the counter and returns the new value
     * @return The new value
     */
    public int decrementAndGet();
}

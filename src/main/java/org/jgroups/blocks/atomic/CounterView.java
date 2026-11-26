package org.jgroups.blocks.atomic;

/**
 * A view representing the counter's state.
 *
 * @author Pedro Ruivo
 * @see CounterFunction
 * @since 5.2
 */
public interface CounterView {

    /**
     * @return The counter's value.
     */
    long get();

    /**
     * Sets the counter's value.
     *
     * @param value The new value.
     */
    void set(long value);

}

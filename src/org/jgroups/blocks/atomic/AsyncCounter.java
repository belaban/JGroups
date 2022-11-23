package org.jgroups.blocks.atomic;

import org.jgroups.annotations.Experimental;
import org.jgroups.util.LongSizeStreamable;
import org.jgroups.util.Streamable;

import java.util.concurrent.CompletionStage;

/**
 * An asynchronous counter interface.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface AsyncCounter extends BaseCounter {

    /**
     * Gets the current value of the counter.
     *
     * @return A {@link CompletionStage} that is completed with the counter's value.
     */
    default CompletionStage<Long> get() {
        return addAndGet(0);
    }

    /**
     * Sets the counter to a new value.
     *
     * @return A {@link CompletionStage} that is completed with the counter's value is updated.
     */
    CompletionStage<Void> set(long new_value);

    /**
     * Atomically updates the counter using a compare-and-set operation.
     *
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return A {@link CompletionStage} that is completed with {@code true} if the counter is updated and {@link false} otherwise.
     */
    default CompletionStage<Boolean> compareAndSet(long expect, long update) {
        return compareAndSwap(expect, update).thenApply(value -> value == expect);
    }

    /**
     * Atomically updates the counter using a compare-and-swap operation.
     *
     * @param expect The expected value of the counter
     * @param update The new value of the counter
     * @return A {@link CompletionStage} that is completed with the current counter's value.
     */
    CompletionStage<Long> compareAndSwap(long expect, long update);

    /**
     * Atomically increments the counter and returns the new value
     *
     * @return A {@link CompletionStage} that is completed with the new counter's value.
     */
    default CompletionStage<Long> incrementAndGet() {
        return addAndGet(1);
    }

    /**
     * Atomically decrements the counter and returns the new value
     *
     * @return A {@link CompletionStage} that is completed with the new counter's value.
     */
    default CompletionStage<Long> decrementAndGet() {
        return addAndGet(-1);
    }


    /**
     * Atomically adds the given value to the current value.
     *
     * @param delta the value to add
     * @return A {@link CompletionStage} that is completed with the updated counter's value.
     */
    CompletionStage<Long> addAndGet(long delta);

    /**
     * Atomically updates the counter's value.
     * <p>
     * Both {@link CounterFunction} and return value must implement {@link Streamable} to be sent over the network.
     * The function should not block thread since it can cause deadlocks neither invoke any operation over the {@link AsyncCounter}.
     * <p>
     * The {@link CounterView} is a copy of the counter's value and the last {@link CounterView#set(long)} will be applied to the counter.
     *
     * @param updateFunction The update {@link CounterFunction}.
     * @param <T>            The return value type.
     * @return The {@link CompletionStage} which will be completed with the {@link CounterFunction} return value.
     * @see CounterFunction
     * @see CounterView
     * @see LongSizeStreamable
     */
    @Experimental
    default <T extends Streamable> CompletionStage<T> update(CounterFunction<T> updateFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    default AsyncCounter async() {
        return this;
    }
}

package org.jgroups.blocks.atomic;

import org.jgroups.util.Streamable;

import java.util.function.Function;

/**
 * A function to update {@link AsyncCounter} or {@link SyncCounter}.
 * <p>
 * The {@link CounterView} contains a view of the counter's value where this function is able to modify.
 * @param <T> T
 * @author Pedro Ruivo
 * @since 5.2
 * @see CounterView
 * @see Streamable
 */
public interface CounterFunction<T extends Streamable> extends Function<CounterView, T>, Streamable {
}

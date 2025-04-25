package org.jgroups.blocks.atomic;

/**
 * @author Bela Ban
 * @since 3.0.0
 * @deprecated Since 5.2 and to be removed in a future version. Use {@link SyncCounter} instead.
 */
@Deprecated
public interface Counter extends SyncCounter {

    @Override
    default long compareAndSwap(long expect, long update) throws Exception {
        // throw exception by default to keep backwards compatibility
        throw new UnsupportedOperationException();
    }

    @Override
    default AsyncCounter async() {
        // throw exception by default to keep backwards compatibility
        throw new UnsupportedOperationException();
    }
}


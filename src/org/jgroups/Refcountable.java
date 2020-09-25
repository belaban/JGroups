package org.jgroups;

/**
 * Provides ref-counting for messages (https://issues.redhat.com/browse/JGRP-2417). The idea is that the payload of the
 * message is a shared resource (e.g. memory from a pool), accessed by different threads. Each thread increments the
 * refcount. When a thread is done, it decrements the refcount, and when the count is 0, the shared resource can be
 * freed (ie. returned to the pool).
 * @author Bela Ban
 * @since  5.1.0
 */
public interface Refcountable<T> {
    /**
     * Increments the refcount
     * @return T the type (typically a message)
     */
    T incr();

    /**
     * Decrements the refcount. This should not become negative. When the refount is 0 for the first time,
     * the shared resource can be returned (in order to be reused).
     * @return T the type (typically a message)
     */
    T decr();

    /**
     * Callback. Called when the refcount becomes 0 for the first time. This can be used for example to return the
     * payload of the message to a memory pool
     */
    // default void release() {}
}

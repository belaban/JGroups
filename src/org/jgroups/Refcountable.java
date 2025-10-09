package org.jgroups;

/**
 * Provides ref-counting for messages (https://issues.redhat.com/browse/JGRP-2942). The idea is that the payload of the
 * message is a shared resource (e.g. memory from a pool), accessed by different threads. Each thread increments the
 * refcount. When a thread is done, it decrements the refcount, and when the count is 0, the shared resource can be
 * released (ie. returned to the pool).
 * @author Bela Ban
 * @since  5.5.1
 */
public interface Refcountable<T> {
    /**
     * Increments the refcount
     * @return The refcount after incrementing it
     */
    default int incr() {return 0;}

    /**
     * Decrements the refcount. This should not become negative. When refcount goes from 1 to 0, the resource can be released.
     * @return The refcount after decrementing it
     */
    default int decr() {return 0;}

    /** @return The current refcount */
    default int refCount() {return 0;}
}

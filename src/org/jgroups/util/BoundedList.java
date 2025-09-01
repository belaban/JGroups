package org.jgroups.util;

/**
 * A bounded list where the oldest elements are removed once the capacity is reached. In most scenarios, this class is
 * used for maintaining a history, e.g. of digests or views, so perf is not important.
 * @param <T> T
 * @author Bela Ban Nov 20, 2003
 */
public class BoundedList<T> extends ConcurrentBlockingRingBuffer<T> {

    public BoundedList(int size) {
        super(size, false, false);
    }

    /**
     * Adds an element at the tail. Removes an object from the head if capacity is exceeded
     * @param obj The object to be added
     */
    public boolean add(T obj) {
        if(obj == null) return false;
        while(size() >= capacity && size() > 0) {
            poll();
        }
        return super.offer(obj);
    }

    public boolean addIfAbsent(T obj) {
        return obj != null && !contains(obj) && add(obj);
    }

    public T removeFromHead() {
        return poll();
    }


}

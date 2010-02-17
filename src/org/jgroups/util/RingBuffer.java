package org.jgroups.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.7 2010/02/17 11:05:57 belaban Exp $
 */
public class RingBuffer<T> {
    private final AtomicReference<T>[]  queue;
    private final int                   capacity;
    private final AtomicLong            next_to_add=new AtomicLong(0);
    private final AtomicLong            next_to_remove=new AtomicLong(0);
    private final AtomicInteger         size=new AtomicInteger(0);



    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        queue=new AtomicReference[capacity];
        this.capacity=capacity;
        for(int i=0; i < capacity; i++)
            queue[i]=new AtomicReference<T>();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("next_to_add=" + next_to_add + ", next_to_remove=" + next_to_remove +
                ", size=" + size);
            }
        });
    }

    /**
     * Adds an elements into the buffer. Blocks if full
     * @param el
     */
    public void add(T el) {
        if(el == null)
            throw new IllegalArgumentException("null element");
        int counter=0;
        long next=next_to_add.getAndIncrement();
        while(true) {
            int tmp_size=size.incrementAndGet();

            if(tmp_size < capacity) {
                int index=(int)(next % capacity);

                AtomicReference<T> ref=queue[index];

                if(ref.compareAndSet(null, el)) {
                    // System.out.println("add(" + el + "), next=" + next + " (index " + index + ", size=" + size + ")");
                    return;
                }
            }
            size.decrementAndGet();

            if(counter >= 5)
                LockSupport.parkNanos(10); // sleep a little after N attempts -- make configurable
            else
                counter++;
        }
    }

    public T remove() {
        long next=next_to_remove.getAndIncrement();
        if(next >= next_to_add.get()) {
            next_to_remove.decrementAndGet();
            return null;
        }

        int index=(int)(next % capacity);
        AtomicReference<T> ref=queue[index];
        T retval=ref.get();

        // System.out.println("remove(): retval = " + retval);
        if(retval != null && ref.compareAndSet(retval, null)) {
            size.decrementAndGet();
            return retval;
        }

        return null;
    }

    public String dumpNonNullElements() {
        StringBuilder sb=new StringBuilder();
        for(AtomicReference<T> ref: queue)
            if(ref.get() != null)
                sb.append(ref.get() + " ");
        return sb.toString();
    }

    public String toString() {
        return size.get() + " elements";
    }
}

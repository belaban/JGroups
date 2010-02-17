package org.jgroups.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.5 2010/02/17 08:58:13 belaban Exp $
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

            if(counter >= 3)
                LockSupport.parkNanos(10); // sleep for 10 ns after 10 attempts
            else
                counter++;
        }
    }

    public T remove() {
        while(true) {
            long next=next_to_remove.get();
            if(next >= next_to_add.get())
                break;
            int index=(int)(next % capacity);

            // T retval=queue[index];
            AtomicReference<T> ref=queue[index];
            T retval=ref.get();


            // System.out.println("remove(): retval = " + retval);
            if(ref.compareAndSet(retval, null)) {
                //System.out.println("removed " + retval + ", next=" + next + " (index " + index + ", size=" +
                //      (next_to_add.get() - next_to_remove.get() + ")"));

                size.decrementAndGet();

                if(next_to_remove.compareAndSet(next, next +1)) {
                    ;
                }
                else
                    System.err.println("\n** remove(): CAS(" + next + ", " + (next+1) +
                            ") failed, next is " + next_to_remove.get());

                return retval;
            }

        }

        return null;
    }

    public String toString() {
        return size.get() + " elements";
    }
}

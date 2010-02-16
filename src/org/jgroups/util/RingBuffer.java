package org.jgroups.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.4 2010/02/16 14:10:51 belaban Exp $
 */
public class RingBuffer<T> {
    private final AtomicReferenceArray<T> queue;
    private final int                     capacity;
    private AtomicLong                    next_to_add=new AtomicLong(0);
    private AtomicLong                    next_to_remove=new AtomicLong(0);

    private static final int LAG=1;


    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        queue=new AtomicReferenceArray<T>(capacity);
        this.capacity=capacity;
    }

    /**
     * Adds an elements into the buffer. Blocks if full
     * @param el
     */
    public void add(T el) {
        if(el == null)
            throw new IllegalArgumentException("null element");
        int counter=0;
        while(true) {
            long next=next_to_add.get();
            long size=next - next_to_remove.get();
            if(size + LAG < capacity) {
                int index=(int)(next % capacity);

                if(queue.compareAndSet(index, null, el)) {
                    // System.out.println("add(" + el + "), next=" + next + " (index " + index + ", size=" + size + ")");

                    if(next_to_add.compareAndSet(next, next +1)) {
                        ;
                    }
                    else
                        System.err.println("\n** add(" + el + "): CAS(" + next + ", " + (next+1) + ") failed, " +
                                "next is " + next_to_add.get() + ", index=" + index);

                    return;
                }

            }

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
            T retval=queue.get(index);
            // System.out.println("remove(): retval = " + retval);
            if(retval != null && queue.compareAndSet(index, retval, null)) {

                //System.out.println("removed " + retval + ", next=" + next + " (index " + index + ", size=" +
                  //      (next_to_add.get() - next_to_remove.get() + ")"));

                if(next_to_remove.compareAndSet(next, next +1)) {
                    ;
                }
                else
                    System.err.println("\n** remove(): CAS(" + next + ", " + (next+1) + ") failed, next is " + next_to_remove.get());

                return retval;
            }
        }

        return null;
    }



}

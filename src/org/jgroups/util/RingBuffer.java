package org.jgroups.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.2 2010/02/15 11:51:39 belaban Exp $
 */
public class RingBuffer<T> {
    private final T[]           queue;
    private final AtomicInteger size=new AtomicInteger(0);
    private AtomicLong          next_to_add=new AtomicLong(0);
    private AtomicLong          next_to_remove=new AtomicLong(0);


    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        queue=(T[])new Object[capacity];
    }

    /**
     * Adds an elements into the buffer. Blocks if full
     * @param obj
     */
    public void add(T obj) {
        int counter=0;
        while(true) {
            if(size.get() < queue.length) {
                long index=next_to_add.get();
                if(next_to_add.compareAndSet(index, index +1)) {
                    size.incrementAndGet();
                    queue[(int)(index % queue.length)]=obj;
                    return;
                }
            }

            if(counter >= 10)
                LockSupport.parkNanos(10); // sleep for 10 ns after 10 attempts
            else
                counter++;
        }
    }

    public T remove() {
        while(true) {
            if(next_to_remove.get() >= next_to_add.get())
                break;
            long index=next_to_remove.get();
            T retval=queue[(int)(index % queue.length)];
            if(retval != null && next_to_remove.compareAndSet(index, index +1)) {
                size.decrementAndGet();
                return retval;
            }
        }

        return null;
    }

    public void dump() {
    }




    public static void main(String[] args) {
        RingBuffer<Integer> queue=new RingBuffer<Integer>(3);
        queue.add(1);
        queue.add(2);
        queue.add(3);

        Object val=queue.remove();
        val=queue.remove();
        val=queue.remove();
    }
}

package org.jgroups.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.1 2010/02/15 10:42:33 belaban Exp $
 */
public class RingBuffer<T> {
    private final T[]           queue;
    private final AtomicInteger size=new AtomicInteger(0);
    private AtomicInteger       next_to_add=new AtomicInteger(0);
    private AtomicInteger       next_to_remove=new AtomicInteger(0);


    private final AtomicInteger remove_counter=new AtomicInteger(0);

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
                int index=next_to_add.get();
                if(next_to_add.compareAndSet(index, (index +1) % queue.length)) {
                    queue[index]=obj;
                    size.incrementAndGet();
                    return;
                }
            }
            counter++;
            if(counter >= 10)
                LockSupport.parkNanos(10); // sleep for 10 ns after 10 attempts
        }
    }

    public T remove() {
        while(true) {
            if(size.get() == 0)
                break;
            int index=next_to_remove.get();
            if(next_to_remove.compareAndSet(index, (index +1) % queue.length)) {
                size.decrementAndGet();
                return queue[index];
            }
            else
                remove_counter.incrementAndGet();
        }

        return null;
    }

    public void dump() {
        System.out.println("remove_counter=" + remove_counter);
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

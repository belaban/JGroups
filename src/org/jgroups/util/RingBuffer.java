package org.jgroups.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.3 2010/02/15 16:46:45 belaban Exp $
 */
public class RingBuffer<T> {
    private final T[]           queue;
    private AtomicLong          next_to_add=new AtomicLong(0);
    private AtomicLong          next_to_remove=new AtomicLong(0);

    private static final int LAG=1;


    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        queue=(T[])new Object[capacity];
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
            long size=next - next_to_remove.get() + LAG;
            if(size < queue.length) {
                int index=(int)(next % queue.length);
                if(next_to_add.compareAndSet(next, next +1)) {

                    // Util.sleep(2000);
                    
                    queue[index]=el;
                    // System.out.println("added " + el + ", next=" + next + " (index " + index + ", size=" + size + ")");
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
            int index=(int)(next % queue.length);
            T retval=queue[index];
            // System.out.println("remove(): retval = " + retval);
            if(retval != null && next_to_remove.compareAndSet(next, next +1)) {
                // System.out.println("removed " + retval + ", next=" + next + " (index " + index + ", size=" +
                    //    (next_to_add.get() - next_to_remove.get() + ")"));
                queue[index]=null;
                return retval;
            }
        }

        return null;
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

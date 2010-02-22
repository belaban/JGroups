package org.jgroups.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 
 * @author Bela Ban
 * @version $Id: RingBuffer.java,v 1.11 2010/02/22 12:28:32 belaban Exp $
 */
public class RingBuffer<T> {
    private final AtomicStampedReference<T>[]  queue;
    private final int                          capacity;
    private final AtomicLong                   next_to_add=new AtomicLong(0);
    private final AtomicLong                   next_to_remove=new AtomicLong(0);


    /*private final AtomicInteger successful_adds=new AtomicInteger(0);
    private final AtomicInteger successful_removes=new AtomicInteger(0);
    private final AtomicInteger failed_adds=new AtomicInteger(0);
    private final AtomicInteger failed_removes=new AtomicInteger(0);*/


    // private final ConcurrentLinkedQueue<String> operations=new ConcurrentLinkedQueue<String>();


    @SuppressWarnings("unchecked")
    public RingBuffer(int capacity) {
        queue=new AtomicStampedReference[capacity];
        this.capacity=capacity;
        for(int i=0; i < capacity; i++)
            queue[i]=new AtomicStampedReference<T>(null, -1);

        /*Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("next_to_add=" + next_to_add + ", next_to_remove=" + next_to_remove);
                System.out.println("successful_adds=" + successful_adds + ", successful_removes=" + successful_removes);
                System.out.println("failed_adds=" + failed_adds + ", failed_removes=" + failed_removes);

//                System.out.println("\nOperations");
//                for(String op: operations)
//                    System.out.println(op);
            }
        });*/
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
        int stamp=(int)(next / capacity);

        while(true) {
            if(next - next_to_remove.get() < capacity) {
                int index=(int)(next % capacity);

                AtomicStampedReference<T> ref=queue[index];

                if(ref.compareAndSet(null, el, -1, stamp)) {
                    // operations.add("queue[" + index + "]=" + el + " (next=" + next + ", next_to_remove=" + next_to_remove + ")");
                    // successful_adds.incrementAndGet();
                    return;
                }
                else {
//                    failed_adds.incrementAndGet();
//                    System.err.println("add(" + el + ") failed at index " + index + ": next_to_add=" + next_to_add +
//                            ", next=" + next + ", next_to_remove=" + next_to_remove +
//                    ", queue[" + index + "]=" + queue[index].getReference());
                }
            }

            if(counter >= 5)
                LockSupport.parkNanos(10); // sleep a little after N attempts -- make configurable
            else
                counter++;
        }
    }

    public T remove() {
        long next=next_to_remove.get();
        if(next >= next_to_add.get())
            return null;

        int stamp=(int)(next / capacity);

        int index=(int)(next % capacity);
        AtomicStampedReference<T> ref=queue[index];
        T retval=ref.getReference();


        if(retval != null && ref.compareAndSet(retval, null, stamp, -1)) {
            // operations.add("queue[" + index + "]: " + retval + " = null (next=" + next + ")");
            // successful_removes.incrementAndGet();
            next_to_remove.incrementAndGet();
            return retval;
        }
//        else
//            failed_removes.incrementAndGet();

        return null;
    }

    public String dumpNonNullElements() {
        StringBuilder sb=new StringBuilder();
        for(AtomicStampedReference<T> ref: queue)
            if(ref.getReference() != null)
                sb.append(ref.getReference() + " ");
        return sb.toString();
    }

    public int size() {
        int size=0;
        int index=(int)(next_to_remove.get() % capacity);
        for(int i=index; i < index + capacity; i++)
            if(queue[i % capacity].getReference() != null)
                size++;
        return size;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(size() + " elements");
        if(size() < 100) {
            sb.append(": ").append(dumpNonNullElements());
        }

        return sb.toString();
    }
}

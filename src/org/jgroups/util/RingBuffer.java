package org.jgroups.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Ring buffer (typically of messages), implemented as an array.
 * Designed for multiple producers (add()) and a single consumer (remove()). <em>Note that the remove() method
 * is not reentrant, so multiple consumers won't work correctly !</em><p/>
 * The buffer has a fixed capacity, and a low (L), highest delivered (HR) and highest received (HR) seqno.<p/>
 * A message with a sequence number (seqno) > low + capacity or < HD will get discarded. Removal and addition of
 * messages never block; addition fails to add a message if the above conditions are met, or the message was already
 * removed. Removal of an element that has not yet been added returns null.<p/>
 * @author Bela Ban
 * @since 3.1
 */
public class RingBuffer<T> {
    /** Atomic ref array so that elements can be checked for null and set atomically */
    protected final AtomicReferenceArray<T> buf;

    /** The lowest seqno. Moved forward by stable() */
    protected volatile long                 low;

    /** The highest delivered seqno. Moved forward by a remove method. The next message to be removed is hd +1 */
    protected volatile long                 hd=0;

    /** The highest received seqno. Moved forward by add(). The next message to be added is hr +1 */
    protected final AtomicLong              hr=new AtomicLong(0);

    protected final long                    offset;


    /**
     * Creates a RingBuffer
     * @param capacity The number of elements the ring buffer's array should hold
     * @param offset The offset. The first element to be added has to be offset +1.
     */
    public RingBuffer(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);
        if(offset < 0)
            throw new IllegalArgumentException("invalid offset of " + offset);
        this.buf=new AtomicReferenceArray<T>(capacity);
        this.low=this.hd=this.offset=offset;
        this.hr.set(offset);
    }


    public boolean add(long seqno, T element) {
        validate(seqno);

        if(seqno <= hd)                 // seqno already delivered, includes check seqno <= low
            return false;

        if(seqno - low > capacity())    // seqno too big
            return false;

        // now we can set any slow > hd and yet not overwriting low (check #1 above)
        int index=index(seqno);
        if(!buf.compareAndSet(index, null, element)) // the element at buf[index] was already present
            return false;

        // now see if hr needs to moved forward
        for(;;) {
            long current_hr=hr.get();
            long new_hr=Math.max(seqno, current_hr);
            if(new_hr <= current_hr || hr.compareAndSet(current_hr, new_hr))
                break;
        }

        return true;
    }

    /**
     * Removes the next element (at hd +1). <em>Note that this method is not concurrent, as
     * RingBuffer can only have 1 remover thread active at any time !</em>
     * @param nullify Nulls the element in the array if true
     * @return T if there was a non-null element at position hd +1, or null if the element at hd+1 was null, or
     * hd+1 > hr.
     */
    public T remove(boolean nullify) {
        long tmp=hd+1;
        if(tmp > hr.get())
            return null;
        int index=index(tmp);
        T element=buf.get(index);
        if(element == null)
            return null;
        hd++;
        if(nullify)
            buf.compareAndSet(index, element, null);
        return element;
    }


     public boolean add2(long seqno, T element, final CountDownLatch one, final CountDownLatch two) throws InterruptedException {
         validate(seqno);

         if(seqno <= hd)                 // seqno already delivered, includes check seqno <= low
             return false;

         if(seqno - low > capacity())    // seqno too big
             return false;

         one.await();

         // now we can set any slow > hd and yet not overwriting low (check #1 above)
         int index=index(seqno);

         two.await();

         if(!buf.compareAndSet(index, null, element)) // the element at buf[index] was already present
             return false;

         // now see if hr needs to moved forward
         for(;;) {
             long current_hr=hr.get();
             long new_hr=Math.max(seqno, current_hr);
             if(new_hr <= current_hr || hr.compareAndSet(current_hr, new_hr))
                 break;
         }

         return true;
     }

    /**
     * Removes the next element (at hd +1). <em>Note that this method is not concurrent, as
     * RingBuffer can only have 1 remover thread active at any time !</em>
     * @param nullify Nulls the element in the array if true
     * @return T if there was a non-null element at position hd +1, or null if the element at hd+1 was null.
     */
    public T remove2(boolean nullify, final CountDownLatch one, final CountDownLatch two) throws InterruptedException {
        int index=index(hd +1);
        T element=buf.get(index);
        if(element == null)
            return null;

        one.await();

        hd++;
        if(nullify)
            buf.compareAndSet(index, element, null);

        two.await();

        return element;
    }




    /**
     * Removes the next element (at hd +1). <em>Note that this method is not concurrent, as
     * RingBuffer can only have 1 remover thread active at any time !</em>
     * @return T if there was a non-null element at position hd +1, or null if the element at hd+1 was null.
     */
    public T remove() {
        return remove(false);
    }

    /*public T[] removeMany(int max) {
        List<T> list=new ArrayList<T>(max);
        long tmp_hr=hr.get();
        long new_hd=hd;
        for(long i=hd +1; i <= tmp_hr; i++) {
            int index=index(i);
            T element=buf.get(index);
            if(element != null) {
                list.add(element);
                new_hd=i;
            }
            else
                break;
        }
        hd=new_hd;
        return (T[])list.toArray();
    }*/


    /** Nulls elements between low and seqno and forwards low */
    public void stable(long seqno) {
        validate(seqno);
        if(seqno > hd)
            throw new IllegalArgumentException("seqno " + seqno + " cannot be bigger than hd (" + hd + ")");
        int from=index(low+1), to=index(seqno);
        for(int i=from; i <= to; i++)
            buf.set(i, null);
        low=seqno;
    }

    public int capacity() {return buf.length();}

    public int size() {
        return count(false);
    }

    public int missing() {
        return count(true);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[" + low + " | " + hd + " | " + hr + "] (" + size() + " elements, " + missing() + " missing)");
        return sb.toString();
    }


    protected static void validate(long seqno) {
        if(seqno < 0)
            throw new IllegalArgumentException("seqno " + seqno + " cannot be negative");
    }

    protected int index(long seqno) {
        return (int)((seqno - offset -1) % capacity());
    }

    protected int count(boolean missing) {
        int retval=0;
        long tmp_hd=hd, tmp_hr=hr.get();
        for(long i=tmp_hd+1; i <= tmp_hr; i++) {
            int index=index(i);
            T element=buf.get(index);
            if(missing && element == null)
                retval++;
            if(!missing && element != null)
                retval++;
        }
        return retval;
    }
}

package org.jgroups.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ring buffer, implemented with a circular array.
 * Designed for multiple producers (add()) and a single consumer (remove()). <em>Note that the remove() methods
 * are not reentrant, so multiple consumers won't work correctly !</em><p/>
 * The buffer has a fixed capacity, and a low (LOW), highest delivered (HD) and highest received (HR) seqno.<p/>
 * An element with a sequence number (seqno) > low + capacity or < HD will get discarded.
 * <p/>
 * Elements are added after HD, but cannot wrap around beyond LOW. Addition doesn't need to be sequential, e.g.
 * adding 5, 6, 8 is OK (as long as a seqno doesn't pass LOW). Addition may advance HR. Addition of elements that
 * are already present is a no-op, and will not set the element again.
 * <p/>
 * Removal of elements starts at HD+1; any non-null element is removed and HD is advanced accordingly. If a remove
 * method is called with nullify=true, then removed elements are nulled and LOW is advanced as well (LOW=HD). Note
 * that <em>all</em> removals in a given RingBufferLockless must either have nullify=true, or all must be false. It is not
 * permitted to do some removals with nullify=true, and others with nullify=false, in the same RingBufferLockless.
 * <p/>
 * The {@link #stable(long)} method is called periodically; it nulls all elements between LOW and HD and advances LOW
 * to HD.
 * <p/>
 * @author Bela Ban
 * @since 3.1
 */
public class RingBufferSeqnoLockless<T> implements Iterable<T> {
    /** Atomic ref array so that elements can be checked for null and set atomically.  Should always be sized to a power of 2. */
    protected final AtomicReferenceArray<T> buf;

    /** The lowest seqno. Moved forward by stable() */
    protected volatile long                 low;

    /** The highest delivered seqno. Moved forward by a remove method. The next message to be removed is hd +1 */
    protected volatile long                 hd;

    /** The highest received seqno. Moved forward by add(). The next message to be added is hr +1 */
    protected final AtomicLong              hr=new AtomicLong(0);

    protected final long                    offset;

    /** Lock for adders to block on when the buffer is full */
    protected final Lock                    lock=new ReentrantLock();

    protected final Condition               buffer_full=lock.newCondition();

    protected volatile boolean              running=true;

    protected final AtomicBoolean           processing=new AtomicBoolean(false);


    /**
     * Creates a RingBuffer
     * @param capacity The number of elements the ring buffer's array should hold
     * @param offset The offset. The first element to be added has to be offset +1.
     */
    public RingBufferSeqnoLockless(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);
        if(offset < 0)
            throw new IllegalArgumentException("invalid offset of " + offset);

        // Find a power of 2 >= buffer capacity
        int cap = 1;
        while (capacity > cap)
           cap <<= 1;

        this.buf=new AtomicReferenceArray<>(cap);
        this.low=this.hd=this.offset=offset;
        this.hr.set(offset);
    }


    public long          getLow()                     {return low;}
    public long          getHighestDelivered()        {return hd;}
    public void          setHighestDelivered(long hd) {this.hd=hd;}
    public long          getHighestReceived()         {return hr.get();}
    public long[]        getDigest()                  {return new long[]{hd, hr.get()};}
    public AtomicBoolean getProcessing()              {return processing;}


    
    public boolean add(long seqno, T element) {
        return add(seqno, element, false);
    }


    /**
     * Adds a new element to the buffer
     * @param seqno The seqno of the element
     * @param element The element
     * @param block If true, add() will block when the buffer is full until there is space. Else, add() will
     * return immediately, either successfully or unsuccessfully (if the buffer is full)
     * @return True if the element was added, false otherwise.
     */
    public boolean  add(long seqno, T element, boolean block) {
        validate(seqno);

        if(seqno <= hd)                 // seqno already delivered, includes check seqno <= low
            return false;

        if(seqno - low > capacity() && (!block || !block(seqno)))  // seqno too big
            return false;

        // now we can set any slow > hd and yet not overwriting low (check #1 above)
        int index=index(seqno);

        // Fix for correctness check #1 (see doc/design/RingBuffer.txt)
        if(buf.get(index) != null || seqno <= hd)
            return false;

        if(!buf.compareAndSet(index, null, element)) // the element at buf[index] was already present
            return false;

        // now see if hr needs to moved forward, this can be concurrent as we may have multiple producers
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
        hd=tmp;

        if(nullify) {
            long tmp_low=low;
            if(tmp == tmp_low +1)
                buf.compareAndSet(index, element, null);
            else {
                int from=index(tmp_low+1), length=(int)(tmp - tmp_low), capacity=capacity();
                for(int i=from; i < from+length; i++) {
                    index=i & (capacity - 1);
                    buf.set(index, null);
                }
            }
            low=tmp;
            lock.lock();
            try {
                buffer_full.signalAll();
            }
            finally {
                lock.unlock();
            }
        }
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

    public List<T> removeMany(boolean nullify, int max_results) {
        return removeMany(null, nullify, max_results);
    }


    public List<T> removeMany(final AtomicBoolean processing, boolean nullify, int max_results) {
        List<T> list=null;
        int num_results=0;
        long original_hd=hd, start=original_hd, end=hr.get();
        T element;
        while(start+1 <= end && (element=buf.get(index(start+1))) != null) {
            if(list == null)
                list=new ArrayList<>(max_results > 0? max_results : 20);
            list.add(element);
            start++;
            if(max_results > 0 && ++num_results >= max_results)
                break;
        }

        if(start > original_hd) { // do we need to move HD forward ?
            hd=start;
            if(nullify) {
                long tmp_low=low;
                int from=index(tmp_low+1), length=(int)(start - tmp_low), capacity=capacity();
                for(int i=from; i < from+length; i++) {
                    int index=i & (capacity - 1);
                    buf.set(index, null);
                }
                // Releases some of the blocked adders
                if(start > low) {
                    low=start;
                    lock.lock();
                    try {
                        buffer_full.signalAll();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            }
        }
        
        if((list == null || list.isEmpty()) && processing != null)
            processing.set(false);
        return list;
    }

    public T get(long seqno) {
        validate(seqno);
        if(seqno <= low || seqno > hr.get())
            return null;
        int index=index(seqno);
        return buf.get(index);
    }

    /** Only used for testing !! */
    public T _get(long seqno) {
        int index=index(seqno);
        return index < 0? null : buf.get(index);
    }


    /**
     * Returns a list of messages in the range [from .. to], including from and to
     * @param from
     * @param to
     * @return A list of messages, or null if none in range [from .. to] was found
     */
    public List<T> get(long from, long to) {
        if(from > to)
            throw new IllegalArgumentException("from (" + from + ") has to be <= to (" + to + ")");
        validate(from);
        List<T> retval=null;
        for(long i=from; i <= to; i++) {
            T element=get(i);
            if(element != null) {
                if(retval == null)
                    retval=new ArrayList<>();
                retval.add(element);
            }
        }
        return retval;
    }


    /** Nulls elements between low and seqno and forwards low */
    public void stable(long seqno) {
        validate(seqno);
        if(seqno <= low)
            return;
        if(seqno > hd)
            throw new IllegalArgumentException("seqno " + seqno + " cannot be bigger than hd (" + hd + ")");

        int from=index(low+1), length=(int)(seqno - low), capacity=capacity();
        for(int i=from; i < from+length; i++) {
            int index=i & (capacity - 1);
            buf.set(index, null);
        }

        // Releases some of the blocked adders
        if(seqno > low) {
            low=seqno;
            lock.lock();
            try {
                buffer_full.signalAll();
            }
            finally {
                lock.unlock();
            }
        }
    }

    public void destroy() {
        lock.lock();
        try {
            running=false;
            buffer_full.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public final int capacity()   {return buf.length();}
    public int       size()       {return count(false);}
    public int       missing()    {return count(true);}
    public int       spaceUsed()  {return (int)(hr.get() - low);}
    public double    saturation() {
        int space=spaceUsed();
        return space == 0? 0.0 : space / (double)capacity();
    }

    public SeqnoList getMissing() {
        SeqnoList missing=null;
        long tmp_hd=hd, tmp_hr=hr.get();
        for(long i=tmp_hd+1; i <= tmp_hr; i++) {
            if(buf.get(index(i)) == null) {
                if(missing == null)
                    missing=new SeqnoList((int)(tmp_hr-tmp_hd), hd);
                long end=i;
                while(buf.get(index(end+1)) == null && end <= tmp_hr)
                    end++;

                if(end == i)
                    missing.add(i);
                else {
                    missing.add(i, end);
                    i=end;
                }
            }
        }
        return missing;
    }

    /**
     * Returns an iterator over the elements of the ring buffer in the range [HD+1 .. HR]
     * @return RingBufferIterator
     * @throws NoSuchElementException is HD is moved forward during the iteration
     */
    public Iterator<T> iterator() {
        return new RingBufferIterator(buf);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[" + low + " | " + hd + " | " + hr + "] (" + size() + " elements, " + missing() + " missing)");
        return sb.toString();
    }


    
    protected static final void validate(long seqno) {
        if(seqno < 0)
            throw new IllegalArgumentException("seqno " + seqno + " cannot be negative");
    }

    protected int index(long seqno) {
        return (int)((seqno - offset -1) & (capacity() - 1));
    }

    protected boolean block(long seqno) {
        lock.lock();
        try {
            while(running && seqno - low > capacity()) {
                try {
                    buffer_full.await();
                }
                catch(InterruptedException e) {
                }
            }
            return running;
        }
        finally {
            lock.unlock();
        }
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


    protected class RingBufferIterator implements Iterator<T> {
        protected final AtomicReferenceArray<T> buffer;
        protected long current=hd+1;

        public RingBufferIterator(AtomicReferenceArray<T> buffer) {
            this.buffer=buffer;
        }

        public boolean hasNext() {
            return current <= hr.get();
        }

        public T next() {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            if(current <= hd)
                current=hd+1;
            return buffer.get(index(current++));
        }

        public void remove() {}
    }

}

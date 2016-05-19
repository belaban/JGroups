package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * that <em>all</em> removals in a given RingBuffer must either have nullify=true, or all must be false. It is not
 * permitted to do some removals with nullify=true, and others with nullify=false, in the same RingBuffer.
 * <p/>
 * The {@link #stable(long)} method is called periodically; it nulls all elements between LOW and HD and advances LOW
 * to HD.
 * <p/>
 * The design of RingBuffer is discussed in doc/design/RingBufferSeqno.txt.
 * <p/>
 * @author Bela Ban
 * @since 3.1
 */
public class RingBufferSeqno<T> implements Iterable<T> {
    /** Atomic ref array so that elements can be checked for null and set atomically.  Should always be sized to a power of 2. */
    protected final T[]            buf;

    /** The lowest seqno. Moved forward by stable() */
    protected long                 low;

    /** The highest delivered seqno. Moved forward by a remove method. The next message to be removed is hd +1 */
    protected long                 hd;

    /** The highest received seqno. Moved forward by add(). The next message to be added is hr +1 */
    protected long                 hr;

    protected final long           offset;

    /** Lock for adders to block on when the buffer is full */
    protected final Lock           lock=new ReentrantLock();

    protected final Condition      buffer_full=lock.newCondition();

    protected boolean              running=true;

    protected final AtomicBoolean  processing=new AtomicBoolean(false);


    /**
     * Creates a RingBuffer
     * @param capacity The number of elements the ring buffer's array should hold.
     * @param offset The offset. The first element to be added has to be offset +1.
     */
    public RingBufferSeqno(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);
        if(offset < 0)
            throw new IllegalArgumentException("invalid offset of " + offset);

        // Find a power of 2 >= buffer capacity
        int cap = 1;
        while (capacity > cap)
           cap <<= 1;

        this.buf=(T[])new Object[cap];
        this.low=this.hd=this.hr=this.offset=offset;
    }


    public long          getLow()                     {return low;}
    public long          getHighestDelivered()        {return hd;}
    public void          setHighestDelivered(long hd) {this.hd=hd;}
    public long          getHighestReceived()         {return hr;}
    public long[]        getDigest()                  {return new long[]{hd, hr};}
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
    public boolean add(long seqno, T element, boolean block) {
        lock.lock();
        try {
            if(seqno <= hd)                 // seqno already delivered, includes check seqno <= low
                return false;

            if(seqno - low > capacity() && (!block || !block(seqno)))  // seqno too big
                return false;

            int index=index(seqno);

            if(buf[index] != null)
                return false;
            else
                buf[index]=element;

            // now see if hr needs to moved forward, this can be concurrent as we may have multiple producers
            if(seqno > hr)
                hr=seqno;
            return true;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes the next element (at hd +1). <em>Note that this method is not concurrent, as
     * RingBuffer can only have 1 remover thread active at any time !</em>
     * @param nullify Nulls the element in the array if true
     * @return T if there was a non-null element at position hd +1, or null if the element at hd+1 was null, or
     * hd+1 > hr.
     */
    public T remove(boolean nullify) {
        lock.lock();
        try {
            long tmp=hd+1;
            if(tmp > hr)
                return null;
            int index=index(tmp);
            T element=buf[index];
            if(element == null)
                return null;
            hd=tmp;

            if(nullify) {
                if(tmp == low +1)
                    buf[index]=null;
                else {
                    int from=index(low+1), length=(int)(tmp - low), capacity=capacity();
                    for(int i=from; i < from+length; i++) {
                        index=i & (capacity - 1);
                        buf[index]=null;
                    }
                }
                low=tmp;
                buffer_full.signalAll();
            }
            return element;
        }
        finally {
            lock.unlock();
        }
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
        T element;

        lock.lock();
        try {
            long start=hd, end=hr;
            while(start+1 <= end && (element=buf[index(start+1)]) != null) {
                if(list == null)
                    list=new ArrayList<>(max_results > 0? max_results : 20);
                list.add(element);
                start++;
                if(max_results > 0 && ++num_results >= max_results)
                    break;
            }

            if(start > hd) { // do we need to move HD forward ?
                hd=start;
                if(nullify) {
                    int from=index(low+1), length=(int)(start - low), capacity=capacity();
                    for(int i=from; i < from+length; i++) {
                        int index=i & (capacity - 1);
                        buf[index]=null;
                    }
                    // Releases some of the blocked adders
                    if(start > low) {
                        low=start;
                        buffer_full.signalAll();
                    }
                }
            }
        
            if((list == null || list.isEmpty()) && processing != null)
                processing.set(false);
            return list;
        }
        finally {
            lock.unlock();
        }
    }

    public T get(long seqno) {
        lock.lock();
        try {
            if(seqno <= low || seqno > hr)
                return null;
            int index=index(seqno);
            return buf[index];
        }
        finally {
            lock.unlock();
        }
    }

    /** Only used for testing !! */
    public T _get(long seqno) {
        int index=index(seqno);
        lock.lock();
        try {
            return index < 0? null : buf[index];
        }
        finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            if(seqno <= low)
                return;
            if(seqno > hd)
                throw new IllegalArgumentException("seqno " + seqno + " cannot be bigger than hd (" + hd + ")");

            int from=index(low+1), length=(int)(seqno - low), capacity=capacity();
            for(int i=from; i < from+length; i++) {
                int index=i & (capacity - 1);
                buf[index]=null;
            }

            // Releases some of the blocked adders
            if(seqno > low) {
                low=seqno;
                buffer_full.signalAll();
            }
        }
        finally {
            lock.unlock();
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

    public final int capacity()   {return buf.length;}
    public int       size()       {return count(false);}
    public int       missing()    {return count(true);}
    public int       spaceUsed()  {return (int)(hr - low);}
    public double    saturation() {
        int space=spaceUsed();
        return space == 0? 0.0 : space / (double)capacity();
    }

    public SeqnoList getMissing() {
        SeqnoList missing=null;
        long tmp_hd=hd, tmp_hr=hr;
        for(long i=tmp_hd+1; i <= tmp_hr; i++) {
            if(buf[index(i)] == null) {
                if(missing == null)
                    missing=new SeqnoList((int)(hr-hd), hd);
                long end=i;
                while(buf[index(end+1)] == null && end <= tmp_hr)
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


    
    protected int index(long seqno) {
        return (int)((seqno - offset -1) & (capacity() - 1));
    }

    @GuardedBy("lock")
    protected boolean block(long seqno) {
        while(running && seqno - low > capacity()) {
            try {
                buffer_full.await();
            }
            catch(InterruptedException e) {
            }
        }
        return running;
    }

    protected int count(boolean missing) {
        int retval=0;
        long tmp_hd=hd, tmp_hr=hr;
        for(long i=tmp_hd+1; i <= tmp_hr; i++) {
            int index=index(i);
            T element=buf[index];
            if(missing && element == null)
                retval++;
            if(!missing && element != null)
                retval++;
        }
        return retval;
    }


    protected class RingBufferIterator implements Iterator<T> {
        protected final T[] buffer;
        protected long current=hd+1;

        public RingBufferIterator(T[] buffer) {
            this.buffer=buffer;
        }

        public boolean hasNext() {
            return current <= hr;
        }

        public T next() {
            if(!hasNext()){
                throw new NoSuchElementException();
            }
            if(current <= hd)
                current=hd+1;
            return buffer[index(current++)];
        }

        public void remove() {}
    }

}

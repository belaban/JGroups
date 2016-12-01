package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.LongStream;

/**
 * Table for storing requests associated with monotonically increasing sequence numbers (seqnos).<p/>
 * Could be used for example in {@link org.jgroups.blocks.RequestCorrelator}. Grows and shrinks when needed.
 * Addition is always at the end, yielding monotonically increasing seqnos. Removal is done by nulling the element(s)
 * between low and high and advancing the low pointer whenever possible.<p/>
 * See <a href="https://issues.jboss.org/browse/JGRP-1982">JGRP-1982</a> for details.
 * @author Bela Ban
 * @since  3.6.7
 */
public class RequestTable<T> {
    protected T[]        buffer;                   // the ring buffer
    protected long       low;                      // pointing to the next element to be removed; low is always <= high
    protected long       high;                     // pointing to the next element to be added; high is >= low
    protected int        removes_till_compaction;  // number of removes before attempt compaction (0 disables this)
    protected int        num_removes;              // current number of removes
    protected final Lock lock=new ReentrantLock(); // to synchronize modifications

    public interface Visitor<T> {
        boolean visit(T element);
    }


    public RequestTable(final int capacity) {
        this(capacity, 0, 0);
    }

    public RequestTable(final int capacity, long low, long high) {
        int len=Util.getNextHigherPowerOfTwo(capacity);
        this.buffer=(T[])new Object[len];
        this.low=low;
        this.high=high;
    }

    public long            low()                           {return low;}
    public long            high()                          {return high;}
    public int             capacity()                      {return buffer.length;}
    public int             index(long seqno)               {return (int)((seqno) & (capacity()-1));}
    public int             removesTillCompaction()         {return removes_till_compaction;}
    public RequestTable<T> removesTillCompaction(int rems) {this.removes_till_compaction=rems; return this;}


    /**
     * Adds a new element and returns the sequence number at which it was inserted. Advances the high
     * pointer and grows the buffer if needed.
     * @param element the element to be added. Must not be null or an exception will be thrown
     * @return the seqno at which element was added
     */
    public long add(T element) {
        lock.lock();
        try {
            long next=high+1;
            if(next - low > capacity())
                _grow(next-low);
            int high_index=index(high);
            buffer[high_index]=element;
            return high++;
        }
        finally {
            lock.unlock();
        }
    }


    public T get(long seqno) {
        lock.lock();
        try {
            int index=index(seqno);
            return buffer[index];
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes the element at the index matching seqno. If seqno == low, tries to advance low until a non-null element
     * is encountered, up to high
     * @param seqno
     * @return
     */
    public T remove(long seqno) {
        lock.lock();
        try {
            if(seqno < low || seqno > high)
                return null;
            int index=index(seqno);
            T retval=buffer[index];
            if(retval != null && removes_till_compaction > 0)
                num_removes++;
            buffer[index]=null;
            if(seqno == low)
                advanceLow();
            if(removes_till_compaction > 0 && num_removes >= removes_till_compaction) {
                _compact();
                num_removes=0;
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes all elements in the stream. Calls the consumer (if not null) on non-null elements
     */
    public RequestTable<T> removeMany(LongStream seqnos, Consumer<T> consumer) {
        if(seqnos == null) return this;
        AtomicBoolean advance=new AtomicBoolean(false);

        seqnos.forEach(seqno -> {
            T element=null;
            lock.lock();
            try {
                if(seqno < low || seqno > high)
                    return;
                int index=index(seqno);
                if((element=buffer[index]) != null && removes_till_compaction > 0)
                    num_removes++;
                buffer[index]=null;
                if(seqno == low)
                    advance.set(true);
            }
            finally {
                lock.unlock();
            }
            if(consumer != null)
                consumer.accept(element);
        });

        lock.lock();
        try {
            if(advance.get())
                advanceLow();
            if(removes_till_compaction > 0 && num_removes >= removes_till_compaction) {
                _compact();
                num_removes=0;
            }
        }
        finally {
            lock.unlock();
        }
        return this;
    }


    /** Removes all elements, compacts the buffer and sets low=high=0 */
    public RequestTable<T> clear() {return clear(0);}

    public RequestTable<T> clear(long mark) {
        lock.lock();
        try {
            low=high=mark;
            buffer=(T[])new Object[2];
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    public RequestTable<T> forEach(Visitor<T> visitor) {
        if(visitor == null) return null;
        lock.lock();
        try {
            for(long i=low, num_iterations=0; i < high && num_iterations < buffer.length; i++, num_iterations++) {
                int index=index(i);
                T el=buffer[index];
                if(!visitor.visit(el))
                    break;
            }
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Non-blocking alternative to {@link #forEach(Visitor)}: iteration is performed on the array that exists at the
     * time of this call. Changes to the underlying array will not be reflected in the iteration.
     * @param visitor the {@link Visitor}.
     */
    public RequestTable<T> forEachNonBlocking(Visitor<T> visitor) {
        if(visitor == null) return null;
        T[]  buf;
        long lo, hi;
        lock.lock();
        try {
            buf=this.buffer; lo=this.low; hi=this.high;
        }
        finally {
            lock.unlock();
        }

        for(long i=lo, num_iterations=0; i < hi && num_iterations < buf.length; i++, num_iterations++) {
            int index=index(i);
            T el=buf[index];
            if(!visitor.visit(el))
                break;
        }
        return this;
    }

    /**
     * Grows the array to at least new_capacity. This method is mainly used for testing and is not typically called
     * directly, but indirectly when adding elements and the underlying array has no space left.
     * @param new_capacity the new capacity of the underlying array. Will be rounded up to the nearest power of 2 value.
     *                     A value smaller than the current capacity is ignored.
     */
    public RequestTable<T> grow(int new_capacity) {
        lock.lock();
        try {
            _grow(new_capacity);
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Shrinks the underlying array to half its size _if_ the new array can hold all of the existing elements.
     * @return true if the compaction succeeded, or false if it failed (e.g. not enough space)
     */
    public boolean compact() {
        lock.lock();
        try {
            return _compact();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Checks if there is at least buffer.length/2 contiguous space in range [low+1 .. high-1] available
     */
    public boolean contiguousSpaceAvailable() {
        lock.lock();
        try {
            return _contiguousSpaceAvailable(buffer.length >> 1);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of non-null elements in range [low .. high-1]
     * @return
     */
    public int size() {
        int retval=0;
        for(long i=low, num_iterations=0; i < high && num_iterations < buffer.length; i++, num_iterations++) {
            int index=index(i);
            if(buffer[index] != null)
                retval++;
        }
        return retval;
    }

    public String toString() {
        return String.format("low=%d high=%d cap=%d, %d element(s)", low, high, buffer.length, size());
    }

    @GuardedBy("lock")
    protected void _grow(long new_capacity) {
        int new_cap=Util.getNextHigherPowerOfTwo((int)Math.max(buffer.length, new_capacity));
        if(new_cap == buffer.length)
            return;
        _copy(new_cap);
    }

    /**
     * Shrinks the array to half of its current size if the current number of elements fit into half of the capacity.
     * @return true if the compaction succeeded, else false (e.g. when the current elements would not fit)
     */
    @GuardedBy("lock")
    protected boolean _compact() {
        int new_cap=buffer.length >> 1; // needs to be a power of 2 for efficient modulo operation, e.g. for index()
        // boolean compactable=this.buffer.length > 0 && (size() <= new_cap || (contiguousSpaceAvailable=_contiguousSpaceAvailable(new_cap)));
        boolean compactable=this.buffer.length > 0 && high-low <= new_cap;
        if(!compactable)
            return false; // not enough space to shrink the buffer to half its size
        _copy(new_cap);
        return true;
    }

    public String dumpContents() {
        StringBuilder sb=new StringBuilder();
        lock.lock();
        try {
            int new_cap=buffer.length >> 1;
            for(long i=low, num_iterations=0; i < high && num_iterations < buffer.length; i++, num_iterations++) {
                int index=index(i);
                T el=buffer[index];
                if(el != null) {
                    long hash=el.hashCode();
                    int small_idx=index(i, new_cap);
                    sb.append(String.format("seqno %d: index: %d val: %d, index in %d-buffer: %d\n", i, index, hash, new_cap, small_idx));
                }
            }
        }
        finally {
            lock.unlock();
        }
        return sb.toString();
    }


    /** Copies elements from old into new array */
    protected void _copy(int new_cap) {
        // copy elements from [low to high-1] into new indices in new array
        T[] new_buf=(T[])new Object[new_cap];
        int new_len=new_buf.length;
        int old_len=this.buffer.length;

        for(long i=low, num_iterations=0; i < high && num_iterations < old_len; i++, num_iterations++) {
            int old_index=index(i, old_len);
            if(this.buffer[old_index] != null) {
                int new_index=index(i, new_len);
                new_buf[new_index]=this.buffer[old_index];
            }
        }
        this.buffer=new_buf;
    }



    /**
     * Check if we have at least space_needed contiguous free slots available in range [low+1 .. high-1]
     * @param space_needed the number of contiguous free slots required to do compaction, usually half of the current
     *                     buffer size
     * @return true if a contiguous space was found, false otherwise
     */
    @GuardedBy("lock")
    protected boolean _contiguousSpaceAvailable(int space_needed) {
        int num_slots_scanned=0;
        int size_of_contiguous_area=0;

        if(high-low-1 < space_needed)
            return false;

        for(long i=low+1; i < high; i++) {
            num_slots_scanned++;
            int index=index(i);
            if(this.buffer[index] == null) {
                if(++size_of_contiguous_area >= space_needed)
                    return true;
            }
            else {
                size_of_contiguous_area=0;
                // we scanned more than half of the current array and found an occupied slot, so there is no chance of
                // finding space_needed contiguous free slots as we have less than half of the current array to scan
                if(num_slots_scanned > space_needed || high-i-1 < space_needed)
                    return false;
            }
        }
        return false;
    }


    protected int highestContiguousSpaceAvailable() {
        int size_of_current_contiguous_area=0;
        int highest=0;

        for(long i=low+1; i < high; i++) {
            int index=index(i);
            if(this.buffer[index] == null)
                size_of_current_contiguous_area++;
            else {
                highest=Math.max(highest, size_of_current_contiguous_area);
                size_of_current_contiguous_area=0;
            }
        }
        return Math.max(highest, size_of_current_contiguous_area);
    }

    @GuardedBy("lock")
    protected void advanceLow() {
        while(low < high) {
            int index=index(low);
            if(buffer[index] != null)
                break;
            low++;
        }
    }

    protected static int index(long seqno, int length) {return (int)((seqno) & length-1);}

}

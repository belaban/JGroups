package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Ring buffer of fixed capacity. Indices low and high point to the beginning and end of the buffer. Sequence numbers
 * (seqnos) are mapped to an index by <pre>seqno % capacity</pre>. High can never pass low, and drops the element
 * or blocks when that's the case.<br/>
 * Note that 'null' is not a valid element, but signifies a missing element<br/>
 * The design is described in doc/design/FixedBuffer.txt.
 * @author Bela Ban
 * @since  5.4
 */
public class FixedBuffer<T> extends Buffer<T> {
    /** Holds the elements */
    protected T[]                 buf;
    protected final Condition     buffer_full=lock.newCondition();

    /** Used to unblock blocked senders on close(). When false, senders don't block when full but discard element */
    protected boolean             open=true;

    protected final LongAdder     num_blockings=new LongAdder();
    protected final AverageMinMax avg_time_blocked=new AverageMinMax(512).unit(TimeUnit.NANOSECONDS);
    /** Number of received messages dropped due to full buffer */
    protected final LongAdder     num_dropped_msgs=new LongAdder();


    public FixedBuffer() {
        this(0);
    }

    public FixedBuffer(long offset) {
        this(32, offset);
    }

    /**
     * Creates a RingBuffer
     *
     * @param capacity The number of elements the ring buffer's array should hold.
     * @param offset   The offset. The first element to be added has to be offset +1.
     */
    public FixedBuffer(int capacity, long offset) {
        if(capacity < 1)
            throw new IllegalArgumentException("incorrect capacity of " + capacity);
        this.buf=(T[])new Object[capacity];
        this.low=this.hd=this.high=this.offset=offset;
    }

    @Override public int capacity()           {return buf.length;}
    public long          numBlockings()       {return num_blockings.sum();}
    public AverageMinMax avgTimeBlocked()     {return avg_time_blocked;}
    public long          numDroppedMessages() {return num_dropped_msgs.sum();}

    @Override
    public boolean add(long seqno, T element, Predicate<T> remove_filter, boolean block_if_full) {
        lock.lock();
        try {
            long dist=seqno - low;
            if(dist <= 0)
                return false; // message already purged

            if(dist > capacity() && (!block_if_full || !block(seqno))) { // buffer is full
                num_dropped_msgs.increment();
                return false;
            }

            int index=index(seqno);
            if(buf[index] != null)
                return false; // message already present
            buf[index]=element;
            size++;

            // see if high needs to be moved forward
            if(seqno - high > 0)
                high=seqno;

            if(remove_filter != null && seqno - hd > 0) {
                Visitor<T> v=(seq,msg) -> {
                    if(msg == null || !remove_filter.test(msg))
                        return false;
                    if(seq - hd > 0)
                        hd=seq;
                    size=Math.max(size-1, 0);
                    return true;
                };
                forEach(highestDelivered()+1, high(), v, false, true);
            }
            return true;
        }
        finally {
            lock.unlock();
        }
    }


    @Override
    public boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value) {
        if(batch == null || batch.isEmpty())
            return false;
        Objects.requireNonNull(seqno_getter);
        boolean retval=false;
        lock.lock();
        try {
            for(Iterator<?> it=batch.iterator(); it.hasNext(); ) {
                T msg=(T)it.next();
                long seqno=seqno_getter.apply(msg);
                if(seqno < 0)
                    continue;
                T element=const_value != null? const_value : msg;
                boolean added=add(seqno, element, null, false);
                retval=retval || added;
                if(!added || remove_from_batch)
                    it.remove();
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean add(final List<LongTuple<T>> list, boolean remove_added_elements, T const_value) {
        if(list == null || list.isEmpty())
            return false;
        boolean added=false;
        lock.lock();
        try {
            for(Iterator<LongTuple<T>> it=list.iterator(); it.hasNext();) {
                LongTuple<T> tuple=it.next();
                long seqno=tuple.getVal1();
                T element=const_value != null? const_value : tuple.getVal2();
                if(add(seqno, element, null, false))
                    added=true;
                else if(remove_added_elements)
                    it.remove();
            }
            return added;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes the next non-null element and advances hd
     * @return T if there was a non-null element at hd+1, otherwise null
     */
    @Override
    public T remove(boolean nullify) {
        lock.lock();
        try {
            long tmp=hd + 1;
            if(tmp - high > 0)
                return null;
            int index=index(tmp);
            T element=buf[index];
            if(element != null) {
                hd=tmp;
                size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
                if(nullify) {
                    buf[index]=null;
                    if(hd - low > 0)
                        low=hd;
                }
                buffer_full.signalAll();
            }
            return element;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter) {
        return removeMany(nullify, max_results, filter, LinkedList::new, LinkedList::add);
    }

    @Override
    public <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter, Supplier<R> result_creator, BiConsumer<R,T> accumulator) {
        Remover<R> remover=new Remover<R>(max_results, filter, result_creator, accumulator);
        lock.lock();
        try {
            forEach(remover, nullify);
            return remover.getResult();
        }
        finally {
            lock.unlock();
        }
    }

    public T get(long seqno) {
        lock.lock();
        try {
            // if(seqno <= low || seqno > high)
            if(seqno - low <= 0 || seqno - high > 0)
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

    @Override
    public int purge(long seqno, boolean force) {
        int purged=0;
        lock.lock();
        try {
            if(seqno - low <= 0)  // ignore if seqno <= low
                return 0;

            if(force) {
                if(seqno - high > 0)
                    seqno=high;
            }
            else {
                if(seqno - hd > 0) // we cannot be higher than the highest removed seqno
                    seqno=hd;
            }
            long tmp=low;
            long from=low + 1;
            int distance=(int)(seqno - from +1);
            for(int i=0; i < distance; i++) {
                int index=index(from);
                if(buf[index] != null) {
                    buf[index]=null;
                    purged++;
                }
                low++; from++;
                hd=Math.max(hd,low);
            }
            if(force)
                size=computeSize();
            if(low - tmp > 0)
                buffer_full.signalAll();
            return purged;
        }
        finally {
            lock.unlock();
        }
    }

    /** Changes the size of the buffer. This method should NOT be used; it is only here to change config in perf tests
     * dynamically!! */
    public void changeCapacity(int new_capacity) {
        if(new_capacity == buf.length)
            return;
        lock.lock();
        try {
            if(new_capacity < buf.length)
                decreaseCapacity(new_capacity);
            else
                increaseCapacity(new_capacity);
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify) {
        forEach(from, to, visitor, nullify, false);
    }

    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify, boolean respect_stop) {
        if(from - to > 0) // same as if(from > to), but prevents long overflow
            return;
        int distance=(int)(to - from +1);
        long start=low;
        for(int i=0; i < distance; i++) {
            int index=index(from);
            T element=buf[index];
            boolean stop=visitor != null && !visitor.visit(from, element);
            if(stop && respect_stop)
                break;
            if(nullify && element != null) {
                buf[index]=null;
                if(from - low > 0)
                    low=from;
            }
            if(stop)
                break;
            from++;
        }
        if(low - start > 0)
            buffer_full.signalAll();
    }

    @Override
    public void resetStats() {
        super.resetStats();
        num_blockings.reset();
        num_dropped_msgs.reset();
        avg_time_blocked.clear();
    }

    public void open(boolean b) {
        lock.lock();
        try {
            open=b;
            buffer_full.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns an iterator over the elements of the ring buffer in the range [LOW+1 .. HIGH]
     *
     * @return FixedBufferIterator
     * @throws NoSuchElementException is HD is moved forward during the iteration
     */
    public Iterator<T> iterator() {
        return new FixedBufferIterator(buf);
    }

    @Override
    public Iterator<T> iterator(long from, long to) {
        return new FixedBufferIterator(buf);
    }

    @Override
    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    public Stream<T> stream(long from, long to) {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

    protected int index(long seqno) {
        return (int)((seqno-offset-1) % capacity());

        // apparently this is faster than mod for n^2 capacity
        // not true: JMH showed about the same perf (~25ns/op):
        // https://github.com/belaban/JmhTests/blob/master/src/main/java/org/jgroups/MyBenchmark.java
        //return (int)((seqno - offset - 1) & (capacity() - 1));
    }

    @GuardedBy("lock")
    protected boolean block(long seqno) {
        while(open && seqno - low > capacity()) {
            num_blockings.increment();
            long start=System.nanoTime();
            try {
                buffer_full.await();
            }
            catch(InterruptedException e) {
            }
            finally {
                long time=System.nanoTime()-start;
                avg_time_blocked.add(time);
            }
        }
        return open;
    }

    @GuardedBy("lock")
    protected void increaseCapacity(int new_cap) {
        T[] tmp=(T[])new Object[new_cap];
        System.arraycopy(buf, 0, tmp, 0, buf.length);
        this.buf=tmp;
    }

    @GuardedBy("lock")
    protected void decreaseCapacity(int new_cap) {
        if(size > new_cap)
            throw new IllegalStateException(String.format("size (%d) is > new capacity (%d)", size, new_cap));

        List<Tuple<Long,T>> list=new ArrayList<>(size);
        Visitor<T> v=(seqno,el) -> list.add(new Tuple<>(seqno, el));
        forEach(v, false);
        this.buf=(T[])new Object[new_cap];
        list.forEach(t -> add(t.val1(), t.val2()));
        size=computeSize();
    }


    protected class FixedBufferIterator implements Iterator<T> {
        protected final T[] buffer;
        protected long      current=hd+1;

        public FixedBufferIterator(T[] buffer) {
            this.buffer=buffer;
        }

        public boolean hasNext() {
            //    return current <= high;
            return high - current >= 0;
        }

        public T next() {
            if(!hasNext())
                throw new NoSuchElementException();
            // if(current <= low)
            if(hd - current >= 0)
                current=hd+1;
            return buffer[index(current++)];
        }

        public void remove() {}
    }

}

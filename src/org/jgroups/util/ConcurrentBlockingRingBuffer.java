package org.jgroups.util;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntBinaryOperator;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

/**
 * MPSC queue, based on a ring buffer implementation which optionally blocks on adding or removing of elements.
 * <br/>
 * The main fields are read-index (ri), write-index (wi) and size (all AtomicIntegers). Producers change wi and size,
 * the single consumer changes ri and size.
 * <br/>
 * A producer tries to increment size. If unsuccessful, it drops the message (or blocks). If successful, it increments
 * wi and writes the element to array[wi]. If size was incremented from 0 -> 1, a producer also wakes up the consumer.
 * <br/>
 * The single consumer tries to remove size elements (drainTo() or poll()), but returns when a null element is found
 * (see below). It then decrements size by the number of removed elements (N for drainTo() and 1 for a
 * successful poll()). If no elements are in the ring buffer, the consumer blocks (if configured), until it is woken
 * up by a producer adding the first element to the empty queue.
 * <br/>
 * Note that this class is designed for a single consumer and has undefined behavior if multiple consumers are used.
 * <br/>
 * There is a special case that has the consumer busy-polling (ri=wi=1): assume we have producers P1-P3,
 * each adding an element concurrently. P3 successfully incremented size from 0->1 and set wi=3. P2 also incremented
 * size from 1->2 and set wi=1, but didn't yet write to the array. P3 incremented size from 2->3 and set wi=2, but also
 * didn't write to the array yet. P1 woke up the consumer, but the consumer saw array[1]=null, array[2]=null,
 * array[3]=el3 (written by P3). The consumer therefore returns on the first element because it is null, but continues
 * looping because size=3. Only when P1 and P2 write their respective elements will the consumer be able to make
 * progress.
 * @author Bela Ban
 * @since  5.5.0
 */
public class ConcurrentBlockingRingBuffer<T> implements BlockingQueue<T> {
    protected final int                     capacity;
    protected final AtomicReferenceArray<T> array;
    protected final AtomicInteger           wi=new AtomicInteger(); // write index, accessed by all producers
    protected int                           ri; // read index, accessed only by a single consumer
    protected final AtomicInteger           size=new AtomicInteger(0);  // cannot exceed array.length
    protected final boolean                 block_on_empty, block_on_full;
    protected final Lock                    lock=new ReentrantLock();
    protected final Condition               not_empty=lock.newCondition();
    protected final Condition               not_full=lock.newCondition();
    protected final IntUnaryOperator        INCR; // increments up to capacity, never exceeds capacity
    protected final IntUnaryOperator        INCR_INDEX;
    protected static final IntUnaryOperator DECR=x -> x == 0? x : x-1; // decrements, but never < 0
    protected static final IntBinaryOperator DECR_DELTA=(x,y) -> y >= x? 0 : x-y;



    public ConcurrentBlockingRingBuffer(int capacity) {
        this(capacity, false, false);
    }

    public ConcurrentBlockingRingBuffer(int capacity, boolean block_on_empty, boolean block_on_full) {
        this.array=new AtomicReferenceArray<>(this.capacity=capacity);
        this.block_on_empty=block_on_empty;
        this.block_on_full=block_on_full;
        if(capacity == 0)
            throw new IllegalArgumentException(String.format("Capacity (%d) needs to be positive", capacity));
        INCR=x -> x >= capacity? x : x+1;
        INCR_INDEX=x -> x == (capacity-1)? 0 : x+1;
    }

    public static <T> ConcurrentBlockingRingBuffer<T> createBlocking(int capacity) {
        return new ConcurrentBlockingRingBuffer<>(capacity, true, true);
    }

    public int capacity()   {return array.length();}
    public int readIndex()  {return ri;}
    public int writeIndex() {return wi.get();}


    @Override
    public void put(T t) throws InterruptedException {
        Objects.requireNonNull(t);
        if(!block_on_full)
            throw new IllegalStateException(String.format("put() must not be called when block_on_full=%b", block_on_full));
        for(;;) {
            boolean rc=offer(t);
            if(rc)
                return;
            lock.lockInterruptibly();
            try {
                while(size.get() >= capacity)
                    not_full.await();
            }
            finally {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean add(T t) {
        boolean rc=offer(t);
        if(rc)
            return rc;
        throw new IllegalStateException(String.format("no space available: capacity=%d, size=%d", capacity, size.get()));
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(T t) {
        int old_size=size.getAndUpdate(INCR);
        if(old_size < capacity) {
            int index=wi.getAndUpdate(INCR_INDEX);
            array.set(index, t);
            if(old_size == 0 && block_on_empty)
                signalNotEmpty();
            return true;
        }
        return false;
    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        wi.set(ri=0);
        size.set(0);
        for(int i=0; i < capacity; i++)
            array.set(i, null);
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public T poll() {
        T val=array.get(ri);
        if(val == null)
            return null;
        int old_size=size.getAndUpdate(DECR); // decrement by 1
        if(old_size == 0)
            return null;
        // we need to null the element: if we didn't, a producer could update size but not yet write the
        // element to the array, and so the consumer would read an old element
        array.set(ri, null);
        ri=advance(ri);
        if(block_on_full && old_size >= capacity)
            signalNotFull();
        return val;
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super T> c, int max) {
        if(this == Objects.requireNonNull(c))
            throw new IllegalArgumentException();
        if(max <= 0)
            return 0;
        int drained=0;
        max=Math.min(max, size.get());
        for(int i=0; i < max; i++) {
            T el=array.get(ri);
            if(el == null)
                break;
            array.set(ri, null); // we *have* to null; see explanation in poll()
            c.add(el);
            drained++;
            ri=advance(ri);
        }
        if(drained > 0) {
            // update size: decrement size by the number of drained elements
            int old_size=size.getAndAccumulate(drained, DECR_DELTA);
            if(block_on_full && old_size >= capacity)
                signalNotFull();
        }
        return drained;
    }

    @Override
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public T element() {
        return null;
    }

    @Override
    public T peek() {
        return null;
    }

    @Override
    public T take() throws InterruptedException {
        if(!block_on_empty)
            throw new IllegalStateException(String.format("take() must not be called when block_on_empty=%b", block_on_empty));

         // retval == null so we have to block until the queue is not empty
         for(;;) {
             T retval=poll();
             if(retval != null)
                 return retval;
             lock.lockInterruptibly();
             try {
                 while(size.get() == 0)
                     not_empty.await();
             }
             catch(InterruptedException iex) {
                 Thread.currentThread().interrupt();
             }
             finally {
                 lock.unlock();
             }
         }
     }

    @Override
    public T remove() {
        T el=poll();
        if(el != null)
            return el;
        throw new NoSuchElementException("queue is empty");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        return new BufIterator();
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public int remainingCapacity() {
        return capacity - size.get();
    }

    @Override
    public String toString() {
        return String.format("ri=%d wi=%d size=%,d%s", ri, wi.get(), size.get(), isEmpty()? "" :
          " [" + Util.printListWithDelimiter(this::iterator, ", ", size()) + "]");
    }

    public List<T> contents() {
        List<T> l=new ArrayList<>(capacity);
        for(T el: this) {
            //noinspection UseBulkOperation
            l.add(el);
        }
        return l;
    }

    protected int advance(int idx) {
        return idx+1 >= capacity? 0 : idx+1;
    }

    protected int advance(int idx, int delta) {
        return idx+delta >= capacity? (idx+delta) % capacity : idx+delta;
    }

    protected void signalNotEmpty() {
        lock.lock();
        try {
            not_empty.signal(); // not signalAll() as there's always only 1 consumer
        }
        finally {
            lock.unlock();
        }
    }

    protected void signalNotFull() {
        lock.lock();
        try {
            not_full.signalAll(); // potentially multiple producers
        }
        finally {
            lock.unlock();
        }
    }

    protected class BufIterator implements Iterator<T> {
        int index=ri, consumed=0, to_consume=size.get();

        @Override
        public boolean hasNext() {
            return consumed < to_consume;
        }

        @Override
        public T next() {
            int idx=index;
            index=INCR_INDEX.applyAsInt(index);
            consumed++;
            if(idx < 0 || idx >= array.length())
                throw new NoSuchElementException(String.format("idx: %d", idx));
            return array.get(idx);
        }
    }

}

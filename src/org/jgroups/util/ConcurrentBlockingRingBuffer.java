package org.jgroups.util;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

/**
 * A ring buffer implementation which optionally blocks on adding or removing of elements
 * @author Bela Ban
 * @since  5.5.0
 */
public class ConcurrentBlockingRingBuffer<T> implements BlockingQueue<T> {
    protected final int              capacity;
    protected final Object[]         array;
    protected final AtomicInteger    wi=new AtomicInteger(), ri=new AtomicInteger();
    protected final AtomicInteger    size=new AtomicInteger(0);  // cannot exceed array.length
    protected final boolean          block_on_empty, block_on_full;
    protected final Lock             lock=new ReentrantLock();
    protected final Condition        not_empty=lock.newCondition();
    protected final Condition        not_full=lock.newCondition();
    protected final IntUnaryOperator INCR; // increments up to capacity, never exceeds capacity
    protected static final IntUnaryOperator DECR=x -> x == 0? x : x-1; // decrements, but never < 0
    protected final IntUnaryOperator INCR_INDEX;



    public ConcurrentBlockingRingBuffer(int capacity, boolean block_on_empty, boolean block_on_full) {
        this.array=new Object[this.capacity=capacity];
        this.block_on_empty=block_on_empty;
        this.block_on_full=block_on_full;
        if(capacity == 0)
            throw new IllegalArgumentException(String.format("Capacity (%d) needs to be positive", capacity));
        INCR=x -> x >= capacity? x : x+1;
        INCR_INDEX=x -> x == (capacity-1)? 0 : x+1;
    }

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
             array[index]=t;
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
        wi.set(0);
        ri.set(0);
        size.set(0);
        Arrays.fill(array, null);
     }

     @Override
     public boolean isEmpty() {
         return size.get() == 0;
     }

     @Override
     public T poll() {
         int index=ri.get();
         int old_size=size.getAndUpdate(DECR);
         if(old_size == 0)
             return null;
         T val=(T)array[index];
         ri.getAndUpdate(INCR_INDEX);
         if(block_on_full && old_size >= capacity)
             signalNotFull();
         return val;
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
     public T poll(long timeout, TimeUnit unit) throws InterruptedException {
         throw new UnsupportedOperationException();
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
             finally {
                 lock.unlock();
             }
         }
     }

     @Override
     public int drainTo(Collection<? super T> c) {
         return drainTo(c, Integer.MAX_VALUE);
     }

     @Override
     public int drainTo(Collection<? super T> c, final int max) {
         Objects.requireNonNull(c);
         if(c == this)
             throw new IllegalArgumentException();
         if(max <= 0)
             return 0;
         int drained=0;
         while(drained < max) {
             T el=poll();
             if(el == null)
                 return drained;
             c.add(el);
             drained++;
         }
         return drained;
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
        return null;
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

}

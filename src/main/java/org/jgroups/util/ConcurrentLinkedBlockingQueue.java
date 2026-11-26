package org.jgroups.util;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

/**
 * Concurrent queue which offers optional blocking on empty / full. Designed for multiple producers, but only
 * a single consumer. Only methods required by message bundlers (offer(), poll(), drainTo(), size() etc) are implemented.
 * Might add more implementation as needed.
 * <br/>
 * See https://issues.redhat.com/browse/JGRP-2890 for details
 * @param <T> T
 * @author Bela Ban
 * @since 5.5.0
 */
@SuppressWarnings("serial")
public class ConcurrentLinkedBlockingQueue<T> extends ConcurrentLinkedQueue<T> implements BlockingQueue<T> {
    protected final int              capacity;
    protected final AtomicInteger    size=new AtomicInteger(0);
    protected final boolean          block_on_empty, block_on_full;
    protected final Lock             lock=new ReentrantLock();
    protected final Condition        not_empty=lock.newCondition();
    protected final Condition        not_full=lock.newCondition();
    protected final IntUnaryOperator INCR; // increments up to capacity, never exceeds capacity


    public ConcurrentLinkedBlockingQueue(int capacity, boolean block_on_empty, boolean block_on_full) {
        this.capacity=capacity;
        this.block_on_empty=block_on_empty;
        this.block_on_full=block_on_full;
        if(capacity <= 0)
            throw new IllegalArgumentException(String.format("Capacity (%d) needs to be positive", capacity));
        INCR=x -> x >= capacity? x : x+1;
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
            boolean rc=super.offer(t);
            if(rc && old_size == 0 && block_on_empty)
                signalNotEmpty();
            return rc;
        }
        return false;
    }

    @Override
    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        super.clear();
        size.set(super.size());
    }

    @Override
    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public T poll() {
        T retval=super.poll();
        if(retval != null) {
            int old_size=size.getAndDecrement();
            if(block_on_full && old_size >= capacity)
                signalNotFull();
        }
        return retval;
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

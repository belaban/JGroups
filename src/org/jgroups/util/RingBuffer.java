package org.jgroups.util;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;


/**
 * Ring buffer of fixed capacity designed for multiple writers <em>but only a single reader</em>. Advancing the read or
 * write index blocks until it is possible to do so.
 * @author Bela Ban
 * @since  4.0
 */
public class RingBuffer<T> {
    protected final T[]          buf;
    protected int                ri, wi;   // read and write indices
    protected int                count;    // number of elements available to be read
    protected final Lock         lock=new ReentrantLock();
    protected final Condition    not_empty=lock.newCondition(); // reader can block on this
    protected final Condition    not_full=lock.newCondition();  // writes can block on this

    public RingBuffer(Class<T> element_type) {
        buf=(T[])Array.newInstance(element_type, 16);
    }

    public RingBuffer(Class<T> element_type, int capacity) {
        int c=Util.getNextHigherPowerOfTwo(capacity); // power of 2 for faster mod operation
        buf=(T[])Array.newInstance(element_type, c);
    }

    public T[] buf()               {return buf;}
    public int capacity()          {return buf.length;}
    public int readIndexLockless() {return ri;}
    public int countLockLockless() {return count;}

    public int readIndex() {
        lock.lock();
        try {
            return ri;
        }
        finally {
            lock.unlock();
        }
    }

    public int writeIndex() {
        lock.lock();
        try {
            return wi;
        }
        finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return count;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return ri == wi;
        }
        finally {
            lock.unlock();
        }
    }

    public RingBuffer<T> clear() {
        lock.lock();
        try {
            count=ri=wi=0;
            Arrays.fill(buf, null);
            return this;
        }
        finally {
            lock.unlock();
        }
    }




    /**
     * Tries to add a new element at the current write index and advances the write index. If the write index is at the
     * same position as the read index, this will block until the read index is advanced.
     * @param element the element to be added. Must not be null, or else this operation returns immediately
     */
    public RingBuffer<T> put(T element) throws InterruptedException {
        if(element == null)
            return this;
        lock.lock();
        try {
            while(count == buf.length)
                not_full.await();

            buf[wi]=element;
            if(++wi == buf.length)
                wi=0;
            count++;
            not_empty.signal();
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes the next available element, blocking until one is available (if needed).
     * @return The next available element
     */
    public T take() throws InterruptedException {
        lock.lock();
        try {
            while(count == 0)
                not_empty.await();
            T el=buf[ri];
            buf[ri]=null;
            if(++ri == buf.length)
                ri=0;
            count--;
            not_full.signal();
            return el;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes as many messages as possible and adds them to c.
     * Same semantics as {@link java.util.concurrent.BlockingQueue#drainTo(Collection)}.
     * @param c The collection to which to add the removed messages.
     * @return The number of messages removed
     * @throws NullPointerException If c is null
     */
    public int drainTo(Collection<? super T> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /**
     * Removes as many messages as possible and adds them to c. Contrary to {@link #drainTo(Collection)}, this method
     * blocks until at least one message is available, or the caller thread is interrupted.
     * @param c The collection to which to add the removed messages.
     * @return The number of messages removed
     * @throws NullPointerException If c is null
     */
    public int drainToBlocking(Collection<? super T> c) throws InterruptedException {
        return drainToBlocking(c, Integer.MAX_VALUE);
    }

    /**
     * Removes a number of messages and adds them to c.
     * Same semantics as {@link java.util.concurrent.BlockingQueue#drainTo(Collection,int)}.
     * @param c The collection to which to add the removed messages.
     * @param max_elements The max number of messages to remove. The actual number of messages removed may be smaller
     *                     if the buffer has fewer elements
     * @return The number of messages removed
     * @throws NullPointerException If c is null
     */
    public int drainTo(Collection<? super T> c, int max_elements) {
        int num=Math.min(count, max_elements); // count may increase in the mean time, but that's ok
        if(num == 0)
            return num;
        int read_index=ri; // no lock as we're the only reader
        for(int i=0; i < num; i++) {
            int real_index=realIndex(read_index +i);
            c.add(buf[real_index]);
            buf[real_index]=null;
        }
        publishReadIndex(num);
        return num;
    }

    /**
     * Removes a number of messages and adds them to c. Contrary to {@link #drainTo(Collection,int)}, this method
     * blocks until at least one message is available, or the caller thread is interrupted.
     * @param c The collection to which to add the removed messages.
     * @param max_elements The max number of messages to remove. The actual number of messages removed may be smaller
     *                     if the buffer has fewer elements
     * @return The number of messages removed
     * @throws NullPointerException If c is null
     */
    public int drainToBlocking(Collection<? super T> c, int max_elements) throws InterruptedException {
        lock.lockInterruptibly(); // fail fast
        try {
            while(count == 0)
                not_empty.await();
            return drainTo(c, max_elements);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes messages and adds them to c.
     * @param c The array to add messages to.
     * @return The number of messages removed and added to c. This is min(count, c.length). If no messages are present,
     * this method returns immediately
     */
    public int drainTo(T[] c) {
        int num=Math.min(count, c.length); // count may increase in the mean time, but that's ok
        if(num == 0)
            return num;
        int read_index=ri; // no lock as we're the only reader
        for(int i=0; i < num; i++) {
            int real_index=realIndex(read_index +i);
            c[i]=(buf[real_index]);
            buf[real_index]=null;
        }
        publishReadIndex(num);
        return num;
    }

    /**
     * Removes messages and adds them to c.
     * @param c The array to add messages to.
     * @return The number of messages removed and added to c. This is min(count, c.length). Contrary to
     * {@link #drainTo(Object[])}, this method blocks until at least one message is available or the caller thread
     * is interrupted.
     */
    public int drainToBlocking(T[] c) throws InterruptedException {
        lock.lockInterruptibly(); // fail fast
        try {
            while(count == 0)
                not_empty.await();
            return drainTo(c);
        }
        finally {
            lock.unlock();
        }
    }

    public RingBuffer<T> publishReadIndex(int num_elements_read) {
        // this.ri is only read/written by the consumer, and since there's only 1 consumer, there's no need to synchronize on it
        this.ri=realIndex(this.ri + num_elements_read);

        lock.lock();
        try {
            this.count-=num_elements_read;
            not_full.signalAll(); // wake up all writers
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /** Blocks until messages are available */
    public int waitForMessages() throws InterruptedException {
        return waitForMessages(40, null);
    }

    /**
     *  Blocks until messages are available
     *  @param num_spins the number of times we should spin before acquiring a lock
     *  @param wait_strategy the strategy used to spin. The first parameter is the iteration count and the second
     *                       parameter is the max number of spins
     */

    public int waitForMessages(int num_spins, final BiConsumer<Integer,Integer> wait_strategy) throws InterruptedException {
        // try spinning first (experimental)
        for(int i=0; i < num_spins && count == 0; i++) {
            if(wait_strategy != null)
                wait_strategy.accept(i, num_spins);
            else
                Thread.yield();
        }
        if(count == 0)
            _waitForMessages();
        return count; // whatever is the last count; could have been updated since lock release
    }

    public void _waitForMessages() throws InterruptedException {
        lock.lockInterruptibly(); // fail fast
        try {
            while(count == 0)
                not_empty.await();
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        return String.format("[ri=%d wi=%d size=%d cap=%d]", ri, wi, size(), buf.length);
    }


    /** Apparently much more efficient than mod (%) */
    protected int realIndex(int index) {
        return index & (buf.length -1);
    }

}

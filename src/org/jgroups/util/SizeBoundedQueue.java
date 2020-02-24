package org.jgroups.util;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Queue;


/**
 * Blocking FIFO queue bounded by the max number of bytes of all elements. When adding threads are blocked due to
 * capacity constraints, and the application terminates, it is the caller's duty to interrupt all threads.
 * @author Bela Ban
 * @since  4.0.4
 */
public class SizeBoundedQueue<T> {
    protected final Lock                   lock;
    protected final Condition              not_full, not_empty;
    protected final int                    max_size; // max accumulated number of bytes of all elements
    protected final Queue<El<T>>           queue=new ConcurrentLinkedQueue<>();
    protected int                          count;    // accumulated bytes
    protected int                          waiters;  // threads blocked on add() because the queue is full
    protected boolean                      done;     // when true, the queue cannot be used anymore


    public SizeBoundedQueue(int max_size) {
        this(max_size, new ReentrantLock(true));
    }

    public SizeBoundedQueue(int max_size, final Lock lock) {
        this.lock=lock;
        this.max_size=max_size;
        if(lock == null)
            throw new IllegalArgumentException("lock must not be null");
        not_full=lock.newCondition();
        not_empty=lock.newCondition();
    }


    public void add(T element, int size) throws InterruptedException {
        if(element == null)
            throw new IllegalArgumentException("element cannot be null");
        boolean incremented=false;
        lock.lockInterruptibly();
        try {
            while(!done && max_size - this.count - size < 0) {
                if(!incremented) {
                    incremented=true;
                    waiters++;
                }
                not_full.await(); // queue is full; we need to block
            }
            if(done)
                return;
            queue.add(new El<>(element, size));
            boolean signal=count == 0;
            this.count+=size;
            if(signal)
                not_empty.signalAll();
        }
        finally {
            if(incremented)
                waiters--;
            lock.unlock();
        }
    }

    /** Removes and returns the first element or null if the queue is empty */
    public T remove() {
        lock.lock();
        try {
            if(done || queue.isEmpty())
                return null;
            El<T> el=queue.poll();
            count-=el.size;
            not_full.signalAll();
            return el.el;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Removes and adds to collection c as many elements as possible (including waiters) but not exceeding max_bytes.
     * E.g. if we have elements {1000b, 2000b, 4000b} and max_bytes=6000, then only the first 2 elements are removed
     * and the new size is 4000
     * @param c The collection to transfer the removed elements to
     * @param max_bytes The max number of bytes to remove
     * @return The accumulated number of bytes of all removed elements
     */
    public int drainTo(Collection<T> c, final int max_bytes) {
        if(c == null)
            throw new IllegalArgumentException("collection to drain elements to must not be null");
        if(max_bytes <= 0)
            return 0;
        int bytes=0;
        El<T> el;
        boolean at_least_one_removed=false;
        lock.lock();
        try {
            // go as long as there are elements in the queue or pending waiters
            while(!done && ((el=queue.peek()) != null || waiters > 0)) {
                if(el != null) {
                    if(bytes + el.size > max_bytes)
                        break;
                    el=queue.poll();
                    at_least_one_removed=true;
                    count-=el.size;
                    bytes+=el.size;
                    c.add(el.el);
                }
                else { // queue is empty, wait on more elements to be added
                    not_full.signalAll(); // releases the waiters, causing them to add their elements to the queue
                    try {
                        not_empty.await();
                    }
                    catch(InterruptedException e) {
                        break;
                    }
                }
            }
            if(at_least_one_removed)
                not_full.signalAll();
            return bytes;
        }
        finally {
            lock.unlock();
        }
    }

    public void clear(boolean done) {
        lock.lock();
        try {
            this.done=done;
            queue.clear();
            count=0;
            not_full.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    /** Returns the number of elements in the queue */
    public int     getElements() {return queue.size();}

    /** Returns the accumulated size of all elements in the queue */
    public int     size()        {return count;}
    public boolean isEmpty()     {return count == 0;}
    public int     getWaiters()  {return waiters;}
    public boolean hasWaiters()  {return waiters > 0;}

    public boolean isDone() {
        lock.lock();
        try {
            return done;
        }
        finally {
            lock.unlock();
        }
    }

    /** For testing only - should always be the same as size() */
    public int queueSize() {
        return queue.stream().map(el -> el.size).reduce(0, Integer::sum);
    }

    public String toString() {
        return String.format("%d elements / %d bytes (%d waiters): %s", getElements(), size(), waiters, queue);
    }


    protected static class El<T> {
        protected final T   el;
        protected final int size;

        public El(T el, int size) {
            this.el=el;
            this.size=size;
        }

        public String toString() {
            return String.format("%s (%d bytes)", el, size);
        }
    }
}

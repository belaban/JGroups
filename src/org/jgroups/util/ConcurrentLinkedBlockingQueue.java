package org.jgroups.util;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;

/**
 * Attempt at writing a fast transfer queue, which is bounded. The take() method blocks until there is an element, but
 * the offer() method drops the element and returns if the queue is full (doesn't block). I thought this class would
 * be useful in ThreadPoolExecutor, as a replacement for LinkedBlockingQueue, but the perf didn't change. I'll keep it
 * for later reference ...
 * @author Bela Ban
 */
public class ConcurrentLinkedBlockingQueue<T> extends ConcurrentLinkedQueue<T> implements BlockingQueue<T> {
    private static final long serialVersionUID=-8884995454506956809L;

    private final int capacity;

    private final Lock lock=new ReentrantLock();
    private final Condition not_empty=lock.newCondition();

    private final AtomicInteger waiting_takers=new AtomicInteger(0);


    public ConcurrentLinkedBlockingQueue(int capacity) {
        this.capacity=capacity;
    }



    
    // The methods below are used by ThreadPoolExecutor ///////////

    /**
     * Drops elements if capacity has been reached. That's OK for the ThreadPoolExecutor as dropped messages
     * will get retransmitted
     * @param t
     * @return
     */
    public boolean offer(T t) {
        boolean retval=size() < capacity && super.offer(t);
        if(waiting_takers.get() > 0) {
            lock.lock();
            try {
                not_empty.signal();
            }
            finally {
                lock.unlock();
            }
        }

        return retval;
    }

    public T take() throws InterruptedException {
        T retval=null;
        for(;;) {
            retval=poll();
            if(retval != null)
                return retval;
            while(size() == 0) {
                waiting_takers.incrementAndGet();
                lock.lockInterruptibly();
                try {
                    not_empty.await();
                }
                finally {
                    lock.unlock();
                    waiting_takers.decrementAndGet();
                }
            }
        }
    }

    public T poll() {
        return super.poll();
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long sleep_time_nanos=TimeUnit.NANOSECONDS.convert(timeout, unit);
        long target_time_nanos=System.nanoTime() + sleep_time_nanos;
        sleep_time_nanos/=5;
        T el=null;

        while(System.nanoTime() < target_time_nanos) {
            if((el=poll()) != null)
                return el;
            LockSupport.parkNanos(sleep_time_nanos);
        }
        
        return el;
    }

    public boolean remove(Object o) {
        return super.remove(o);
    }

    public int remainingCapacity() {
        return capacity - size();
    }

    public int drainTo(Collection<? super T> c) {
        int count=0;
        if(c == null)
            return count;

        for(;;) {
            T el=poll();
            if(el == null)
                break;
            c.add(el);
            count++;
        }

        return count;
    }

    /////////////////////////////////////////////////////////////////


    
    public void put(T t) throws InterruptedException {
        super.offer(t);
    }

    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(t);
    }







    public int drainTo(Collection<? super T> c, int maxElements) {
        return drainTo(c);
    }
}

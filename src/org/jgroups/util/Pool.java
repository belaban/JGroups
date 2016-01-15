package org.jgroups.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages a fixed pool of resources (e.g. buffers). Uses the fast try-lock operation to get a resource from the pool,
 * or returns a newly created resource. When the returned lock is unlocks, that resource becomes available again
 * for consumption
 * @author Bela Ban
 * @since  3.5
 */
public class Pool<T> {
    protected final T[]        pool;
    protected final Lock[]     locks;
    protected final Creator<T> creator;

    public interface Creator<T> {
        T create();
    }

    @SuppressWarnings("unchecked")
    public Pool(int capacity, Creator<T> creator) {
        this.creator=creator;
        this.pool=(T[])new Object[Util.getNextHigherPowerOfTwo(capacity)];
        this.locks=new Lock[pool.length];
        for(int i=0; i < locks.length; i++)
            locks[i]=new ReentrantLock();
    }

    public T[] getElements() {return pool;}

    public int getNumLocked() {
        int retval=0;
        for(Lock lock: locks)
            if(((ReentrantLock)lock).isLocked())
                retval++;
        return retval;
    }


    /**
     * Gets the next available resource for which the lock can be acquired and returns it and its associated
     * lock, which needs to be released when the caller is done using the resource. If no resource in the pool can be
     * locked, returns a newly created resource and a null lock. This means that no lock was acquired and thus
     * doesn't need to be released.
     * @return An Element with T and a lock (possibly null if newly created)
     */
    public Element<T> get() {
        // start at a random index, so different threads don't all start at index 0 and compete for the lock
        int starting_index=((int)(Math.random() * pool.length)) & (pool.length - 1);
        for(int i=0; i < locks.length; i++) {
            int index=(starting_index + i) & (pool.length -1);
            Lock lock=locks[index];
            if(lock.tryLock()) {
                if(pool[index] != null)
                    return new Element<>(pool[index], lock);
                pool[index]=creator.create();
                return new Element<>(pool[index], lock);
            }
        }
        return new Element<>(creator.create(), null);
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        int locked=getNumLocked();
        sb.append("capacity=" + pool.length + ", locked=" + locked + ", available=" + (pool.length - locked));
        return sb.toString();
    }

    public class Element<T> {
        protected final T    element;
        protected final Lock lock;

        public Element(T element, Lock lock) {
            this.element=element;
            this.lock=lock;
        }

        public T      getElement() {return element;}
        public Lock   getLock()    {return lock;}
        public String toString()   {return element + ", " + lock;}
    }



    public static void main(String[] args) {
        final int length=8;
        final Map<Integer,Integer> map=new HashMap<>(length);
        for(int i=0; i < 100; i++) {
            int index=((int)(Math.random() * length)) & (length - 1);
            System.out.println("index = " + index);
            Integer count=map.get(index);
            if(count == null)
                map.put(index, 1);
            else
                map.put(index, ++count);
        }
        System.out.println("map = " + map);
    }

}

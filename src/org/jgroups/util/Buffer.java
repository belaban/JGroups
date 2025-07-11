package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for message buffers. Used on the senders (keeping track of sent messages and purging
 * delivered messages) and receivers (delivering messages in the correct order and asking senders for missing messages).
 * @author Bela Ban
 * @since  5.4
 */
public abstract class Buffer<T> implements Iterable<T>, Closeable {
    protected final Lock          lock=new ReentrantLock();
    protected final AtomicInteger adders=new AtomicInteger(0);
    protected long                offset;

    /** sender: highest seqno seen by everyone, receiver: highest delivered seqno */
    protected long                low;

    /** The highest delivered (=removed) seqno */
    protected long                hd;

    /** The highest received/sent seqno. Moved forward by add(). The next message to be added is high+1.
     low <= hd <= high always holds */
    protected long                high;

    /** The number of non-null elements */
    protected int                 size;


    public Lock          lock()               {return lock;}
    public AtomicInteger getAdders()          {return adders;}
    public long          offset()             {return offset;}
    public long          low()                {return low;}
    public long          highestDelivered()   {return hd;}
    public long          hd()                 {return hd;}
    public long          high()               {return high;}
    public int           size()               {return size;}
    public boolean       isEmpty()            {return size <= 0;}


    /** Returns the current capacity in the buffer. This value is fixed in a fixed-size buffer
     * (e.g. {@link FixedBuffer}), but can change in a dynamic buffer ({@link DynamicBuffer}) */
    public abstract int  capacity();
    public void          resetStats() {}
    public void          open(boolean b) {}
    @Override
    public void          close() {open(false);}

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     * @param seqno
     * @param element
     * @return True if the element at the computed index was null, else false
     */
    // used: single message received
    public boolean add(long seqno, T element) {
        return add(seqno, element, null, true);
    }

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     *
     * @param seqno         The seqno of the element
     * @param element       The element to be added
     * @param remove_filter A filter used to remove all consecutive messages passing the filter (and non-null). This
     *                      doesn't necessarily null a removed message, but may simply advance an index
     *                      (e.g. highest delivered). Ignored if null.
     * @param block_if_full If true: blocks when an element is to be added to the buffer, else drops the element
     * @return True if the element at the computed index was null, else false
     */
    // used: send / receive message
    public abstract boolean add(long seqno, T element, Predicate<T> remove_filter, boolean block_if_full);

    // used: MessageBatch received
    public abstract boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value);

    /**
     * Adds elements from the list
     * @param list The list of tuples of seqnos and elements. If remove_added_elements is true, if elements could
     *             not be added (e.g. because they were already present or the seqno was < HD), those
     *             elements will be removed from list
     * @param remove_added_elements If true, elements that could not be added to the table are removed from list
     * @param const_value If non-null, this value should be used rather than the values of the list tuples
     * @return True if at least 1 element was added successfully, false otherwise.
     */
    // used: MessageBatch received by UNICAST3/4
    public abstract boolean add(final List<LongTuple<T>> list, boolean remove_added_elements, T const_value);

    // used: retransmision etc
    public abstract T get(long seqno);

    public abstract T _get(long seqno);

    public T remove() {
        return remove(true);
    }

    public abstract T remove(boolean nullify);

    public List<T> removeMany(boolean nullify, int max_results) {
        return removeMany(nullify, max_results, null);
    }

    public abstract List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter);

    // used in removeAndDeliver()
    public abstract <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter,
                                     Supplier<R> result_creator, BiConsumer<R,T> accumulator);

    /** Removes all elements <= seqno from the buffer. Does this by nulling all elements < index(seqno) */
    public int purge(long seqno) {
        return purge(seqno, false);
    }

    /**
     * Purges (nulls) all elements <= seqno.
     * @param seqno All elements <= seqno will be purged.
     * @param force If false, seqno is max(seqno,hd), else max(seqno,high). In the latter case (seqno > hd), we might
     *              purge elements that have not yet been received
     * @return 0. The number of purged elements
     */
    public abstract int purge(long seqno, boolean force);

    public void forEach(Visitor<T> visitor, boolean nullify) {
        forEach(highestDelivered()+1, high(), visitor, nullify);
    }

    public abstract void forEach(long from, long to, Visitor<T> visitor, boolean nullify);

    public abstract Iterator<T> iterator(long from, long to);

    // used
    public abstract Stream<T> stream();

    public abstract Stream<T> stream(long from, long to);

    /** Iterates from hd to high and adds up non-null values. Caller must hold the lock. */
    @GuardedBy("lock")
    public int computeSize() {
        return (int)stream().filter(Objects::nonNull).count();
    }

    /** Returns the number of null elements in the range [hd+1 .. hr-1] excluding hd and hr */
    public int numMissing() {
        lock.lock();
        try {
            return (int)(high - hd - size);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns a list of missing (= null) elements
     * @return A SeqnoList of missing messages, or null if no messages are missing
     */
    public SeqnoList getMissing() {
        return getMissing(0);
    }

    /**
     * Returns a list of missing messages
     * @param max_msgs If > 0, the max number of missing messages to be returned (oldest first), else no limit
     * @return A SeqnoList of missing messages, or null if no messages are missing
     */
    public SeqnoList getMissing(int max_msgs) {
        lock.lock();
        try {
            if(isEmpty())
                return null;
            long start_seqno=getHighestDeliverable() + 1;
            int capacity=(int)(high - start_seqno);
            int max_size=max_msgs > 0? Math.min(max_msgs, capacity) : capacity;
            if(max_size <= 0)
                return null;
            Missing missing=new Missing(start_seqno, max_size);
            long to=max_size > 0? Math.min(start_seqno + max_size - 1, high - 1) : high - 1;
            forEach(start_seqno, to, missing, false);
            return missing.getMissingElements();
        }
        finally {
            lock.unlock();
        }
    }

    /** Returns the number of messages that can be delivered */
    public int getNumDeliverable() {
        NumDeliverable visitor=new NumDeliverable();
        lock.lock();
        try {
            forEach(visitor, false);
            return visitor.getResult();
        }
        finally {
            lock.unlock();
        }
    }

    /** Returns the highest deliverable (= removable) seqno. This may be higher than {@link #highestDelivered()},
     * e.g. if elements have been added but not yet removed */
    // used in retransmissions
    public long getHighestDeliverable() {
        HighestDeliverable visitor=new HighestDeliverable();
        lock.lock();
        try {
            forEach(visitor, false);
            long retval=visitor.getResult();
            return retval == -1? highestDelivered() : retval;
        }
        finally {
            lock.unlock();
        }
    }

    public long[] getDigest() {
        lock.lock();
        try {
            return new long[]{hd, high};
        }
        finally {
            lock.unlock();
        }
    }

    /** Only used internally on a state transfer (setting the digest). Don't use this in application code! */
    public Buffer<T> highestDelivered(long seqno) {
        lock.lock();
        try {
            hd=seqno;
            return this;
        }
        finally {
            lock.unlock();
        }
    }

    /** Dumps all non-null messages (used for testing) */
    public String dump() {
        lock.lock();
        try {
            return stream(low(), high()).filter(Objects::nonNull).map(Object::toString)
              .collect(Collectors.joining(", "));
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return String.format("[%,d | %,d | %,d] (size: %,d, missing: %,d)", low, hd, high, size, numMissing());
    }

    public interface Visitor<T> {
        /**
         * Iteration over the table, used by {@link DynamicBuffer#forEach(long, long, Buffer.Visitor, boolean)}.
         *
         * @param seqno   The current seqno
         * @param element The element at matrix[row][column]
         * @return True if we should continue the iteration, false if we should break out of the iteration
         */
        boolean visit(long seqno, T element);
    }

    protected class Remover<R> implements Visitor<T> {
        protected final int          max_results;
        protected int                num_results;
        protected final Predicate<T> filter;
        protected R                  result;
        protected Supplier<R>        result_creator;
        protected BiConsumer<R,T>    result_accumulator;

        public Remover(int max_results, Predicate<T> filter, Supplier<R> creator, BiConsumer<R,T> accumulator) {
            this.max_results=max_results;
            this.filter=filter;
            this.result_creator=creator;
            this.result_accumulator=accumulator;
        }

        public R getResult() {return result;}

        @GuardedBy("lock")
        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            if(filter == null || filter.test(element)) {
                if(result == null)
                    result=result_creator.get();
                result_accumulator.accept(result, element);
                num_results++;
            }
            size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
            if(seqno - hd > 0)
                hd=seqno;
            return max_results == 0 || num_results < max_results;
        }
    }


    protected class NumDeliverable implements Visitor<T> {
        protected int num_deliverable=0;

        public int getResult() {return num_deliverable;}

        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            num_deliverable++;
            return true;
        }
    }

    protected class HighestDeliverable implements Visitor<T> {
        protected long highest_deliverable=-1;

        public long getResult() {return highest_deliverable;}

        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            highest_deliverable=seqno;
            return true;
        }
    }

    protected class Missing implements Visitor<T> {
        protected final SeqnoList missing_elements;
        protected final int       max_num_msgs;
        protected int             num_msgs;

        protected Missing(long start, int max_number_of_msgs) {
            missing_elements=new SeqnoList(max_number_of_msgs, start);
            this.max_num_msgs=max_number_of_msgs;
        }

        protected SeqnoList getMissingElements() {return missing_elements;}

        public boolean visit(long seqno, T element) {
            if(element == null) {
                if(++num_msgs > max_num_msgs)
                    return false;
                missing_elements.add(seqno);
            }
            return true;
        }
    }
}

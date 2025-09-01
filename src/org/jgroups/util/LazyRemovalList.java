package org.jgroups.util;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * List which doesn't remove elements on remove(), removeAll() or retainAll(), but only removes elements when a
 * configurable size limit has been exceeded. In that case, all elements marked as removable and older than a
 * configurable time are evicted. Elements are marked as removable by remove(), removeAll() and retainAll(). When
 * an elements is marked as removable, but later reinserted, the mark is removed.<br/>
 * Note that methods get() and remove() iterate through the list (linear cost), so large lists will be slow.
 * @param <T> T
 * @author Bela Ban
 * @since 5.4.4
 */
public class LazyRemovalList<T> {
    private final Queue<Entry<T>> list=new ConcurrentLinkedQueue<>();

    /** Max number of elements, if exceeded, we remove all elements marked as removable and older than max_age ms */
    private final int  max_elements;
    private final long max_age; // ns


    public interface Printable<T> {
        String print(Entry<T> e);
    }


    public LazyRemovalList() {
        this(200, 5000L);
    }

    /**
     * Creates a new instance
     * @param max_elements The max number of elements in the cache
     * @param max_age The max age (in ms) an entry can have before it is considered expired (and can be removed on
     *                the next sweep)
     */
    public LazyRemovalList(int max_elements, long max_age) {
        this.max_elements=max_elements;
        this.max_age=TimeUnit.NANOSECONDS.convert(max_age, TimeUnit.MILLISECONDS);
    }

    public boolean add(T val) {
        boolean rc=list.add(new Entry<>(Objects.requireNonNull(val)));
        if(rc)
            checkMaxSizeExceeded();
        return rc;
    }

    public T get(T val) {
        if(val == null)
            return null;
        return list.stream().map(e -> e.val).filter(v -> v.equals(val)).findFirst().orElse(null);
    }

    public void forEach(Consumer<T> c) {
        if(c == null)
            return;
        for(Entry<T> e: list) {
            try {
                if(!e.removable)
                    c.accept(e.val);
            }
            catch(Throwable t) {
            }
        }
    }

    public void remove(T val) {
        if(val == null)
            return;
        for(Iterator<Entry<T>> it=list.iterator(); it.hasNext();) {
            Entry<T> e=it.next();
            if(e.val.equals(val)) {
                e.removable(true);
                break;
            }
        }
    }

    public void clear(boolean force) {
        if(force)
            list.clear();
        else
            list.forEach(e -> e.removable(true));
    }

    /**
     * Adds all value which have not been marked as removable to the returned set
     * @return
     */
    public List<T> nonRemovedValues() {
        return list.stream().filter(e -> !e.removable).map(e -> e.val).collect(Collectors.toList());
    }

    public int size() {
        return list.size();
    }

    public String printCache() {
        return list.stream().map(Entry::toString).collect(Collectors.joining("\n"));
    }

    public String printCache(Printable<T> print_function) {
        return list.stream().map(print_function::print).collect(Collectors.joining("\n"));
    }

    public String toString() {
        return printCache();
    }

    private void checkMaxSizeExceeded() {
        if(list.size() > max_elements)
            removeMarkedElements(false);
    }

    /**
     * Removes elements marked as removable
     * @param force If set to true, all elements marked as 'removable' will get removed, regardless of expiration
     */
    public void removeMarkedElements(boolean force) {
        long curr_time=System.nanoTime();
        list.removeIf(e -> e.removable && (force || (curr_time - e.timestamp) >= max_age));
    }

    /** Removes elements marked as removable */
    public void removeMarkedElements() {
        removeMarkedElements(false);
    }


    public static class Entry<T> {
        protected final T    val;
        protected long       timestamp=System.nanoTime();
        protected boolean    removable=false;

        public Entry(T val) {
            this.val=val;
        }

        public boolean removable() {
            return removable;
        }
        public Entry<T> removable(boolean flag) {
            if(this.removable != flag) {
                this.removable=flag;
                timestamp=System.nanoTime();
            }
            return this;
        }

        public T val() {
            return val;
        }

        public String toString() {
            return toString(null);
        }

        public String toString(Function<T,String> print_val) {
            StringBuilder sb=new StringBuilder(print_val != null? print_val.apply(val) : val.toString()).append(" (");
            long age=TimeUnit.MILLISECONDS.convert(System.nanoTime() - timestamp, TimeUnit.NANOSECONDS);
            if(age < 1000)
                sb.append(age).append(" ms");
            else
                sb.append(TimeUnit.SECONDS.convert(age, TimeUnit.MILLISECONDS)).append(" secs");
            sb.append(" old").append((removable? ", removable" : "")).append(")");
            return sb.toString();
        }

    }
}

package org.jgroups.blocks;

import java.util.*;

/**
 * Hash set which doesn't remove elements on remove(), removeAll() or retainAll(), but only removes elements when a
 * configurable size limit has been exceeded. In that case, all elements marked as removable and older than a
 * configurable time are evicted. Elements are marked as removable by remove(), removeAll() and retainAll(). When
 * an elements is marked as removable, but later reinserted, the mark is removed.
 * @author Bela Ban
 */
public class LazyRemovalSet<V> {
    private final Set<Entry<V>> set=new HashSet<Entry<V>>();

    /** Max number of elements, if exceeded, we remove all elements marked as removable and older than max_age ms */
    private final int max_elements;

    private final long max_age;


    public interface Printable<V> {
        String print(V val);
    }


    public LazyRemovalSet() {
        this(200, 5000L);
    }

    public LazyRemovalSet(int max_elements, long max_age) {
        this.max_elements=max_elements;
        this.max_age=max_age;
    }

    public void add(V val) {
        if(val != null) {
            final Entry<V> entry=new Entry<V>(val);
            if(set.contains(entry))
                set.remove(entry);
            set.add(entry); // overwrite existing element (new timestamp, and possible removable mark erased)
        }
        checkMaxSizeExceeded();
    }

    public void add(V ... vals) {
        add(Arrays.asList(vals));
    }

    public void add(Collection<V> vals) {
        if(vals == null || vals.isEmpty())
            return;
        for(V val: vals) {
            Entry<V> entry=find(val);
            if(entry != null)
                set.remove(entry);
            set.add(new Entry<V>(val)); // overwrite existing element (new timestamp, and possible removable mark erased)
        }
        checkMaxSizeExceeded();
    }

    public boolean contains(V val) {
        if(val == null)
            return false;
        Entry<V> entry=new Entry<V>(val);
        return set.contains(entry);
    }

    

    public void remove(V val) {
        remove(val, false);
    }

    public void remove(V val, boolean force) {
        if(val == null)
            return;
        Entry<V> entry=new Entry<V>(val);
        if(force)
            set.remove(entry);
        else {
            entry=find(val);
            if(entry != null)
                entry.removable=true;
        }
        checkMaxSizeExceeded();
    }

    public void removeAll(Collection<V> values) {
        removeAll(values, false);
    }

    public void removeAll(Collection<V> values, boolean force) {
        if(values == null || values.isEmpty())
            return;
        if(force)
            set.removeAll(values);
        else {
            for(Entry<V> entry: set) {
                if(values.contains(entry.val))
                    entry.removable=true;
            }
        }
        checkMaxSizeExceeded();
    }

    public void clear(boolean force) {
        if(force)
            set.clear();
        else {
            for(Entry<V> entry: set) {
                if(entry.val != null)
                    entry.removable=true;
            }
        }
    }

    public void retainAll(Collection<V> values) {
        retainAll(values, false);
    }

    public void retainAll(Collection<V> values, boolean force) {
        if(values == null || values.isEmpty())
            return;

        if(force)
            set.retainAll(values);
        else {
            for(Entry<V> entry: set) {
                if(!values.contains(entry.val))
                    entry.removable=true;
            }
        }

        // now make sure that all elements in keys have removable=false
        for(V val: values) {
            Entry<V> entry=find(val);
            if(entry != null)
                entry.removable=false;
        }

        checkMaxSizeExceeded();
    }

    public Set<V> values() {
        Set<V> retval=new HashSet<V>();
        for(Entry<V> entry: set)
            retval.add(entry.val);
        return retval;
    }

    /**
     * Adds all value which have not been marked as removable to the returned set
     * @return
     */
    public Set<V> nonRemovedValues() {
        Set<V> retval=new HashSet<V>();
        for(Entry<V> entry: set) {
            if(!entry.removable)
                retval.add(entry.val);
        }
        return retval;
    }


    public int size() {
        return set.size();
    }

    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Entry<V> entry: set) {
            sb.append(entry).append("\n");
        }
        return sb.toString();
    }

    public String printCache(Printable<V> print_function) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(Entry<V> entry: set) {
            V val=entry.val;
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(print_function.print(val));
        }
        return sb.toString();
    }

    public String toString() {
        return printCache();
    }


    private void checkMaxSizeExceeded() {
        if(set.size() > max_elements) {
            removeMarkedElements();
        }
    }

    protected Entry<V> find(V val) {
        if(val == null)
            return null;
        for(Entry<V> entry: set) {
            if(val.equals(entry.val))
                return entry;
        }
        return null;
    }

    /**
     * Removes elements marked as removable
     * @param force If set to true, all elements marked as 'removable' will get removed, regardless of expiration
     */
    public void removeMarkedElements(boolean force) {
        long curr_time=System.currentTimeMillis();
        for(Iterator<Entry<V>> it=set.iterator(); it.hasNext();) {
            Entry<V> entry=it.next();
            if(entry == null)
                continue;
            if(entry.removable && (curr_time - entry.timestamp) >= max_age || force) {
                it.remove();
            }
        }
    }

    /**
     * Removes elements marked as removable
     */
    public void removeMarkedElements() {
        removeMarkedElements(false);
    }


    protected static class Entry<V> {
        protected final V    val;
        protected final long timestamp=System.currentTimeMillis();
        protected boolean    removable=false;

        public Entry(V val) {
            this.val=val;
        }

        public boolean equals(Object obj) {
            if(obj instanceof Entry) {
                return val.equals(((Entry)obj).val);
            }
            return val.equals(obj);
        }

        public int hashCode() {
            return val.hashCode();
        }

        public String toString() {
            return val + " (" + (System.currentTimeMillis() - timestamp) + "ms old" + (removable? ", removable" : "") + ")";
        }
    }
}

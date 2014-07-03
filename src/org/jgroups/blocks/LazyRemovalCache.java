package org.jgroups.blocks;

import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache which doesn't remove elements on remove(), removeAll() or retainAll(), but only removes elements when a
 * configurable size limit has been exceeded. In that case, all elements marked as removable and older than a
 * configurable time are evicted. Elements are marked as removable by remove(), removeAll() and retainAll(). When
 * an elements is marked as removable, but later reinserted, the mark is removed.
 * @author Bela Ban
 */
public class LazyRemovalCache<K,V> {
    private final ConcurrentMap<K, Entry<V>> map=Util.createConcurrentMap();

    /** Max number of elements, if exceeded, we remove all elements marked as removable and older than max_age ms */
    private final int max_elements;

    private final long max_age;


    public interface Printable<K,V> {
        String print(K key,V val);
    }


    public LazyRemovalCache() {
        this(200, 5000L);
    }

    public LazyRemovalCache(int max_elements, long max_age) {
        this.max_elements=max_elements;
        this.max_age=max_age;
    }

    public void add(K key, V val) {
        if(key != null && val != null)
            map.put(key, new Entry<V>(val)); // overwrite existing element (new timestamp, and possible removable mark erased)
        checkMaxSizeExceeded();
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    /** Returns true if all of the keys in keys are present. Returns false if one or more of the keys are absent */
    public boolean containsKeys(Collection<K> keys) {
        for(K key: keys)
            if(!map.containsKey(key))
                return false;
        return true;
    }


    public V get(K key) {
        if(key == null)
            return null;
        Entry<V> entry=map.get(key);
        return entry != null? entry.val : null;
    }

    public K getByValue(V val) {
        if(val == null) return null;
        for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
            Entry<V> v=entry.getValue();
            if(v.val != null && v.val.equals(val))
                return entry.getKey();
        }
        return null;
    }

    public void remove(K key) {
        remove(key, false);
    }

    public void remove(K key, boolean force) {
        if(key == null)
            return;
        if(force)
            map.remove(key);
        else {
            Entry<V> entry=map.get(key);
            if(entry != null)
                entry.setRemovable(true);
        }
        checkMaxSizeExceeded();
    }

    public void removeAll(Collection<K> keys) {
        removeAll(keys, false);
    }

    public void removeAll(Collection<K> keys, boolean force) {
        if(keys == null || keys.isEmpty())
            return;
        if(force)
            map.keySet().removeAll(keys);
        else {
            for(K key: keys) {
                Entry<V> entry=map.get(key);
                if(entry != null)
                    entry.setRemovable(true);
            }
        }
        checkMaxSizeExceeded();
    }

    public void clear(boolean force) {
        if(force)
            map.clear();
        else {
            for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
                Entry<V> val=entry.getValue();
                if(val != null) {
                    Entry<V> tmp=entry.getValue();
                    if(tmp != null)
                        tmp.setRemovable(true);
                }
            }
        }
    }

    public void retainAll(Collection<K> keys) {
        retainAll(keys, false);
    }

    public void retainAll(Collection<K> keys, boolean force) {
        if(keys == null || keys.isEmpty())
            return;
        if(force)
            map.keySet().retainAll(keys);
        else {
            for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
                if(!keys.contains(entry.getKey())) {
                    Entry<V> val=entry.getValue();
                    if(val != null)
                        val.setRemovable(true);
                }
            }
        }

        // now make sure that all elements in keys have removable=false
        for(K key: keys) {
            Entry<V> val=map.get(key);
            if(val != null && val.removable)
                val.setRemovable(false);
        }

        checkMaxSizeExceeded();
    }

    public Set<V> values() {
        Set<V> retval=new HashSet<V>();
        for(Entry<V> entry: map.values()) {
            retval.add(entry.val);
        }
        return retval;
    }

    public Iterable<Entry<V>> valuesIterator() {
        return new Iterable<Entry<V>>() {
            public Iterator<Entry<V>> iterator() {
                return map.values().iterator();
            }
        };
    }

    /**
     * Adds all value which have not been marked as removable to the returned set
     * @return
     */
    public Set<V> nonRemovedValues() {
        Set<V> retval=new HashSet<V>();
        for(Entry<V> entry: map.values()) {
            if(!entry.removable)
                retval.add(entry.val);
        }
        return retval;
    }

    public Map<K,V> contents() {
        Map<K,V> retval=new HashMap<K,V>();
        for(Map.Entry<K,Entry<V>> entry: map.entrySet())
            retval.put(entry.getKey(), entry.getValue().val);
        return retval;
    }

    public int size() {
        return map.size();
    }

    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    public String printCache(Printable print_function) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
            K key=entry.getKey();
            V val=entry.getValue().val;
            sb.append(print_function.print(key, val));
        }
        return sb.toString();
    }

    public String toString() {
        return printCache();
    }


    private void checkMaxSizeExceeded() {
        if(map.size() > max_elements) {
            removeMarkedElements();
        }
    }

    /**
     * Removes elements marked as removable
     * @param force If set to true, all elements marked as 'removable' will get removed, regardless of expiration
     */
    public void removeMarkedElements(boolean force) {
        long curr_time=System.currentTimeMillis();
        for(Iterator<Map.Entry<K,Entry<V>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, Entry<V>> entry=it.next();
            Entry<V> tmp=entry.getValue();
            if(tmp == null)
                continue;
            if(tmp.removable && ((curr_time - tmp.timestamp) >= max_age || force)) {
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


    public static class Entry<V> {
        protected final V    val;
        protected long       timestamp=System.currentTimeMillis();
        protected boolean    removable=false;

        public Entry(V val) {
            this.val=val;
        }

        public boolean isRemovable() {
            return removable;
        }

        public void setRemovable(boolean flag) {
            if(this.removable != flag) {
                this.removable=flag;
                timestamp=System.currentTimeMillis();
            }
        }

        public long getTimestamp() {
            return timestamp;
        }

        public V getVal() {
            return val;
        }

        public String toString() {
            return val + " (" + (System.currentTimeMillis() - timestamp) + "ms old" + (removable? ", removable" : "") + ")";
        }
    }
}

package org.jgroups.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded linked hashmap; used by SEQUENCER
 * @author Bela Ban
 * @since  3.3
 */
public class BoundedHashMap<K,V> extends LinkedHashMap<K,V> {
    private static final long serialVersionUID=-5368387761328082187L;
    protected final int max_size;

    public BoundedHashMap(int max_size) {
        this.max_size=max_size;
    }

    protected boolean removeEldestEntry(Map.Entry<K,V> eldest) {
        return size() > max_size;
    }

    public boolean add(K key, V val) {
        return super.put(key, val) == null;
    }
}

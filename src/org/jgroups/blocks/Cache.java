package org.jgroups.blocks;

import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Simple cache which maintains keys and value. A reaper can be enabled which periodically evicts expired entries.
 * Also, when the cache is configured to be bounded, entries in excess of the max size will be evicted on put().
 * @author Bela Ban
 */
public class Cache<K,V> {
    private static final Log log=LogFactory.getLog(Cache.class);
    private final ConcurrentMap<K,Value<V>> map=Util.createConcurrentMap();
    private ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1);
    private Future task=null;
    private final AtomicBoolean is_reaping=new AtomicBoolean(false);

    private final Set<ChangeListener> change_listeners=new LinkedHashSet<>();

    /** The maximum number of keys, When this value is exceeded we evict older entries, until we drop below this 
     * mark again. This effectively maintains a bounded cache. A value of 0 means don't bound the cache.
     */
    @ManagedAttribute(writable=true)
    private int max_num_entries=0;

    public int getMaxNumberOfEntries() {
        return max_num_entries;
    }

    public void setMaxNumberOfEntries(int max_num_entries) {
        this.max_num_entries=max_num_entries;
    }

    public void addChangeListener(ChangeListener l) {
        change_listeners.add(l);
    }

    public void removeChangeListener(ChangeListener l) {
        change_listeners.remove(l);
    }

    @ManagedAttribute
    public int getSize() {
        return map.size();
    }

    @ManagedAttribute
    public boolean isReapingEnabled() {
        return task != null && !task.isCancelled();
    }

    /** Runs the reaper every interval ms, evicts expired items */
    @ManagedOperation
    public void enableReaping(long interval) {
        if(task != null)
            task.cancel(false);
        task=timer.scheduleWithFixedDelay(new Reaper(), 0, interval, TimeUnit.MILLISECONDS);
    }

    @ManagedOperation
    public void disableReaping() {
        if(task != null) {
            task.cancel(false);
            task=null;
        }
    }

    @ManagedOperation
    public void start() {
        if(timer == null)
            timer=new ScheduledThreadPoolExecutor(1);
    }

    @ManagedOperation
    public void stop() {
        if(timer != null)
            timer.shutdown();
        timer=null;
    }

    /**
     *
     * @param key
     * @param val
     * @param caching_time Number of milliseconds to keep an entry in the cache. -1 means don't cache (if reaping
     * is enabled, we'll evict an entry with -1 caching time), 0 means never evict. In the latter case, we can still
     * evict an entry with 0 caching time: when we have a bounded cache, we evict in order of insertion no matter
     * what the caching time is.
     */
    @ManagedOperation
    public V put(K key, V val, long caching_time) {
        if(log.isTraceEnabled())
            log.trace("put(" + key + ", " + val + ", " + caching_time + ")");
        Value<V> value=new Value<>(val, caching_time);
        Value<V> retval=map.put(key, value);

        if(max_num_entries > 0 && map.size() > max_num_entries) {
            boolean rc=is_reaping.compareAndSet(false, true);
            if(rc) {
                if(log.isTraceEnabled())
                    log.trace("reaping: max_num_entries=" + max_num_entries + ", size=" + map.size());
                timer.execute(() -> {
                    if(max_num_entries > 0) {
                        try {
                            if(map.size() > max_num_entries) {
                                evict(); // see if we can gracefully evict expired items
                            }
                            if(map.size() > max_num_entries) {
                                // still too many entries: now evict entries based on insertion time: oldest first
                                int diff=map.size() - max_num_entries; // we have to evict diff entries
                                SortedMap<Long,K> tmp=new TreeMap<>();
                                for(Map.Entry<K,Value<V>> entry: map.entrySet()) {
                                    tmp.put(entry.getValue().insertion_time, entry.getKey());
                                }

                                Collection<K> vals=tmp.values();
                                for(K k: vals) {
                                    if(diff-- > 0) {
                                        Value<V> v=map.remove(k);
                                        if(log.isTraceEnabled())
                                            log.trace("evicting " + k + ": " + v.value);
                                    }
                                    else
                                        break;
                                }
                            }
                            if(log.isTraceEnabled())
                                log.trace("done reaping (size=" + map.size() + ")");
                        }
                        finally {
                            is_reaping.set(false);
                        }
                    }
                });
            }
        }

        return getValue(retval);
    }

    @ManagedOperation
    public V get(K key) {
        if(log.isTraceEnabled())
            log.trace("get(" + key + ")");
        Value<V> val=map.get(key);

        if (isExpired(val)) {
            map.remove(key);
            return null;
        }

        return getValue(val);
    }

    /**
     * This method should not be used to add or remove elements ! It was just added because ReplCacheDemo
     * requires it for its data model
     * @return
     */
    public ConcurrentMap<K, Value<V>> getInternalMap() {
        return map;
    }

    public Value<V> getEntry(K key) {
        if(log.isTraceEnabled())
            log.trace("getEntry(" + key + ")");
        return map.get(key);
    }

    public V remove(K key) {
        if(log.isTraceEnabled())
            log.trace("remove(" + key + ")");
        return getValue(map.remove(key));
    }

    public Set<Map.Entry<K,Value<V>>> entrySet() {
        return map.entrySet();
    }

    @ManagedOperation
    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Value<V>> entry: map.entrySet()) {
            Value<V> val=entry.getValue();
            sb.append(entry.getKey()).append(": ").append(entry.getValue().getValue());
            sb.append(" (expiration_time: ");
            long expiration_time=val.getTimeout();
            if(expiration_time <= 0)
                sb.append(expiration_time);
            else {
                sb.append(new Date(expiration_time));
            }
            sb.append(")\n");
        }
        return sb.toString();
    }

    public String dump() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Value<V>> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ");
            V val = getValue(entry.getValue());
            if(val != null) {
                if(val instanceof byte[])
                    sb.append(" (" + ((byte[])val).length).append(" bytes)");
                else
                    sb.append(val);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private void evict() {
        boolean evicted=false;
        for(Iterator<Map.Entry<K,Value<V>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K,Value<V>> entry=it.next();
            Value<V> val=entry.getValue();
            evicted = isExpired(val);
            if (evicted) {
                if(log.isTraceEnabled())
                    log.trace("evicting " + entry.getKey() + ": " + getValue(val));
                it.remove();
            }
        }
        if(evicted)
            notifyChangeListeners();
    }

    private void notifyChangeListeners() {
        for(ChangeListener l: change_listeners) {
            try {
                l.changed();
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedNotifyingChangeListener"), t);
            }
        }
    }

    private V getValue(Value<V> val) {
        return val == null ? null : val.getValue();
    }

    private boolean isExpired(Value<V> val) {
        return val != null &&
          (val.timeout == -1 || (val.timeout > 0 && System.currentTimeMillis() > val.insertion_time + val.timeout));
    }

    public static class Value<V> implements Externalizable {
        private V value;

        private long insertion_time=System.currentTimeMillis();
        
        /** When the value can be reaped (in ms) */
        private transient long timeout;
        private static final long serialVersionUID=-3445944261826378608L;


        public Value(V value, long timeout) {
            this.value=value;
            this.timeout=timeout;
        }

        public Value() {
        }

        public V getValue() {return value;}
        public long getInsertionTime() {return insertion_time;}
        public long getTimeout() {return timeout;}

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(timeout);
            out.writeObject(value);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            insertion_time=System.currentTimeMillis();
            timeout=in.readLong();
            value=(V)in.readObject();
        }
    }
    

    private class Reaper implements Runnable {

        public void run() {
            evict();
        }
    }

    public interface ChangeListener {
        void changed();
    }

}

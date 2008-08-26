package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;

import java.util.concurrent.*;
import java.util.*;
import java.io.*;

/**
 * Simple cache which maintains keys and value. A reaper can be enabled which periodically evicts expired entries.
 * Also, when the cache is configured to be bounded, entries in excess of the max size will be evicted on put(). 
 * @author Bela Ban
 * @version $Id: Cache.java,v 1.5 2008/08/26 06:36:47 belaban Exp $
 */
@Experimental
@Unsupported
public class Cache<K,V> {
    private static final Log log=LogFactory.getLog(Cache.class);
    private final ConcurrentMap<K,Value<V>> map=new ConcurrentHashMap<K,Value<V>>();
    private ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1);
    private Future task=null;

    /** The maximum number of keys, When this value is exceeded we evict older entries, until we drop below this 
     * mark again. This effectively maintains a bounded cache. A value of 0 means don't bound the cache.
     */
    private int max_num_entries=0;

    public int getMaxNumberOfEntries() {
        return max_num_entries;
    }

    public void setMaxNumberOfEntries(int max_num_entries) {
        this.max_num_entries=max_num_entries;
    }

    /** Runs the reaper every interval ms, evicts expired items */
    public void enableReaping(long interval) {
        if(task != null)
            task.cancel(false);
        task=timer.scheduleWithFixedDelay(new Reaper(), 0, interval, TimeUnit.MILLISECONDS);
    }

    public void disableReaping() {
        if(task != null) {
            task.cancel(false);
            task=null;
        }
    }

    public void start() {
        if(timer == null)
            timer=new ScheduledThreadPoolExecutor(1);
    }

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
    public V put(K key, V val, long caching_time) {
        if(log.isTraceEnabled())
            log.trace("put(" + key + ", " + val + ", " + caching_time + ")");
        Value<V> value=new Value<V>(val, caching_time <= 0? caching_time : System.currentTimeMillis() + caching_time);
        Value<V> retval=map.put(key, value);

        if(max_num_entries > 0) {
            if(map.size() > max_num_entries) {
                evict(); // see if we can gracefully evict expired items
            }
            if(map.size() > max_num_entries) {
                // still too many entries: now evict entries based on insertion time: oldest first
                int diff=map.size() - max_num_entries; // we have to evict diff entries
                SortedMap<Long,K> tmp=new TreeMap<Long,K>();
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
        }
        return retval != null? retval.value : null;
    }

    public V get(K key) {
        if(log.isTraceEnabled())
            log.trace("get(" + key + ")");
        Value<V> val=map.get(key);
        if(val == null)
            return null;
        if(val.expiration_time == -1 ||
                (val.expiration_time > 0 && val.expiration_time < System.currentTimeMillis())) {
            map.remove(key);
            return null;
        }
        return val.value;
    }

    public Value<V> getEntry(K key) {
        if(log.isTraceEnabled())
            log.trace("getEntry(" + key + ")");
        return map.get(key);
    }

    public V remove(K key) {
        if(log.isTraceEnabled())
            log.trace("remove(" + key + ")");
        Value<V> val=map.remove(key);
        return val != null? val.value : null;
    }

    public Set<Map.Entry<K,Value<V>>> entrySet() {
        return map.entrySet();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Value<V>> entry: map.entrySet()) {
            Value<V> val=entry.getValue();
            sb.append(entry.getKey()).append(": ").append(entry.getValue().getValue());
            sb.append(" (expiration_time: ");
            long expiration_time=val.getExpirationTime();
            if(expiration_time <= 0)
                sb.append(expiration_time);
            else {
                sb.append(new Date(expiration_time));
            }
            sb.append(")\n");
        }
        return sb.toString();
    }

    private void evict() {
        for(Iterator<Map.Entry<K,Value<V>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K,Value<V>> entry=it.next();
            Value<V> val=entry.getValue();
            if(val != null) {
                if(val.expiration_time == -1 || (val.expiration_time > 0 && System.currentTimeMillis() > val.expiration_time)) {
                    if(log.isTraceEnabled())
                        log.trace("evicting " + entry.getKey() + ": " + entry.getValue().value);
                    it.remove();
                }
            }
        }
    }

    

    public static class Value<V> implements Externalizable {
        private V value;

        private long insertion_time=System.currentTimeMillis();
        
        /** When the value can be reaped (in ms) */
        private transient long expiration_time;
        private static final long serialVersionUID=-3445944261826378608L;


        public Value(V value, long expiration_time) {
            this.value=value;
            this.expiration_time=expiration_time;
        }

        public Value() {
        }

        public V getValue() {return value;}
        public long getInsertionTime() {return insertion_time;}
        public long getExpirationTime() {return expiration_time;}

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(expiration_time);
            out.writeObject(value);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            insertion_time=System.currentTimeMillis();
            expiration_time=in.readLong();
            value=(V)in.readObject();
        }
    }
    

    private class Reaper implements Runnable {

        public void run() {
            evict();
        }
    }

}

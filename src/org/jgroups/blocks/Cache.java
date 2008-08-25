package org.jgroups.blocks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Unsupported;

import java.util.concurrent.*;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simple cache which maintains keys and value. A reaper can be enabled which periodically evicts expired entries.
 * Also, when the cache is configured to be bounded, entries in excess of the max size will be evicted on put(). 
 * @author Bela Ban
 * @version $Id: Cache.java,v 1.1 2008/08/25 11:31:51 belaban Exp $
 */
@Experimental
@Unsupported
public class Cache<K,V> {
    private static final Log log=LogFactory.getLog(Cache.class);
    private final ConcurrentMap<K,Value<V>> map=new ConcurrentHashMap<K,Value<V>>();
    private final ScheduledThreadPoolExecutor timer=new ScheduledThreadPoolExecutor(1);
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


    public void put(K key, V val, long caching_time) {
        if(log.isTraceEnabled())
            log.trace("put(" + key + ", " + val + ", " + caching_time + ")");
        Value<V> value=new Value<V>(val, caching_time <= 0? caching_time : System.currentTimeMillis() + caching_time);
        map.put(key, value);

        if(max_num_entries > 0) {
            if(map.size() > max_num_entries) {
                evict(); // see if we can gracefully evict expired items
            }
            if(map.size() > max_num_entries) {
                // still too many entries: now evict entries based on insertion time: oldest first
                int diff=max_num_entries - map.size(); // we have to evict diff entries
                SortedMap<Long,K> tmp=new TreeMap<Long,K>();
                for(Map.Entry<K,Value<V>> entry: map.entrySet()) {
                    tmp.put(entry.getValue().insertion_time, entry.getKey());
                }

                for(K k: tmp.values()) {
                    if(diff-- > 0)
                        map.remove(k);
                    else
                        break;
                }
            }
        }
    }

    public Object get(K key) {
        if(log.isTraceEnabled())
            log.trace("get(" + key + ")");
        Value<V> val=map.get(key);
        if(val == null)
            return val;
        if(val.expiration_time < System.currentTimeMillis()) {
            map.remove(key);
            return null;
        }
        return val.value;
    }

    public void remove(K key) {
        if(log.isTraceEnabled())
            log.trace("remove(" + key + ")");
        map.remove(key);
    }

    private void evict() {
        for(Iterator<Map.Entry<K,Value<V>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K,Value<V>> entry=it.next();
            Value<V> val=entry.getValue();
            if(val != null && val.expiration_time > 0 && System.currentTimeMillis() > val.expiration_time) {
                if(log.isTraceEnabled())
                    log.trace("evicting " + entry.getKey() + ": " + entry.getValue().value);
                it.remove();
            }
        }
    }

    

    private static class Value<V> {
        private final V value;

        private final long insertion_time=System.currentTimeMillis();
        
        /** When the value can be reaped (in ms) */
        private final long expiration_time;

        public Value(V value, long expiration_time) {
            this.value=value;
            this.expiration_time=expiration_time;
        }
    }

    private class Reaper implements Runnable {

        public void run() {
            evict();
        }
    }

}

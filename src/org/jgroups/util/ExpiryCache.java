package org.jgroups.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/** Cache which maintains timestamps for keys, and methods to remove/replace expired keys. Compared to {@link AgeOutCache},
 * ExpiryCache doesn't require a timer task to run
 * @author  Bela Ban
 * @version 3.3
 */
public class ExpiryCache<K> {
    protected long                        timeout; // in nanoseconds

    // Maintains keys and timestamps (in nanoseconds)
    protected final ConcurrentMap<K,Long> map=new ConcurrentHashMap<>();

    /**
     * Creates a new instance
     * @param timeout Timeout in ms
     */
    public ExpiryCache(long timeout) {
        setTimeout(timeout);
    }

    public long getTimeout() {return TimeUnit.MILLISECONDS.convert(timeout, TimeUnit.NANOSECONDS);}

    public void setTimeout(long timeout) {this.timeout=TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);}


    public boolean addIfAbsentOrExpired(K key) {
        Long val=map.get(key);
        if(val == null)
            return map.putIfAbsent(key, System.nanoTime()) == null;
        long current_time=System.nanoTime();
        return hasExpired(val, current_time) && map.replace(key, val, current_time);
    }

    public boolean contains(K key) {
        return key != null && map.containsKey(key);
    }

    public boolean hasExpired(K key) {
        Long val=map.get(key);
        return val == null || hasExpired(val, System.nanoTime());
    }

    public void remove(K key) {
        map.remove(key);
    }

    public void removeAll(Collection<K> keys) {
        map.keySet().removeAll(keys);
    }


    public int removeExpiredElements() {
        int removed=0;
        long current_time=System.nanoTime();
        for(Map.Entry<K,Long> entry: map.entrySet()) {
            Long val=entry.getValue();
            if(val == null || hasExpired(val, current_time)) {
                map.remove(entry.getKey());
                removed++;
            }
        }
        return removed;
    }

    public void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        long current_time=System.nanoTime();
        for(Map.Entry<K,Long> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": (age: ");
            long val=entry.getValue(); // timestamp in ns
            long age=TimeUnit.MILLISECONDS.convert(current_time - val, TimeUnit.NANOSECONDS); // ms
            if(age< 1000L)
                sb.append(age).append(" ms)");
            else
                sb.append(TimeUnit.SECONDS.convert(age, TimeUnit.MILLISECONDS)).append(" secs");
            sb.append("\n");
        }
        return sb.toString();
    }


    protected boolean hasExpired(long val, long current_time) {
        return current_time - val > timeout;
    }
}

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
    protected final ConcurrentMap<K,Long> map=new ConcurrentHashMap<K,Long>();

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
            return map.putIfAbsent(key, System.nanoTime() + timeout) == null;
        return hasExpired(val) && map.replace(key, val, System.nanoTime() + timeout);
    }

    public boolean contains(K key) {
        return key != null && map.containsKey(key);
    }

    public boolean hasExpired(K key) {
        Long val=map.get(key);
        return val == null || hasExpired(val);
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
            if(val == null || current_time >= val) {
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
            sb.append(entry.getKey()).append(": ");
            long val=entry.getValue(); // expiry time in ns
            long diff=val - current_time;
            long diff_ms=TimeUnit.MILLISECONDS.convert(diff, TimeUnit.NANOSECONDS);
            if(diff <= 0) { // expired

                sb.append("(expired ").append(Math.abs(diff_ms)).append(" ms ago)");
            }
            else { // not expired
                sb.append("(expiring in ").append(diff_ms).append(" ms)");
            }
            sb.append("\n");
        }
        return sb.toString();
    }


    protected static boolean hasExpired(long val) {
        return System.nanoTime() >= val;
    }
}

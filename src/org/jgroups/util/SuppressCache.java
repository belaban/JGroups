package org.jgroups.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache which keeps a timestamp and counter for every key. When a timestamp has expired (based on an expiry time), the
 * corresponding key is removed.<p/>
 * This cache is mainly used to suppress identical warning messages (in TP), e.g. if we get 1000 warnings about reception
 * of messages from P (who's not in our cluster), we can print such a message only every 60 seconds (expiry time = 60 secs).
 * @author Bela Ban
 * @since 3.2
 */
public class SuppressCache<T> {
    protected final ConcurrentMap<T,Value> map=new ConcurrentHashMap<>();
    protected final T NULL_KEY=(T)new Object();


    /**
     * Adds a new key to the hashmap, or updates the Value associated with the existing key if present. If expiry_time
     * is greater than the age of the Value, the key will be removed.
     * @param key The key
     * @param expiry_time Expiry time (in ms)
     * @return Null if the key was present and not expired, or the Value associated with the existing key
     * (its count incremented)
     */
    public Value putIfAbsent(T key, long expiry_time) {
        if(key == null)
            key=NULL_KEY;
        Value val=map.get(key);
        if(val == null) {
            val=new Value();
            Value existing=map.putIfAbsent(key, val);
            if(existing == null)
                return val;
            val=existing;
        }
        // key already exists
        if(val.update().age() > expiry_time) {
            map.remove(key);
            map.putIfAbsent(key, new Value());
            return val;
        }
        return null;
    }

    public void clear() {map.clear();}

    public void retainAll(Collection<T> list) {
        if(list != null)
            map.keySet().retainAll(list);
    }

    public void removeAll(Collection<T> list) {
        if(list != null)
            map.keySet().removeAll(list);
    }

    public void removeExpired(long expiry_time) {
        map.entrySet().stream().filter(entry -> entry.getValue().age() >= expiry_time).forEach(entry -> map.remove(entry.getKey()));
    }

    /** Returns the total count of all values */
    public int size() {
        int count=0;
        for(Value val: map.values())
            count+=val.count();

        return count;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<T,Value> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    public static final class Value {
        public Value() {
            this.timestamp=System.currentTimeMillis();
            this.count=1;
        }

        protected final long timestamp; // time of last update
        protected int        count;     // number of accesses since last update

        public long               age()    {return System.currentTimeMillis() - timestamp;}
        public synchronized Value update() {count++; return this;}
        public int count()                 {return count;}

        public String toString() {return count + " update(s) in " + age() + " ms";}
    }
}

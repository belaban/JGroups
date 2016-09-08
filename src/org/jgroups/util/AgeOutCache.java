package org.jgroups.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

/** Cache which removes its elements after a certain time
 * @author Bela Ban
 */
public class AgeOutCache<K> {
    private final TimeScheduler           timer;
    private long                          timeout;
    private final ConcurrentMap<K,Future> map=new ConcurrentHashMap<>();
    private Handler                       handler;

    public interface Handler<K> {
        void expired(K key);
    }


    public AgeOutCache(TimeScheduler timer, long timeout) {
        this.timer=timer;
        this.timeout=timeout;
    }

    public AgeOutCache(TimeScheduler timer, long timeout, Handler handler) {
        this(timer, timeout);
        this.handler=handler;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout=timeout;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler=handler;
    }

    public void add(final K key) {
        Future<?> future=timer.schedule(new Runnable() {
            public void run() {
                if(handler != null) {
                    try {
                        handler.expired(key);
                    }
                    catch(Throwable t) {
                    }
                }
                Future<?> tmp=map.remove(key);
                if(tmp != null)
                    tmp.cancel(true);
            }

            public String toString() {
                return "AgeOutCache (timeout=" + timeout +
                  ", handler=" + (handler != null? handler.getClass().getSimpleName() : null) + ")";
            }
        }, timeout, TimeUnit.MILLISECONDS, false); // this task never blocks
        Future<?> result=map.putIfAbsent(key, future);
        if(result != null)
            future.cancel(true);
    }

    public boolean contains(K key) {
        return key != null && map.containsKey(key);
    }

    public void remove(K key) {
        Future<?> future=map.remove(key);
        if(future != null)
            future.cancel(true);
    }

    public void removeAll(Collection<K> keys) {
        if(keys != null)
            keys.forEach(this::remove);
    }

    public void clear() {
        for(Future<?> future: map.values())
            future.cancel(true);
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Future> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }
}

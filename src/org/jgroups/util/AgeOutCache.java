package org.jgroups.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

/** Cache which removes its elements after a certain time
 * @author Bela Ban
 * @version $Id: AgeOutCache.java,v 1.5.2.2 2009/09/10 20:49:39 belaban Exp $
 */
public class AgeOutCache<K> {
    private final ScheduledExecutorService timer;
    private long timeout;
    private final ConcurrentMap<K,ScheduledFuture> map=new ConcurrentHashMap<K,ScheduledFuture>();
    private Handler handler=null;

    public interface Handler<K> {
        void expired(K key);
    }


    public AgeOutCache(ScheduledExecutorService timer, long timeout) {
        this.timer=timer;
        this.timeout=timeout;
    }

    public AgeOutCache(ScheduledExecutorService timer, long timeout, Handler handler) {
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
        ScheduledFuture<?> future=timer.schedule(new Runnable() {
            public void run() {
                if(handler != null) {
                    try {
                        handler.expired(key);
                    }
                    catch(Throwable t) {
                    }
                }
                ScheduledFuture tmp=map.remove(key);
                if(tmp != null)
                    tmp.cancel(true);
            }
        }, timeout, TimeUnit.MILLISECONDS);
        ScheduledFuture result=map.putIfAbsent(key, future);
        if(result != null)
            future.cancel(true);
    }

    public boolean contains(K key) {
        return key != null && map.containsKey(key);
    }

    public void remove(K key) {
        ScheduledFuture future=map.remove(key);
        if(future != null)
            future.cancel(true);
    }

    public void removeAll(Collection<K> keys) {
        if(keys != null) {
            for(K key: keys)
                remove(key);
        }
    }

    public void clear() {
        for(ScheduledFuture future: map.values())
            future.cancel(true);
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,ScheduledFuture> entry: map.entrySet()) {
            long time_to_expire=entry.getValue().getDelay(TimeUnit.MILLISECONDS);
            sb.append(entry.getKey()).append(": ");
            if(time_to_expire > 0)
                sb.append(time_to_expire).append(" ms to expire\n");
            else
                sb.append("expired");
        }
        return sb.toString();
    }
}

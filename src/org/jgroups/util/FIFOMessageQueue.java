package org.jgroups.util;

import org.jgroups.Address;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Blocking queue which can only process 1 message per service concurrently, establishing FIFO order per sender. Example:
 * if message A1, A2, A3, B1, B2 (where A and B are service names for services on top of a Multiplexer) arrive at the
 * same time, then this class will deliver A1 and B1 concurrently (ie. pass them up to the thread pool for processing).
 * Only when A1 is done will A2 be processed, same for B2: it will get processed when B1 is done. Thus, messages
 * for different services are processed concurrently; messages from the same service are processed FIFO.
 * @author Bela Ban
 * @version $Id: FIFOMessageQueue.java,v 1.9 2009/06/22 14:34:26 belaban Exp $
 * @deprecated Will be removed together with the Multiplexer in 3.0
 */
@Deprecated
public class FIFOMessageQueue<K, V> {
    /** Used for consolidated takes */
    final BlockingQueue<V>                       queue=new LinkedBlockingQueue<V>();

    /** One queue per sender and destination. This is a two level hashmap, with sender's addresses as keys and hashmaps
     * as values. Those hashmaps have destinations (K) as keys and Entries (list of Vs) as values */
    final ConcurrentMap<Address,ConcurrentMap<K,Entry<V>>> queues=new ConcurrentHashMap<Address,ConcurrentMap<K,Entry<V>>>();

    private final AtomicInteger size=new AtomicInteger(0);


    public V take() throws InterruptedException {
        V retval=queue.take();
        if(retval != null)
            size.decrementAndGet();
        return retval;
    }


    public V poll(long timeout) throws InterruptedException {
        V retval=queue.poll(timeout, TimeUnit.MILLISECONDS);
        if(retval != null)
            size.decrementAndGet();
        return retval;
    }



    public void put(Address sender, K dest, V el) throws InterruptedException {
        if(sender == null) {
            size.incrementAndGet();
            queue.add(el);
            return;
        }

        ConcurrentMap<K,Entry<V>> dests=queues.get(sender);
        if(dests == null) {
            dests=new ConcurrentHashMap<K,Entry<V>>();
            if(queues.putIfAbsent(sender, dests) != null) // already existed (someone else inserted the key/value mapping)
                dests=queues.get(sender);
        }

        Entry<V> entry=dests.get(dest);
        if(entry == null) {
            entry=new Entry<V>();
            if(dests.putIfAbsent(dest, entry) != null)
                entry=dests.get(dest);
        }

        synchronized(entry) {
            size.incrementAndGet();
            if(entry.ready) {
                entry.ready=false;
                queue.add(el);
            }
            else {
                entry.list.add(el);
            }
        }
    }


    public void done(Address sender, K dest) {
        if(sender == null)
            return;
        Map<K,Entry<V>> dests=queues.get(sender);
        if(dests == null) return;

        Entry<V> entry=dests.get(dest);
        if(entry != null) {
            V el=null;
            synchronized(entry) {
                if(!entry.list.isEmpty()) {
                    el=entry.list.removeFirst();
                    queue.add(el);
                }
                else {
                    entry.ready=true;
                }
            }
        }
    }

    public int size() {
        return size.get();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("queue: ").append(queue).append("\nqueues:\n");
        for(ConcurrentMap.Entry<Address,ConcurrentMap<K,Entry<V>>> entry: queues.entrySet()) {
            sb.append("sender ").append(entry.getKey()).append(":\n");
            for(Map.Entry<K,Entry<V>> entry2: entry.getValue().entrySet()) {
                sb.append(entry2.getKey()).append(": ").append(entry2.getValue().list).append("\n");
            }
        }
        return sb.toString();
    }



    static class Entry<T> {
        boolean ready=true;
        LinkedList<T> list=new LinkedList<T>();
    }
    
}

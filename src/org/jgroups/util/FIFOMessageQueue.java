package org.jgroups.util;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Blocking queue which can only process 1 message per sender concurrently, establishing FIFO order per sender.
 * @author Bela Ban
 * @version $Id: FIFOMessageQueue.java,v 1.2 2007/02/27 17:12:30 belaban Exp $
 */
public class FIFOMessageQueue<K, V> {
    /** Used for consolidated takes */
    final BlockingQueue<V>               queue=new LinkedBlockingQueue<V>();
    /** One queue per sender */
    final ConcurrentMap<K,Entry<V>> queues=new ConcurrentHashMap<K,Entry<V>>();

    private int size=0;


    public V take() throws InterruptedException {
        V retval=queue.take();
        size--;
        return retval;
    }

    public V poll(long timeout) throws InterruptedException {
        V retval=queue.poll(timeout, TimeUnit.MILLISECONDS);
        size--;
        return retval;
    }

    public void put(K dest, V el) throws InterruptedException {
        Entry<V> entry=queues.get(dest);
        if(entry == null) {
            synchronized(queues) {
                entry=new Entry<V>();
                queues.put(dest, entry);
            }
        }
        boolean add=false;
        synchronized(entry) {
            if(entry.ready) {
                entry.ready=false;
                add=true;
            }
            else {
                entry.list.add(el);
                size++;
            }
        }
        if(add) {
            queue.put(el);
            size++;
        }
    }


    public void done(K dest) {
        Entry<V> entry=queues.get(dest);
        if(entry != null) {
            V el=null;
            synchronized(entry) {
                // entry.ready=true;
                if(!entry.list.isEmpty()) {
                    el=entry.list.removeFirst();
                }
            }
            if(el != null)
                queue.add(el);
        }
    }

    public int size() {
        return size;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("queue: ").append(queue).append("\nqueues:\n");
        for(Map.Entry<K,Entry<V>> entry: queues.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue().list).append("\n");
        }
        return sb.toString();
    }



    static class Entry<T> {
        boolean ready=true;
        LinkedList<T> list=new LinkedList<T>();
    }
    
}

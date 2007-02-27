package org.jgroups.util;

import org.jgroups.Event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;

/**
 * Blocking queue which can only process 1 message per sender concurrently, establishing FIFO order per sender.
 * @author Bela Ban
 * @version $Id: FIFOMessageQueue.java,v 1.1 2007/02/27 13:16:59 belaban Exp $
 */
public class FIFOMessageQueue {
    /** Used for consolidated takes */
    final BlockingQueue<Event>        queue=new LinkedBlockingQueue<Event>();
    /** One queue per sender */
    final ConcurrentMap<Object,Entry> queues=new ConcurrentHashMap();


    public Event take() throws InterruptedException {
        return queue.take();
    }

    public void put(Object dest, Event evt) throws InterruptedException {
        Entry entry=queues.get(dest);
        if(entry == null) {
            synchronized(queues) {
                entry=new Entry();
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
                entry.list.add(evt);
            }
        }
        if(add)
            queue.put(evt);
    }


    public void done(Object dest) {
        Entry entry=queues.get(dest);
        if(entry != null) {
            Event evt=null;
            synchronized(entry) {
                entry.ready=true;
                if(!entry.list.isEmpty()) {
                    evt=entry.list.remove(0);
                }
            }
            if(evt != null)
                queue.add(evt);
        }
    }



    static class Entry {
        boolean ready=true;
        java.util.List<Event> list=new LinkedList<Event>();
    }
    
}

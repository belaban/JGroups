package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A cache associating members and messages
 * @author Bela Ban
 * @since  5.3.2
 */
public class MessageCache {
    protected final Map<Address,Queue<Message>> map=new ConcurrentHashMap<>();
    protected volatile boolean                  is_empty=true;

    public MessageCache add(Address sender, Message msg) {
        Queue<Message> list=map.computeIfAbsent(sender, addr -> new ConcurrentLinkedQueue<>());
        list.add(msg);
        is_empty=false;
        return this;
    }

    public Collection<Message> drain(Address sender) {
        if(sender == null)
            return null;
        Queue<Message> queue=map.remove(sender);
        if(map.isEmpty())
            is_empty=true;
        return queue;
    }

    public MessageCache clear() {
        map.clear();
        is_empty=true;
        return this;
    }

    /** Returns a count of all messages */
    public int size() {
        return map.values().stream().mapToInt(Collection::size).sum();
    }

    public boolean isEmpty() {
        return is_empty;
    }

    public String toString() {
        return String.format("%d message(s)", size());
    }
}

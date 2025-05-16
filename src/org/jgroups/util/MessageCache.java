package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * A cache associating members and messages
 * @author Bela Ban
 * @since  5.3.2
 */
public class MessageCache {
    protected final Map<Address,Queue<Message>> map=new ConcurrentHashMap<>();
    protected static final Function<Address, Queue<Message>> FUNC=__ -> new ConcurrentLinkedQueue<>();

    public MessageCache add(Address sender, Message msg) {
        Queue<Message> list=map.computeIfAbsent(sender, FUNC);
        list.add(msg);
        return this;
    }

    public Collection<Message> drain(Address sender) {
        if(sender == null)
            return null;
        return map.remove(sender);
    }

    public MessageCache clear() {
        map.clear();
        return this;
    }

    /** Returns a count of all messages */
    public int size() {
        return map.values().stream().mapToInt(Collection::size).sum();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public String toString() {
        return String.format("%d message(s)", size());
    }
}

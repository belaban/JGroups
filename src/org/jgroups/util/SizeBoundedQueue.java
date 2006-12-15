package org.jgroups.util;

import EDU.oswego.cs.dl.util.concurrent.BoundedChannel;
import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

import java.util.Map;

/**
 * Queue as described in http://jira.jboss.com/jira/browse/JGRP-376. However, this queue only works with
 * {@link org.jgroups.protocols.TP.IncomingMessage} elements.
 * The queue maintains a max number of bytes and a total of all of the messages in the internal queues. Whenever a
 * message is added, we increment the total by the length of the message. When a message is removed, we decrement the
 * total. Removal blocks until a message is available, addition blocks if the max size has been exceeded, until there
 * is enough space to add another message.
 * Note that the max size should always be greater than the size of the largest message to be received, otherwise an
 * additon would always fail because msg.length > max size !<br/>
 * Access patterns: this instance is always accessed by the thread pool only ! Concurrent take() or poll() methods,
 * but only a single thread at a time calls put() !
 * @author Bela Ban
 * @version $Id: SizeBoundedQueue.java,v 1.2 2006/12/15 15:54:42 belaban Exp $
 */
public class SizeBoundedQueue implements BoundedChannel {
    int max_size=1000 * 1000;
    int capacity=0;
    /** Map<Address, List<IncomingMessage>>. Maintains a list of unicast messages per sender */
    final Map ucast_msgs=new ConcurrentHashMap(10);
    /** Map<Address, List<IncomingMessage>>. Maintains a list of multicast messages per sender */
    final Map mcast_msgs=new ConcurrentHashMap(10);


    public SizeBoundedQueue() {
    }

    public SizeBoundedQueue(int max_size) {
        this.max_size=max_size;
    }

    public int capacity() {
        return capacity;
    }

    public void setCapacity(int new_capacity) {
        capacity=new_capacity;
        // todo: compute and adjust capacity of all internal queues
    }

    public void put(Object item) throws InterruptedException {
    }

    public boolean offer(Object item, long msecs) throws InterruptedException {
        return false;
    }

    public Object take() throws InterruptedException {
        return null;
    }

    public Object poll(long msecs) throws InterruptedException {
        return null;
    }

    public Object peek() {
        return null;
    }


}

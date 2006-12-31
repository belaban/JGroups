package org.jgroups.util;


import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Queue as described in http://jira.jboss.com/jira/browse/JGRP-376. However, this queue only works with
 * {@link org.jgroups.protocols.TP.IncomingPacket} elements.
 * The queue maintains a max number of bytes and a total of all of the messages in the internal queues. Whenever a
 * message is added, we increment the total by the length of the message. When a message is removed, we decrement the
 * total. Removal blocks until a message is available, addition blocks if the max size has been exceeded, until there
 * is enough space to add another message.
 * Note that the max size should always be greater than the size of the largest message to be received, otherwise an
 * additon would always fail because msg.length > max size !<br/>
 * Access patterns: this instance is always accessed by the thread pool only ! Concurrent take() or poll() methods,
 * but only a single thread at a time calls put() !
 * @author Bela Ban
 * @version $Id: SizeBoundedQueue.java,v 1.3 2006/12/31 14:29:52 belaban Exp $
 */
public class SizeBoundedQueue implements BlockingQueue {
    int max_size=1000 * 1000;
    int capacity=0;
    /** Map<Address, List<IncomingMessage>>. Maintains a list of unicast messages per sender */
    final Map ucast_msgs=new ConcurrentHashMap(10);
    /** Map<Address, List<IncomingMessage>>. Maintains a list of multicast messages per sender */
    final Map mcast_msgs=new ConcurrentHashMap(10);


    public boolean add(Object o) {
        return false;
    }

    public int drainTo(Collection c) {
        return 0;
    }

    public int drainTo(Collection c, int maxElements) {
        return 0;
    }

    public boolean offer(Object o) {
        return false;
    }

    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    public void put(Object o) throws InterruptedException {
    }

    public int remainingCapacity() {
        return 0;
    }

    public Object take() throws InterruptedException {
        return null;
    }


    public Object element() {
        return null;
    }

    public Object peek() {
        return null;
    }

    public Object poll() {
        return null;
    }

    public Object remove() {
        return null;
    }

    public boolean addAll(Collection c) {
        return false;
    }

    public void clear() {
    }

    public boolean contains(Object o) {
        return false;
    }

    public boolean containsAll(Collection c) {
        return false;
    }

    public boolean isEmpty() {
        return false;
    }

    public Iterator iterator() {
        return null;
    }

    public boolean remove(Object o) {
        return false;
    }

    public boolean removeAll(Collection c) {
        return false;
    }

    public boolean retainAll(Collection c) {
        return false;
    }

    public int size() {
        return 0;
    }

    public Object[] toArray() {
        return new Object[0];
    }

    public Object[] toArray(Object[] a) {
        return new Object[0];
    }
}

package org.jgroups.util;

import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * A bounded subclass of LinkedList, oldest elements are removed once max capacity is exceeded. Note that this
 * class is not synchronized (like LinkedList).
 * @author Bela Ban Nov 20, 2003
 * @version $Id: BoundedList.java,v 1.5 2007/08/08 12:02:05 belaban Exp $
 */
public class BoundedList<T> extends ConcurrentLinkedQueue<T> {
    int max_capacity=10;


    public BoundedList() {
        super();
    }

    public BoundedList(int size) {
        super();
        max_capacity=size;
    }


    /**
     * Adds an element at the tail. Removes an object from the head if capacity is exceeded
     * @param obj The object to be added
     */
    public boolean add(T obj) {
        if(obj == null) return false;
        while(size() >= max_capacity && size() > 0) {
            poll();
        }
        return super.add(obj);
    }



    public T removeFromHead() {
        return poll();
    }


}

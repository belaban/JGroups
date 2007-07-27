package org.jgroups.util;


/**
 * A bounded subclass of LinkedList, oldest elements are removed once max capacity is exceeded. Note that this
 * class is not synchronized (like LinkedList).
 * @author Bela Ban Nov 20, 2003
 * @version $Id: BoundedList.java,v 1.3 2007/07/27 11:01:00 belaban Exp $
 */
public class BoundedList<T> extends java.util.LinkedList<T> {
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
            removeFirst();
        }
        return super.add(obj);
    }


    /**
     * Adds an object to the head, removes an element from the tail if capacity has been exceeded
     * @param obj The object to be added
     */
    public void addAtHead(T obj) {
        if(obj == null) return;
        while(size() >= max_capacity && size() > 0) {
            removeFirst();
        }
        super.add(0, obj);
    }


    public T removeFromHead() {
        return removeFirst();
    }


}

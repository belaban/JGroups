package org.jgroups.util;


/**
 * A bounded subclass of List, oldest elements are removed once max capacity is exceeded
 * @author Bela Ban Nov 20, 2003
 * @version $Id: BoundedList.java,v 1.1 2003/11/21 06:54:50 belaban Exp $
 */
public class BoundedList extends List {
    int max_capacity=10;


    public BoundedList(int size) {
        super();
        max_capacity=size;
    }

    /**
     * Adds an element at the tail. Removes an object from the head if capacity is exceeded
     * @param obj The object to be added
     */
    public void add(Object obj) {
        if(obj == null) return;
        while(size >= max_capacity && size > 0) {
            removeFromHead();
        }
        super.add(obj);
    }


    /**
     * Adds an object to the head, removes an element from the tail if capacity has been exceeded
     * @param obj The object to be added
     */
    public void addAtHead(Object obj) {
        if(obj == null) return;
        while(size >= max_capacity && size > 0) {
            remove();
        }
        super.addAtHead(obj);
    }
}

// $Id: Queue.java,v 1.3 2003/09/20 01:32:45 belaban Exp $

package org.jgroups.util;


import org.jgroups.TimeoutException;
import org.jgroups.log.Trace;

import java.util.Vector;




/**
 * Elements are added at the tail and removed from the head. Class is thread-safe in that
 * 1 producer and 1 consumer may add/remove elements concurrently. The class is not
 * explicitely designed for multiple producers or consumers. Implemented as a linked
 * list, so that removal of an element at the head does not cause a right-shift of the
 * remaining elements (as in a Vector-based implementation).
 * @author Bela Ban
 * @author Filip Hanik
 */
public class Queue {
    /*head and the tail of the list so that we can easily add and remove objects*/
    Element head=null, tail=null;

    /*flag to determine the state of the queue*/
    boolean closed=false;

    /*current size of the queue*/
    int     size=0;

    /* Lock object for synchronization. Is notified when element is added */
    Object  add_mutex=new Object();

    /*the number of end markers that have been added*/
    int     num_markers=0;


    /**
     * if the queue closes during the runtime
     * an endMarker object is added to the end of the queue to indicate that
     * the queue will close automatically when the end marker is encountered
     * This allows for a "soft" close.
     * @see Queue#close
     */
    private static final Object endMarker=new Object();

    /**
     * the class Element indicates an object in the queue.
     * This element allows for the linked list algorithm by always holding a
     * reference to the next element in the list.
     * if Element.next is null, then this element is the tail of the list.
     */
    class Element {
        /*the actual value stored in the queue*/
        Object obj=null;
        /*pointer to the next item in the (queue) linked list*/
        Element next=null;

        /**
         * creates an Element object holding its value
         * @param o - the object to be stored in the queue position
         */
        Element(Object o) {
            obj=o;
        }

        /**
         * prints out the value of the object
         */
        public String toString() {
            return obj != null? obj.toString() : "null";
        }
    }


    /**
     * creates an empty queue
     */
    public Queue() {
    }


    /**
     * Returns the first element. Returns null if no elements are available.
     */
    public Object getFirst() {
        return head != null? head.obj : null;
    }

    /**
     * Returns the last element. Returns null if no elements are available.
     */
    public Object getLast() {
        return tail != null? tail.obj : null;
    }


    /**
     * returns true if the Queue has been closed
     * however, this method will return false if the queue has been closed
     * using the close(true) method and the last element has yet not been received.
     * @return true if the queue has been closed
     */
    public boolean closed() {
        return closed;
    }

    /**
     * adds an object to the tail of this queue
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     */
    public void add(Object obj) throws QueueClosedException {
        if(obj == null) {
            Trace.error("Queue.add()", "argument must not be null");
            return;
        }
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("Queue.add(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        /*lock the queue from other threads*/
        synchronized(add_mutex) {
            /*create a new linked list element*/
            Element el=new Element(obj);
            /*check the first element*/
            if(head == null) {
                /*the object added is the first element*/
                /*set the head to be this object*/
                head=el;
                /*set the tail to be this object*/
                tail=head;
                /*set the size to be one, since the queue was empty*/
                size=1;
            }
            else {
                /*add the object to the end of the linked list*/
                tail.next=el;
                /*set the tail to point to the last element*/
                tail=el;
                /*increase the size*/
                size++;
            }
            /*wake up all the threads that are waiting for the lock to be released*/
            add_mutex.notifyAll();
            // add_mutex.notify();
        }
    }


    /**
     * Adds a new object to the head of the queue
     * basically (obj.equals(queue.remove(queue.add(obj)))) returns true
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     *
     */
    public void addAtHead(Object obj) throws QueueClosedException {
        if(obj == null) {
            Trace.error("Queue.addAtHead()", "argument must not be null");
            return;
        }
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("Queue.addAtHead(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        /*lock the queue from other threads*/
        synchronized(add_mutex) {
            Element el=new Element(obj);
            /*check the head element in the list*/
            if(head == null) {
                /*this is the first object, we could have done add(obj) here*/
                head=el;
                tail=head;
                size=1;
            }
            else {
                /*set the head element to be the child of this one*/
                el.next=head;
                /*set the head to point to the recently added object*/
                head=el;
                /*increase the size*/
                size++;
            }
            /*wake up all the threads that are waiting for the lock to be released*/
            add_mutex.notifyAll();
            // add_mutex.notify();
        }
    }


    /**
     * Removes 1 element from head or <B>blocks</B>
     * until next element has been added or until queue has been closed
     * @return the first element to be taken of the queue
     */
    public Object remove() throws QueueClosedException {
        /*initialize the return value*/
        Object retval=null;
        /*lock the queue*/
        synchronized(add_mutex) {
            /*wait as long as the queue is empty. return when an element is present or queue is closed*/
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    add_mutex.wait();
                }
                catch(IllegalMonitorStateException ex) {
                    throw ex;
                }
                catch(InterruptedException ex) {
                }
            }

            if(closed)
                throw new QueueClosedException();

            /*remove the head from the queue, if we make it to this point, retval should not be null !*/
            retval=removeInternal();
            if(retval == null)
                Trace.error("Queue.remove()", "element was null, should never be the case");
        }

        /*
         * we ran into an Endmarker, which means that the queue was closed before
         * through close(true)
         */
        if(retval == endMarker) {
            close(false); // mark queue as closed
            throw new QueueClosedException();
        }

        /*return the object*/
        return retval;
    }


    /**
     * Removes 1 element from the head.
     * If the queue is empty the operation will wait for timeout ms.
     * if no object is added during the timeout time, a Timout exception is thrown
     * @param timeout - the number of milli seconds this operation will wait before it times out
     * @return the first object in the queue
     */
    public Object remove(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        /*lock the queue*/
        synchronized(add_mutex) {
            /*if the queue size is zero, we want to wait until a new object is added*/
            if(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    /*release the add_mutex lock and wait no more than timeout ms*/
                    add_mutex.wait(timeout);
                }
                catch(IllegalMonitorStateException ex) {
                    throw ex;
                }
                catch(IllegalArgumentException ex2) {
                    throw ex2;
                }
                catch(InterruptedException ex) {
                }
            }
            /*we either timed out, or got notified by the add_mutex lock object*/

            /*check to see if the object closed*/
            if(closed)
                throw new QueueClosedException();

            /*get the next value*/
            retval=removeInternal();
            /*null result means we timed out*/
            if(retval == null) throw new TimeoutException();

            /*if we reached an end marker we are going to close the queue*/
            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            /*at this point we actually did receive a value from the queue, return it*/
            return retval;
        }
    }


    /**
     * removes a specific object from the queue.
     * the object is matched up using the Object.equals method.
     * @param   obj the actual object to be removed from the queue
     */
    public void removeElement(Object obj) throws QueueClosedException {
        Element el, tmp_el;

        if(obj == null) {
            Trace.error("Queue.removeElement()", "argument must not be null");
            return;
        }

        /*lock the queue*/
        synchronized(add_mutex) {
            el=head;

            /*the queue is empty*/
            if(el == null) return;

            /*check to see if the head element is the one to be removed*/
            if(el.obj.equals(obj)) {
                /*the head element matched we will remove it*/
                head=el.next;
                el.next=null;
                /*check if we only had one object left
                 *at this time the queue becomes empty
                 *this will set the tail=head=null
                 */
                if(size == 1)
                    tail=head;  // null
                decrementSize();
                /*and end the operation, it was successful*/
                return;
            }

            /*look through the other elements*/
            while(el.next != null) {
                if(el.next.obj.equals(obj)) {
                    tmp_el=el.next;
                    if(tmp_el == tail) // if it is the last element, move tail one to the left (bela Sept 20 2002)
                        tail=el;
                    el.next=el.next.next;  // point to the el past the next one. can be null.
                    tmp_el.next=null;
                    decrementSize();
                    break;
                }
                el=el.next;
            }
        }
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added
     * @return the first object on the queue
     */
    public Object peek() throws QueueClosedException {
        Object retval=null;

        synchronized(add_mutex) {
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    add_mutex.wait();
                }
                catch(IllegalMonitorStateException ex) {
                    throw ex;
                }
                catch(InterruptedException ex) {
                }
            }

            if(closed)
                throw new QueueClosedException();

            retval=(head != null)? head.obj : null;

            // @remove:
            if(retval == null) {
                // print some diagnostics
                Trace.error("Queue.peek()", "retval is null: head=" + head + ", tail=" + tail + ", size()=" + size() +
                                            ", num_markers=" + num_markers + ", closed()=" + closed());
            }
        }

        if(retval == endMarker) {
            close(false); // mark queue as closed
            throw new QueueClosedException();
        }

        return retval;
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added or the operation times out
     * @param timeout how long in milli seconds will this operation wait for an object to be added to the queue
     *        before it times out
     * @return the first object on the queue
     */

    public Object peek(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        synchronized(add_mutex) {
            if(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    add_mutex.wait(timeout);
                }
                catch(IllegalMonitorStateException ex) {
                    throw ex;
                }
                catch(IllegalArgumentException ex2) {
                    throw ex2;
                }
                catch(InterruptedException ex) {
                }
            }
            if(closed)
                throw new QueueClosedException();

            retval=head != null? head.obj : null;

            if(retval == null) throw new TimeoutException();

            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            return retval;
        }
    }


    /**
     Marks the queues as closed. When an <code>add</code> or <code>remove</code> operation is
     attempted on a closed queue, an exception is thrown.
     @param flush_entries When true, a end-of-entries marker is added to the end of the queue.
     Entries may be added and removed, but when the end-of-entries marker
     is encountered, the queue is marked as closed. This allows to flush
     pending messages before closing the queue.
     */
    public void close(boolean flush_entries) {
        if(flush_entries) {
            try {
                add(endMarker); // add an end-of-entries marker to the end of the queue
                num_markers++;
            }
            catch(QueueClosedException closed) {
            }
            return;
        }

        synchronized(add_mutex) {
            closed=true;
            try {
                add_mutex.notifyAll();
                // add_mutex.notify();
            }
            catch(Exception e) {
                Trace.error("Queue.close()", "exception=" + e);
            }
        }
    }


    /**
     * resets the queue.
     * This operation removes all the objects in the queue and marks the queue open
     */
    public void reset() {
        num_markers=0;
        if(!closed)
            close(false);

        synchronized(add_mutex) {
            size=0;
            head=null;
            tail=null;
            closed=false;
        }
    }


    /**
     * returns the number of objects that are currently in the queue
     */
    public int size() {
        return size - num_markers;
    }

    /**
     * prints the size of the queue
     */
    public String toString() {
        return "Queue (" + size() + ") messages";
    }

    /**
     * Dumps internal state @remove
     */
    public String debug() {
        return toString() + ", head=" + head + ", tail=" + tail + ", closed()=" + closed() + ", contents=" + getContents();
    }


    /**
     * returns a vector with all the objects currently in the queue
     */
    public Vector getContents() {
        Vector retval=new Vector();
        Element el;

        synchronized(add_mutex) {
            el=head;
            while(el != null) {
                retval.addElement(el.obj);
                el=el.next;
            }
        }
        return retval;
    }


    /**
     * Blocks until the queue has no elements left. If the queue is empty, the call will return
     * immediately
     * @param timeout Call returns if timeout has elapsed (number of milliseconds). 0 means to wait forever
     * @throws QueueClosedException Thrown if queue has been closed
     * @throws TimeoutException Thrown if timeout has elapsed
     */
    public void waitUntilEmpty(long timeout) throws QueueClosedException, TimeoutException {

        // TODO: use Locks instead of add_mutex: acquire add_lock, the remove_lock, the release add_lock, and
        // wait on remove_lock. Use util.concurrent locks

        // TODO: compare lock-based impl vs. synchronized-based impl (speed for large insertions)

        synchronized(add_mutex) {
            //System.out.println("SIZE: " + size);
            while(size > 0) {
                try {
                    add_mutex.wait(1);
                }
                catch(InterruptedException e) {}
            }
        }

//        synchronized(remove_mutex) {
//            if(closed)
//                throw new QueueClosedException();
//            if(size == 0)
//                return;
//            try {
//                remove_mutex.wait(timeout);
//            }
//            catch(InterruptedException e) {
//            }
//            if(closed)
//                throw new QueueClosedException();
//            if(size > 0)
//                throw new TimeoutException("queue has " + size + " elements");
//        }
//

    }


    /* ------------------------------------- Private Methods ----------------------------------- */


    /**
     * Removes the first element. Returns null if no elements in queue.
     * Always called with add_mutex locked (we don't have to lock add_mutex ourselves)
     */
    private Object removeInternal() {
        Element retval;

        /*if the head is null, the queue is empty*/
        if(head == null)
            return null;

        retval=head;       // head must be non-null now

        head=head.next;
        if(head == null)
            tail=null;

        decrementSize();

        if(head != null && head.obj == endMarker) {
            closed=true;
        }

        retval.next=null;
        return retval.obj;
    }


    void decrementSize() {
        size--;
        if(size < 0)
            size=0;
    }


    /* ---------------------------------- End of Private Methods -------------------------------- */

}

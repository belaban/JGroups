
package org.jgroups.util;


import org.jgroups.TimeoutException;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * Elements are added at the tail and removed from the head. Class is thread-safe in that
 * 1 producer and 1 consumer may add/remove elements concurrently. The class is not
 * explicitely designed for multiple producers or consumers. Implemented as a linked
 * list, so that removal of an element at the head does not cause a right-shift of the
 * remaining elements (as in a Vector-based implementation).
 * @author Bela Ban
 */
public class Queue {

    /*head and the tail of the list so that we can easily add and remove objects*/
    private Element head=null, tail=null;

    /*flag to determine the state of the queue*/
    private volatile boolean closed=false;

    /*current size of the queue*/
    private volatile int size=0;

    /* Lock object for synchronization. Is notified when element is added */
    private final Object  mutex=new Object();

    /** Lock object for syncing on removes. It is notified when an object is removed */
    // Object  remove_mutex=new Object();

    /*the number of end markers that have been added*/
    private int     num_markers=0;

    /**
     * if the queue closes during the runtime
     * an endMarker object is added to the end of the queue to indicate that
     * the queue will close automatically when the end marker is encountered
     * This allows for a "soft" close.
     * @see Queue#close
     */
    private static final Object endMarker=new Object();

    protected static final Log log=LogFactory.getLog(Queue.class);


    /**
     * the class Element indicates an object in the queue.
     * This element allows for the linked list algorithm by always holding a
     * reference to the next element in the list.
     * if Element.next is null, then this element is the tail of the list.
     */
    static class Element {
        /*the actual value stored in the queue*/
        Object  obj=null;
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
        synchronized(mutex) {
            return head != null? head.obj : null;
        }
    }

    /**
     * Returns the last element. Returns null if no elements are available.
     */
    public Object getLast() {
        synchronized(mutex) {
            return tail != null? tail.obj : null;
        }
    }


    /**
     * returns true if the Queue has been closed
     * however, this method will return false if the queue has been closed
     * using the close(true) method and the last element has yet not been received.
     * @return true if the queue has been closed
     */
    public boolean closed() {
        synchronized(mutex) {
            return closed;
        }
    }

    /**
     * adds an object to the tail of this queue
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     */
    public void add(Object obj) throws QueueClosedException {
        if(obj == null)
            return;

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");
            addInternal(obj);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }

    public void addAll(Collection c) throws QueueClosedException {
        if(c == null)
            return;

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");

            Object obj;
            for(Iterator it=c.iterator(); it.hasNext();) {
                obj=it.next();
                if(obj != null)
                    addInternal(obj);
            }

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }


    public void addAll(List<Object> list) throws QueueClosedException {
        if(list == null)
            return;

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");

            for(Object obj: list) {
                if(obj != null)
                    addInternal(obj);
            }

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }




    /**
     * Removes 1 element from head or <B>blocks</B>
     * until next element has been added or until queue has been closed
     * @return the first element to be taken of the queue
     */
    public Object remove() throws QueueClosedException {
        synchronized(mutex) {
            /*wait as long as the queue is empty. return when an element is present or queue is closed*/
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    mutex.wait();
                }
                catch(InterruptedException ex) {
                }
            }

            if(closed)
                throw new QueueClosedException();

            /*remove the head from the queue, if we make it to this point, retval should not be null !*/
            return removeInternal();
        }
    }


    /**
     * Removes 1 element from the head.
     * If the queue is empty the operation will wait for timeout ms.
     * if no object is added during the timeout time, a Timout exception is thrown
     * (bela Aug 2009) Note that the semantics of remove(long timeout) are weird - the method waits until an element has
     * been added, but doesn't do so in a loop ! So if we have 10 threads waiting on an empty queue, and 1 thread
     * adds an element, all 10 threads will return (but only 1 will have the element), therefore 9 will throw
     * a TimeoutException ! If I change this to the 'correct' semantics, however (e.g. the method removeWait() below),
     * GMS.ViewHandler doesn't work correctly anymore. I won't change this now, as Queue will get removed anyway in 3.0.
     * @param timeout - the number of milli seconds this operation will wait before it times out
     * @return the first object in the queue
     */
    public Object remove(long timeout) throws QueueClosedException, TimeoutException {
        Object retval;

        synchronized(mutex) {
            if(closed)
                throw new QueueClosedException();

            /*if the queue size is zero, we want to wait until a new object is added*/
            if(size == 0) {
                try {
                    /*release the mutex lock and wait no more than timeout ms*/
                    mutex.wait(timeout);
                }
                catch(InterruptedException ex) {
                }
            }
            /*we either timed out, or got notified by the mutex lock object*/
            if(closed)
                throw new QueueClosedException();

            /*get the next value*/
            retval=removeInternal();
            /*null result means we timed out*/
            if(retval == null) throw new TimeoutException("timeout=" + timeout + "ms");

            /*if we reached an end marker we are going to close the queue*/
//            if(retval == endMarker) {
//                close(false);
//                throw new QueueClosedException();
//            }
            /*at this point we actually did receive a value from the queue, return it*/
            return retval;
        }
    }


    public Object removeWait(long timeout) throws QueueClosedException, TimeoutException {
        synchronized(mutex) {
            if(closed)
                throw new QueueClosedException();

            final long end_time=System.currentTimeMillis() + timeout;
            long wait_time, current_time;

            /*if the queue size is zero, we want to wait until a new object is added*/
            while(size == 0 && (current_time=System.currentTimeMillis()) < end_time) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    /*release the mutex lock and wait no more than timeout ms*/
                    wait_time=end_time - current_time;  // guarnteed to be > 0
                    mutex.wait(wait_time);
                }
                catch(InterruptedException ex) {
                }
            }
            /*we either timed out, or got notified by the mutex lock object*/
            if(closed)
                throw new QueueClosedException();

            /*get the next value*/
            Object retval=removeInternal();
            /*null result means we timed out*/
            if(retval == null) throw new TimeoutException("timeout=" + timeout + "ms");

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

        if(obj == null)
            return;

        synchronized(mutex) {
            if(closed) /*check to see if the queue is closed*/
                throw new QueueClosedException();

            el=head;

            /*the queue is empty*/
            if(el == null) return;

            /*check to see if the head element is the one to be removed*/
            if(el.obj.equals(obj)) {
                /*the head element matched we will remove it*/
                head=el.next;
                el.next=null;
                el.obj=null;
                /*check if we only had one object left
                 *at this time the queue becomes empty
                 *this will set the tail=head=null
                 */
                if(size == 1)
                    tail=head;  // null
                decrementSize();
                return;
            }

            /*look through the other elements*/
            while(el.next != null) {
                if(el.next.obj.equals(obj)) {
                    tmp_el=el.next;
                    if(tmp_el == tail) // if it is the last element, move tail one to the left (bela Sept 20 2002)
                        tail=el;
                    el.next.obj=null;
                    el.next=el.next.next;  // point to the el past the next one. can be null.
                    tmp_el.next=null;
                    tmp_el.obj=null;
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
        Object retval;

        synchronized(mutex) {
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    mutex.wait();
                }
                catch(InterruptedException ex) {
                }
            }

            if(closed)
                throw new QueueClosedException();

            retval=(head != null)? head.obj : null;
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
        Object retval;

        synchronized(mutex) {
            if(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    mutex.wait(timeout);
                }
                catch(InterruptedException ex) {
                }
            }
            if(closed)
                throw new QueueClosedException();

            retval=head != null? head.obj : null;

            if(retval == null) throw new TimeoutException("timeout=" + timeout + "ms");

            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            return retval;
        }
    }

    /** Removes all elements from the queue. This method can succeed even when the queue is closed */
    public void clear() {
        synchronized(mutex) {
            head=tail=null;
            size=0;
            num_markers=0;
            mutex.notifyAll();
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
        synchronized(mutex) {
            if(flush_entries && size > 0) {
                try {
                    add(endMarker); // add an end-of-entries marker to the end of the queue
                    num_markers++;
                }
                catch(QueueClosedException closed_ex) {
                }
                return;
            }
            closed=true;
            mutex.notifyAll();
        }
    }

    /** Waits until the queue has been closed. Returns immediately if already closed
     * @param timeout Number of milliseconds to wait. A value <= 0 means to wait forever
     */
    public void waitUntilClosed(long timeout) {
        synchronized(mutex) {
            if(closed)
                return;
            try {
                mutex.wait(timeout);
            }
            catch(InterruptedException e) {
            }
        }
    }


    /**
     * resets the queue.
     * This operation removes all the objects in the queue and marks the queue open
     */
    public void reset() {
        synchronized(mutex) {
           num_markers=0;
           if(!closed)
              close(false);
            size=0;
            head=null;
            tail=null;
            closed=false;
            mutex.notifyAll();
        }
    }

    /**
     * Returns all the elements of the queue
     * @return A copy of the queue
     */
    public LinkedList values() {
        LinkedList retval=new LinkedList();
        synchronized(mutex) {
            Element el=head;
            while(el != null) {
                retval.add(el.obj);
                el=el.next;
            }
        }
        return retval;
    }


    /**
     * returns the number of objects that are currently in the queue
     */
    public int size() {
        synchronized(mutex) {
            return size - num_markers;
        }
    }

    /**
     * prints the size of the queue
     */
    public String toString() {
        return "Queue (" + size() + ") elements";
    }




    /* ------------------------------------- Private Methods ----------------------------------- */


    private final void addInternal(Object obj) {
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
    }

    /**
     * Removes the first element. Returns null if no elements in queue.
     * Always called with mutex locked (we don't have to lock mutex ourselves)
     */
    private Object removeInternal() {
        Element retval;
        Object obj;

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
            mutex.notifyAll();
        }

        retval.next=null;
        obj=retval.obj;
        retval.obj=null;
        return obj;
    }


    /** Doesn't need to be synchronized; is always called from synchronized methods */
    final private void decrementSize() {
        size--;
        if(size < 0)
            size=0;
    }


    /* ---------------------------------- End of Private Methods -------------------------------- */

}

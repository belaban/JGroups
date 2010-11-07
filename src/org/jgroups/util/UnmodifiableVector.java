package org.jgroups.util;

import java.util.*;

/**
 * Vector which cannot be modified
 * @author Bela Ban
 * @version $Id: UnmodifiableVector.java,v 1.3 2006/12/09 22:59:34 belaban Exp $
 */
public class UnmodifiableVector extends Vector {
    Vector v;

    public UnmodifiableVector(Vector v) {
        this.v=v;
    }

    public synchronized void copyInto(Object[] anArray) {
        v.copyInto(anArray);
    }

    public synchronized void trimToSize() {
        throw new UnsupportedOperationException();
    }

    public synchronized void ensureCapacity(int minCapacity) {
        throw new UnsupportedOperationException();
    }

    public synchronized void setSize(int newSize) {
        throw new UnsupportedOperationException();
    }

    public synchronized int capacity() {
        return v.capacity();
    }

    public synchronized int size() {
        return v.size();
    }

    public synchronized boolean isEmpty() {
        return v.isEmpty();
    }

    public Enumeration elements() {
        return v.elements();
    }

    public boolean contains(Object elem) {
        return v.contains(elem);
    }

    public int indexOf(Object elem) {
        return v.indexOf(elem);
    }

    public synchronized int indexOf(Object elem, int index) {
        return v.indexOf(elem, index);
    }

    public synchronized int lastIndexOf(Object elem) {
        return v.lastIndexOf(elem);
    }

    public synchronized int lastIndexOf(Object elem, int index) {
        return v.lastIndexOf(elem, index);
    }

    public synchronized Object elementAt(int index) {
        return v.elementAt(index);
    }

    public synchronized Object firstElement() {
        return v.firstElement();
    }

    public synchronized Object lastElement() {
        return v.lastElement();
    }

    public synchronized void setElementAt(Object obj, int index) {
        v.setElementAt(obj, index);
    }

    public synchronized void removeElementAt(int index) {
        throw new UnsupportedOperationException();
    }

    public synchronized void insertElementAt(Object obj, int index) {
        throw new UnsupportedOperationException();
    }

    public synchronized void addElement(Object obj) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean removeElement(Object obj) {
        throw new UnsupportedOperationException();
    }

    public synchronized void removeAllElements() {
        throw new UnsupportedOperationException();
    }

    public synchronized Object clone() {
        return v.clone();
    }

    public synchronized Object[] toArray() {
        return v.toArray();
    }

    public synchronized Object[] toArray(Object[] a) {
        return v.toArray(a);
    }

    public synchronized Object get(int index) {
        return v.get(index);
    }

    public synchronized Object set(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean add(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public void add(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    public synchronized Object remove(int index) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean containsAll(Collection c) {
        return v.containsAll(c);
    }

    public synchronized boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean addAll(int index, Collection c) {
        throw new UnsupportedOperationException();
    }

    public synchronized boolean equals(Object o) {
        return v.equals(o);
    }

    public synchronized int hashCode() {
        return v.hashCode();
    }

    public synchronized String toString() {
        return v.toString();
    }

    public synchronized java.util.List subList(int fromIndex, int toIndex) {
        return v.subList(fromIndex, toIndex);
    }

    public ListIterator listIterator() {
        return new ListIterator() {
            ListIterator i = v.listIterator();

            public boolean hasNext() {return i.hasNext();}
            public Object next() 	 {return i.next();}

            public boolean hasPrevious() {
                return i.hasPrevious();
            }

            public Object previous() {
                return i.previous();
            }

            public int nextIndex() {
                return i.nextIndex();
            }

            public int previousIndex() {
                return i.previousIndex();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void set(Object o) {
                throw new UnsupportedOperationException();
            }

            public void add(Object o) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public ListIterator listIterator(final int index) {
        return new ListIterator() {
            ListIterator i = v.listIterator(index);

            public boolean hasNext() {return i.hasNext();}
            public Object next() 	 {return i.next();}

            public boolean hasPrevious() {
                return i.hasPrevious();
            }

            public Object previous() {
                return i.previous();
            }

            public int nextIndex() {
                return i.nextIndex();
            }

            public int previousIndex() {
                return i.previousIndex();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

            public void set(Object o) {
                throw new UnsupportedOperationException();
            }

            public void add(Object o) {
                throw new UnsupportedOperationException();
            }
        };
    }


    public Iterator iterator() {
        return new Iterator() {
            Iterator i = v.iterator();

            public boolean hasNext() {return i.hasNext();}
            public Object next() 	 {return i.next();}
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}

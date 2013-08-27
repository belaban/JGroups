package org.jgroups.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator over an array of elements of type T.
 * @author Bela Ban
 * @since  3.4
 */
public class ArrayIterator<T> implements Iterator<T> {
    protected int       index=0;
    protected final T[] elements;

    public ArrayIterator(T[] elements) {
        this.elements=elements;
    }

    public boolean hasNext() {
        return index < elements.length;
    }

    public T next() {
        if(index >= elements.length)
            throw new NoSuchElementException("index=" + index + ", length=" + elements.length);
        return elements[index++];
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

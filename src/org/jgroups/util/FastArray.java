package org.jgroups.util;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple <pre>unsynchronized</pre> array. The array can only grow, but never shrinks (no arraycopy()). Elements
 * are removed by nulling them. A size variable is maintained for quick size() / isEmpty().
 * @author Bela Ban
 * @since  5.2
 */
public class FastArray<T> implements Iterable<T> {
    protected T[] elements;
    protected int index; // position at which the next element is inserted; only increments, never decrements
    protected int size;  // size: basically index - null elements
    protected int increment=5;
    protected int print_limit=20; // max numnber of elements to be printed in toString

    public FastArray(int capacity) {
        elements=(T[])new Object[capacity];
    }

    public int          capacity()        {return elements.length;}
    public int          index()           {return index;}
    public int          size()            {return size;}
    public boolean      isEmpty()         {return size == 0;}
    public int          increment()       {return increment;}
    public FastArray<T> increment(int i)  {this.increment=ensurePositive(i); return this;}
    public int          printLimit()      {return print_limit;}
    public FastArray<T> printLimit(int l) {this.print_limit=l; return this;}


    public int add(T el) {
        return add(el, true);
    }

    public int add(T el, boolean resize) {
        if(el == null)
            return 0;
        if(index == elements.length) {
            if(!resize)
                return 0;
            resize(index + increment);
        }
        elements[index++]=el;
        size++;
        return 1;
    }

    /**
     * Adds elements from an array els to this array
     * @param els The other array, can have null elements
     * @param length The number of elements to add. must be <= els.length
     * @return The number of elements added
     */
    public int add(T[] els, int length) {
        if(els == null)
            return 0;
        if(length > els.length)
            length=els.length;
        if(index + length > elements.length)
            resize(index + length + increment);
        System.arraycopy(els, 0, elements, index, length);
        int added=0, end_index=index+length;
        while(index < end_index) {
            if(elements[index++] != null)
                added++;
        }
        size+=added;
        return added;
    }

    @SafeVarargs
    public final int add(T... els) {
        return els == null? 0 : add(els, els.length);
    }

    public int add(Collection<T> list) {
        if(list == null)
            return 0;
        int list_size=list.size();
        if(index + list_size > elements.length)
            resize(index + list_size + increment);
        int old_size=size;
        for(T el: list) {
            if(el != null) {  // null elements need to be handled
                elements[index++]=el;
                size++;
            }
        }
        return size - old_size;
    }

    public int add(FastArray<T> fa) {
        return add(fa, true);
    }

    public int add(FastArray<T> fa, boolean resize) {
        if(fa == null)
            return 0;
        if(this == fa)
            throw new IllegalArgumentException("cannot add FastArray to itself");
        int fa_size=fa.size();
        if(index+fa_size > elements.length && resize)
            resize(index + fa_size + increment);
        int old_size=size;
        for(T el: fa) {
            if(index >= elements.length)
                return size-old_size;
            elements[index++]=el; // no need to handle null elements; the iterator skips null elements
            size++;
        }
        return size-old_size;
    }

    /**
     * Copies the messages from the other array into this one, <pre>including</pre> null elements
     * (using {@link System#arraycopy(Object, int, Object, int, int)}. This is the same as calling {@link #clear(boolean)}
     * followed by {@link #add(FastArray, boolean)}, but supposedly faster.
     * @param other The other array
     * @param clear Clears the other array after the transfer when true
     * @return The number of non-null elements transferred from other
     */
    public int transferFrom(FastArray<T> other, boolean clear) {
        if(other == null || this == other)
            return 0;
        int capacity=elements.length, other_size=other.size(), other_capacity=other.capacity();
        if(other_size == 0)
            return 0;
        if(capacity < other_capacity)
            elements=Arrays.copyOf(other.elements, other_capacity);
        else
            System.arraycopy(other.elements, 0, this.elements, 0, other_capacity);
        if(this.index > other.index)
            for(int i=other.index; i < this.index; i++)
                elements[i]=null;
        this.index=other.index;
        this.size=other_size; // not all other.elements may have been 0
        if(clear)
            other.clear(true);
        return other_size;
    }

    public T get(int idx) {
        if(idx < 0 || idx >= this.index)
            return null;
        return elements[idx];
    }

    public FastArray<T> set(int idx, T el) {
        if(idx < 0 || idx >= this.index)
            return this;
        T old_el=elements[idx];
        if(old_el == null) {
            if(el != null)
                size++;
        }
        else {
            if(el == null)
                size--;
        }
        elements[idx]=el;
        return this;
    }

    public FastArray<T> set(T[] elements) {
        this.elements=Objects.requireNonNull(elements);
        index=elements.length;
        size=count();
        return this;
    }

    public boolean anyMatch(Predicate<T> pred) {
        if(pred == null)
            return false;
        for(Iterator<T> it=iterator(); it.hasNext();) {
            T el=it.next();
            if(el != null && pred.test(el))
                return true;
        }
        return false;
    }


    public FastArray<T> remove(int idx) {
        return set(idx, null);
    }

    public FastArray<T> removeIf(Predicate<T> filter, boolean replace_all) {
        return replaceIf(filter, null, replace_all);
    }

    /**
     * Replaces any or all elements matching filter with a new element
     * @param filter The filter, must ne non-null or no replacements will take place
     * @param new_el The new element, can be null
     * @param replace_all When false, the method returns after the first match (if any). Otherwise, all matching
     *                    elements are replaced
     */
    public FastArray<T> replaceIf(Predicate<T> filter, T new_el, boolean replace_all) {
        if(filter == null)
            return this;
        for(FastIterator it=iteratorWithFilter(filter); it.hasNext();) {
            it.next();
            it.replace(new_el);
            if(!replace_all)
                break;
        }
        return this;
    }

    public FastArray<T> clear(boolean null_elements) {
        if(null_elements)
            for(int i=0; i < index; i++)
                elements[i]=null;
        index=size=0;
        return this;
    }


    /** Iterator which iterates only over non-null elements, skipping null elements */
    public FastIterator iterator() {
        return new FastIterator(null);
    }

    /** Iterates over all non-null elements which match filter */
    public FastIterator iteratorWithFilter(Predicate<T> filter) {
        return new FastIterator(filter);
    }

    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size, 0);
        return StreamSupport.stream(sp, false);
    }



    /** Returns the number of non-null elements, should have the same result as size(). Only used for testing! */
    public int count() {
        int cnt=0;
        for(int i=0; i < index; i++) {
            if(elements[i] != null)
                cnt++;
        }
        return cnt;
    }

    public String toString() {
        return String.format("%d elements (cap=%d): [%s]", size, capacity(), print());
    }

    public String print() {
        return print(print_limit);
    }



    public FastArray<T> resize(int new_capacity) {
        if(new_capacity <= elements.length)
            return this;
        elements=Arrays.copyOf(elements, new_capacity);
        return this;
    }


    protected String print(int limit) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        int count=0;
        for(T el: elements) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(el);
            if(limit > 0 && ++count >= limit)
                return sb.append(" ...").toString();
        }
        return sb.toString();
    }

    protected static int ensurePositive(int i) {
        if(i < 1)
            throw new IllegalArgumentException("value needs to be >= 1");
        return i;
    }


    public class FastIterator implements Iterator<T> {
        protected int                current_index=-1;
        protected final Predicate<T> filter;
        protected int                hit_count; // number of non-null elements

        public FastIterator(Predicate<T> filter) {
            this.filter=filter;
        }

        public boolean hasNext() {
            // skip null msgs or msgs that don't match the filter (if set)
            while(current_index +1 < index && hit_count < size && nullOrNoFilterMatch(current_index+1))
                current_index++;
            return current_index +1 < index && hit_count < size;
        }

        public T next() {
            if(current_index +1 >= index)
                throw new NoSuchElementException();
            hit_count++;
            return elements[++current_index];
        }

        public void remove() {
            replace(null);
        }

        public void replace(T el) {
            if(current_index >= 0) {
                int old_size=size;
                set(current_index, el);
                if(size < old_size)
                    hit_count=Math.max(hit_count-1, 0);
            }
        }

        public int currentIndex() {return current_index;}
        public int hitCount()     {return hit_count;}

        protected boolean nullOrNoFilterMatch(int index) {
            if(elements[index] == null)
                return true;
            boolean result=filter != null && filter.test(elements[index]) == false;
            if(result)
                hit_count++;
            return result;
        }

        public String toString() {
            return String.format("curr-index=%d hit-count=%d", current_index, hit_count);
        }
    }



}

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
public class FastArray<T> implements Iterable<T>, List<T> {
    protected T[] elements;
    protected int index; // position at which the next element is inserted; only increments, never decrements
    protected int size;  // size: basically index - null elements
    protected int increment; // if 0, use the built-in resize (new capacity: old capacity * 1.5)
    protected int print_limit=20; // max numnber of elements to be printed in toString

    public FastArray() {
        this(16);
    }

    public FastArray(int capacity) {
        elements=(T[])new Object[capacity];
    }

    public FastArray(T[] elements, int index) {
        this.elements=Objects.requireNonNull(elements);
        this.index=index;
        this.size=count();
    }

    public FastArray(Collection<? extends T> c) {
        this(c != null? c.size() : 16);
        addAll(c);
    }

    public int          capacity()        {return elements.length;}
    public int          index()           {return index;}
    public int          size()            {return size;}
    public boolean      isEmpty()         {return size == 0;}
    public int          increment()       {return increment;}
    public FastArray<T> increment(int i)  {this.increment=i; return this;}
    public int          printLimit()      {return print_limit;}
    public FastArray<T> printLimit(int l) {this.print_limit=l; return this;}

    @Override
    public boolean add(T el) {
        return add(el, true);
    }

    public boolean add(T el, boolean resize) {
        if(el == null)
            return false;
        if(index == elements.length) {
            if(!resize)
                return false;
            resize(index +1);
        }
        elements[index++]=el;
        size++;
        return true;
    }

    @Override
    public void add(int idx, T el) {
        checkIndex(idx);
        if(index+1 > elements.length)
            resize(index +1);

        System.arraycopy(elements, idx,
                         elements, idx+1,
                         index - idx);
        elements[idx]=el;
        if(el != null)
            size++;
        index++;
    }

    /**
     * Adds elements from an array els to this array
     * @param els The other array, can have null elements
     * @param length The number of elements to add. must be {@literal <= els.length}
     * @return The number of elements added
     */
    public boolean addAll(T[] els, int length) {
        if(els == null)
            return false;
        if(length > els.length)
            length=els.length;
        if(index + length > elements.length)
            resize(index + length);
        System.arraycopy(els, 0, elements, index, length);
        int added=0, end_index=index+length;
        while(index < end_index) {
            if(elements[index++] != null)
                added++;
        }
        size+=added;
        return true;
    }

    @SafeVarargs
    public final boolean addAll(T... els) {
        return els != null && addAll(els, els.length);
    }

    @Override
    public boolean addAll(Collection<? extends T> list) {
        if(list == null)
            return false;
        int list_size=list.size();
        if(index + list_size > elements.length)
            resize(index + list_size);
        int old_size=size;
        for(T el: list) {
            if(el != null) {  // null elements need to be handled
                elements[index++]=el;
                size++;
            }
        }
        return size > old_size;
    }

    public boolean addAll(FastArray<T> fa) {
        return addAll(fa, true);
    }

    public boolean addAll(FastArray<T> fa, boolean resize) {
        if(fa == null)
            return false;
        if(this == fa)
            throw new IllegalArgumentException("cannot add FastArray to itself");
        int fa_size=fa.size();
        if(index+fa_size > elements.length && resize)
            resize(index + fa_size);
        int old_size=size;
        for(T el: fa) {
            if(index >= elements.length)
                return size > old_size;
            elements[index++]=el; // no need to handle null elements; the iterator skips null elements
            size++;
        }
        return size > old_size;
    }

    @Override
    public boolean addAll(int idx, Collection<? extends T> c) {
        checkIndex(idx);
        if(c == null || c.isEmpty())
            return false;
        int old_size=size;
        int new_elements=c.size();
        if(index + new_elements > elements.length)
            resize(index + new_elements); // no increment here

        int elements_to_move=index - idx;
        if(elements_to_move > 0)
            System.arraycopy(elements, idx,
                             elements, idx + new_elements,
                             elements_to_move);

        Iterator<? extends T> it=c.iterator();
        for(int i=0; i < new_elements; i++) {
            T el=it.next();
            elements[idx+i]=el;
            if(el != null)
                size++;
        }
        index+=new_elements;
        return size != old_size;
    }


    /**
     * Copies the messages from the other array into this one, <pre>including</pre> null elements
     * (using {@link System#arraycopy(Object, int, Object, int, int)}. This is the same as calling
     * {@link #clear(boolean)} followed by {@link #addAll(FastArray, boolean)}, but faster.
     * @param other The other array
     * @param clear Clears the other array after the transfer when true
     * @return The number of non-null elements transferred from other
     */
    public int transferFrom(FastArray<T> other, boolean clear) {
        if(other == null || this == other || other.isEmpty())
            return 0;
        int capacity=elements.length, other_index=other.index(), other_size=other.size();
        if(capacity < other_index)
            elements=Arrays.copyOf(other.elements, other_index);
        else
            System.arraycopy(other.elements, 0, this.elements, 0, other_index);
        if(this.index > other.index)
            for(int i=other.index; i < this.index; i++)
                elements[i]=null;
        this.index=other.index;
        this.size=other_size; // not all other.elements may have been 0
        if(clear)
            other.clear(true);
        return other_size;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object e : c)
            if(!contains(e))
                return false;
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this)
            return true;

        if(!(obj instanceof List))
            return false;

        return (obj.getClass() == FastArray.class)?
          equalsArrayList((FastArray<?>)obj)
          : equalsRange((List<?>)obj, 0, size);
    }

    @Override
    public int indexOf(Object o) {
        for(int i=0; i < index; i++) {
            T el=elements[i];
            if(el != null) {
                if(Objects.equals(el, o))
                    return i;
            }
            else if(o == null)
                return i;
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        for(int i=index-1; i >= 0; i--) {
            T el=elements[i];
            if(el != null) {
                if(Objects.equals(el, o))
                    return i;
            }
            else if(o == null)
                return i;
        }
        return -1;
    }

    public boolean anyMatch(Predicate<T> pred) {
        if(pred == null)
            return false;
        for(int i=0; i < index; i++) {
            T el=elements[i];
            if(el != null && pred.test(el))
                return true;
        }
        return false;
    }

    @Override
    public T get(int idx) {
        if(idx < 0 || idx >= this.index)
            return null;
        return elements[idx];
    }

    @Override
    public T set(int idx, T el) {
        if(idx < 0 || idx >= this.index)
            return null;
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
        return old_el;
    }

    public FastArray<T> set(T[] elements) {
        this.elements=Objects.requireNonNull(elements);
        index=elements.length;
        size=count();
        return this;
    }

    @Override
    public T remove(int idx) {
        return set(idx, null);
    }

    @Override
    public boolean remove(Object o) {
        int idx=indexOf(o);
        if(idx >= 0) {
            remove(idx);
            return true;
        }
        return false;
    }

    /** Removes the first non-null element starting at index 0 */
    //@Override // uncomment when Java 21 is baseline
    public T removeFirst() {
        if(this.isEmpty())
            throw new NoSuchElementException();
        for(int i=0; i < index; i++) {
            if(elements[i] != null) {
                T el=elements[i];
                elements[i]=null;
                size--;
                return el;
            }
        }
        return null;
    }

    //@Override // uncomment when Java 21 is baseline
    public T removeLast() {
        if(this.isEmpty())
            throw new NoSuchElementException();
        for(int i=index-1; i >= 0; i--) {
            if(elements[i] != null) {
                T el=elements[i];
                elements[i]=null;
                size--;
                index=i;
                return el;
            }
        }
        return null;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        if(c == null || c.isEmpty())
            return false;
        boolean removed=false;
        for(Object obj: c)
            if(remove(obj))
                removed=true;
        return removed;
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
        for(FastIterator it=iterator(filter); it.hasNext();) {
            it.next();
            int saved_cursor=it.cursor, saved_last_idx=it.last_idx;
            it.replace(new_el);
            if(!replace_all)
                break;
            it.cursor=saved_cursor;
            it.last_idx=saved_last_idx;
        }
        return this;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        int old_size=size;
        if(c == null || c.isEmpty())
            return false;
        replaceIf(el -> !c.contains(el), null, true);
        return size != old_size;
    }

    @Override
    public void clear() {
        clear(true);
    }

    public FastArray<T> clear(boolean null_elements) {
        if(null_elements)
            for(int i=0; i < index; i++)
                elements[i]=null;
        index=size=0;
        return this;
    }

    /**
     * Attempts to reduce the current capacity to new_capacity
     * @param new_capacity The new capacity. If greater than the current capacity, this will be a no-op. If smaller
     *                     than the current size, the current size will be taken instead as new capacity.
     * @return
     */
    public FastArray<T> trimTo(int new_capacity) {
        if(new_capacity >= elements.length)
            return this;
        if(new_capacity <= index)
            return this;
        this.elements=Arrays.copyOf(elements, new_capacity);
        return this;
    }

    /** Iterator which iterates only over non-null elements, skipping null elements */
    public FastIterator iterator() {
        return new FastIterator(null);
    }

    /** Iterates over all non-null elements which match filter */
    public FastIterator iterator(Predicate<T> filter) {
        return new FastIterator(filter);
    }

    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size, 0);
        return StreamSupport.stream(sp, false);
    }

    @Override
    public ListIterator<T> listIterator() {
        return new FastListIterator(null);
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return new FastListIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOf(elements, index);
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return (T1[])Arrays.copyOf(elements, index, a.getClass());
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
        return String.format("%d elements: [%s]", size, print());
    }

    public String print() {
        return print(print_limit);
    }

    public FastArray<T> resize(int new_capacity) {
        if(new_capacity <= elements.length)
            return this;
        int new_cap;
        if(increment > 0)
            new_cap=new_capacity+increment;
        else {
            int old_capacity=elements.length;
            int min_growth=new_capacity - old_capacity;
            int preferred_growth=old_capacity >> 1; // 50% of the old capacity
            new_cap=old_capacity + Math.max(min_growth, preferred_growth);
        }
        elements=Arrays.copyOf(elements, new_cap);
        return this;
    }

    protected boolean equalsArrayList(FastArray<?> other) {
        if(size != other.size())
            return false;
        final Object[] other_elements=other.elements;
        final Object[] es=this.elements;
        for(int i=0; i < size; i++)
            if(!Objects.equals(es[i], other_elements[i]))
                return false;
        return true;
    }

    protected boolean equalsRange(List<?> other, int from, int to) {
        final Object[] es=elements;
        var oit=other.iterator();
        for(; from < to; from++) {
            if(!oit.hasNext() || !Objects.equals(es[from], oit.next()))
                return false;
        }
        return !oit.hasNext();
    }

    protected String print(int limit) {
        boolean first=true;
        StringBuilder sb=new StringBuilder();
        int count=0;
        for(int i=0; i < index; i++) {
            T el=elements[i];
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

    protected int checkIndex(int idx) {
        if(idx > index || idx < 0)
            throw new IndexOutOfBoundsException(String.format("0 >= idx (%d) < index (%d)", idx, index));
        return idx;
    }


    // FastIterator is public in order to be used by unit tests (e.g. hitCount() or currentIndex())
    public class FastIterator implements Iterator<T> {
        protected int                cursor;
        protected int                last_idx=-1; // index of last element returned; -1 if no such
        protected final Predicate<T> filter;
        protected int                hit_count;   // number of non-null elements

        public FastIterator(Predicate<T> filter) {
            this.filter=filter;
        }

        public boolean hasNext() {
            // skip null msgs or msgs that don't match the filter (if set)
            while(cursor < index && hit_count < size && nullOrNoFilterMatch(cursor))
                cursor++;
            return cursor < index && hit_count < size;
        }

        public T next() {
            int i=cursor;
            if(i >= index)
                throw new NoSuchElementException();
            if(i >= elements.length)
                throw new ConcurrentModificationException();
            cursor=i+1;
            hit_count++;
            return elements[last_idx=i];
        }

        public void remove() {
            if(last_idx < 0)
                throw new IllegalStateException();
            replace(null);
        }

        public void replace(T el) {
            if(last_idx < 0)
                throw new IllegalStateException();
            int old_size=size;
            set(last_idx, el);
            if(size < old_size)
                hit_count=Math.max(hit_count-1, 0);
        }

        public int cursor() {return cursor;}
        public int hitCount()     {return hit_count;}

        protected boolean nullOrNoFilterMatch(int idx) {
            if(elements[idx] == null)
                return true;
            boolean result=filter != null && filter.test(elements[idx]) == false;
            if(result)
                hit_count++;
            return result;
        }

        public String toString() {
            return String.format("cursor=%d hit-count=%d", cursor, hit_count);
        }
    }

    public class FastListIterator extends FastIterator implements ListIterator<T> {

        public FastListIterator(int idx) {
            this(null, idx);
        }

        public FastListIterator(Predicate<T> filter) {
            this(filter, 0);
        }

        public FastListIterator(Predicate<T> filter, int idx) {
            super(filter);
            cursor=checkIndex(idx);
        }

        @Override
        public boolean hasPrevious() {
            return cursor != 0;
        }

        @Override
        public T previous() {
            int i=cursor-1;
            if(i < 0)
                throw new NoSuchElementException();
            if(i >= elements.length)
                throw new ConcurrentModificationException();
            cursor=i;
            return elements[last_idx=i];
        }

        @Override
        public int nextIndex() {
            return cursor;
        }

        @Override
        public int previousIndex() {
            return cursor-1;
        }

        @Override
        public void set(T el) {
            if(last_idx < 0)
                throw new IllegalStateException();
            FastArray.this.set(last_idx, el);
        }

        @Override
        public void add(T el) {
            int i=cursor;
            FastArray.this.add(i, el);
            cursor=i+1;
            last_idx=-1;
        }
    }


}

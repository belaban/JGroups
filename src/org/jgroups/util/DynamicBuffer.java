package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Copy of {@link Table}. Implementation of {@link Buffer )}, expanding and shrinking dynamically.
 * <br/>
 * A store for elements (typically messages) to be retransmitted or delivered. Used on sender and receiver side.
 * <br/>
 * DynamicBuffer maintains a matrix of elements, which are stored by mapping their seqno to an index.
 * E.g. when we have 10 rows of 1000 elements each, and first_seqno is 3000, then an element with seqno=5600, will
 * be stored in the 3rd row, at index 600.
 * <br/>
 * Rows are removed when all elements in that row have been delivered.
 * <br/>
 * @author Bela Ban
 * @version 3.1
 */
public class DynamicBuffer<T> extends Buffer<T> {
    protected final int            num_rows;
    /** Must be a power of 2 for efficient modular arithmetic **/
    protected final int            elements_per_row;
    protected final double         resize_factor;
    protected T[][]                matrix;

    /** Time (in nanoseconds) after which a compaction should take place. 0 disables compaction */
    protected long                 max_compaction_time=NANOSECONDS.convert(DEFAULT_MAX_COMPACTION_TIME, MILLISECONDS);

    /** The time when the last compaction took place. If a {@link #compact()} takes place and sees that the
     * last compaction is more than max_compaction_time nanoseconds ago, a compaction will take place */
    protected long                 last_compaction_timestamp;

    protected int                  num_compactions, num_resizes, num_moves, num_purges;

    protected static final long    DEFAULT_MAX_COMPACTION_TIME=10000; // in milliseconds

    protected static final double  DEFAULT_RESIZE_FACTOR=1.2;


    public DynamicBuffer() {
        this(5, 8192, 0, DEFAULT_RESIZE_FACTOR);
    }

    public DynamicBuffer(long offset) {
        this();
        this.offset=this.low=this.high=this.hd=offset;
    }

    public DynamicBuffer(int num_rows, int elements_per_row, long offset) {
        this(num_rows,elements_per_row, offset, DEFAULT_RESIZE_FACTOR);
    }

    public DynamicBuffer(int num_rows, int elements_per_row, long offset, double resize_factor) {
        this(num_rows,elements_per_row, offset, resize_factor, DEFAULT_MAX_COMPACTION_TIME);
    }

    /**
     * Creates a new table
     * @param num_rows the number of rows in the matrix
     * @param elements_per_row the number of elements per row
     * @param offset the seqno before the first seqno to be inserted. E.g. if 0 then the first seqno will be 1
     * @param resize_factor teh factor with which to increase the number of rows
     * @param max_compaction_time the max time in milliseconds after we attempt a compaction
     */
    public DynamicBuffer(int num_rows, int elements_per_row, long offset, double resize_factor, long max_compaction_time) {
        this.num_rows=num_rows;
        this.elements_per_row=Util.getNextHigherPowerOfTwo(elements_per_row);
        this.resize_factor=resize_factor;
        this.max_compaction_time=NANOSECONDS.convert(max_compaction_time, MILLISECONDS);
        this.offset=this.low=this.high=this.hd=offset;
        matrix=(T[][])new Object[num_rows][];
        if(resize_factor <= 1)
            throw new IllegalArgumentException("resize_factor needs to be > 1");
    }

    public int  getElementsPerRow()    {return elements_per_row;}

    /** Returns the total capacity in the matrix */
    @Override
    public int  capacity()             {return matrix.length * elements_per_row;}
    public int  getNumRows()           {return matrix.length;}
    public int  getNumCompactions()    {return num_compactions;}
    public int  getNumMoves()          {return num_moves;}
    public int  getNumResizes()        {return num_resizes;}
    public int  getNumPurges()         {return num_purges;}
    public long getMaxCompactionTime() {return max_compaction_time;}

    public DynamicBuffer<T> setMaxCompactionTime(long max_compaction_time) {
        this.max_compaction_time=NANOSECONDS.convert(max_compaction_time, MILLISECONDS);
        return this;
    }

    @Override
    public void resetStats() {
        num_compactions=num_moves=num_resizes=num_purges=0;
    }

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     *
     * @param seqno
     * @param element
     * @param remove_filter If not null, a filter used to remove all consecutive messages passing the filter
     * @return True if the element at the computed index was null, else false
     */
    @Override
    public boolean add(long seqno, T element, Predicate<T> remove_filter) {
        lock.lock();
        try {
            if(seqno - hd <= 0)
                return false;

            int row_index=computeRow(seqno);
            if(row_index >= matrix.length) {
                resize(seqno);
                row_index=computeRow(seqno);
            }
            T[] row=getRow(row_index);
            int index=computeIndex(seqno);
            T existing_element=row[index];
            if(existing_element != null)
                return false;
            row[index]=element;
            size++;
            if(seqno - high > 0)
                high=seqno;
            if(remove_filter != null && seqno-hd > 0) {
                Visitor<T> v=(seq,msg) -> {
                    if(msg == null || !remove_filter.test(msg))
                        return false;
                    if(seq - hd > 0)
                        hd=seq;
                    size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
                    return true;
                };
                forEach(v, false);
            }
            return true;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Adds all messages from the given batch to the table
     * @param batch The batch
     * @param seqno_getter A function to return the sequence number (seqno) of a given Message. Must be non-null. If
     *                     the function return -1, then the message won't be added
     * @param remove_from_batch If true, the message is removed from the batch
     * @param const_value If non-null, this value should be used rather than the values of the list tuples
     * @return True if at least 1 element was added successfully, false otherwise.
     */
    @Override
    public boolean add(MessageBatch batch, Function<T,Long> seqno_getter, boolean remove_from_batch, T const_value) {
        if(batch == null || batch.isEmpty())
            return false;
        Objects.requireNonNull(seqno_getter);
        boolean retval=false;
        // find the highest seqno (unfortunately, the list is not ordered by seqno)
        long highest_seqno=findHighestSeqno(batch, seqno_getter);
        lock.lock();
        try {
            if(highest_seqno != -1 && computeRow(highest_seqno) >= matrix.length)
                resize(highest_seqno);

            for(Iterator<?> it=batch.iterator(); it.hasNext();) {
                T msg=(T)it.next();
                long seqno=seqno_getter.apply(msg);
                if(seqno < 0)
                    continue;
                T element=const_value != null? const_value : msg;
                boolean added=add(seqno, element, null);
                retval=retval || added;
                if(!added || remove_from_batch)
                    it.remove();
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    public boolean add(final List<LongTuple<T>> list, boolean remove_added_elements, T const_value) {
        if(list == null || list.isEmpty())
            return false;
        boolean added=false;
        // find the highest seqno (unfortunately, the list is not ordered by seqno)
        long highest_seqno=findHighestSeqno(list);
        lock.lock();
        try {
            if(highest_seqno != -1 && computeRow(highest_seqno) >= matrix.length)
                resize(highest_seqno);

            for(Iterator<LongTuple<T>> it=list.iterator(); it.hasNext();) {
                LongTuple<T> tuple=it.next();
                long seqno=tuple.getVal1();
                T element=const_value != null? const_value : tuple.getVal2();
                if(add(seqno, element, null))
                    added=true;
                else if(remove_added_elements)
                    it.remove();
            }
            return added;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns an element at seqno
     * @param seqno
     * @return
     */
    @Override
    public T get(long seqno) {
        lock.lock();
        try {
            if(seqno - low <= 0 || seqno - high > 0)
                return null;
            int row_index=computeRow(seqno);
            if(row_index < 0 || row_index >= matrix.length)
                return null;
            T[] row=matrix[row_index];
            if(row == null)
                return null;
            int index=computeIndex(seqno);
            return index >= 0? row[index] : null;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * To be used only for testing; doesn't do any index or sanity checks
     * @param seqno
     * @return
     */
    @Override
    public T _get(long seqno) {
        lock.lock();
        try {
            int row_index=computeRow(seqno);
            if(row_index < 0 || row_index >= matrix.length)
                return null;
            T[] row=matrix[row_index];
            if(row == null)
                return null;
            int index=computeIndex(seqno);
            return index >= 0? row[index] : null;
        }
        finally {
            lock.unlock();
        }
    }

    /** Removes the next non-null element and nulls the index if nullify=true */
    @Override
    public T remove(boolean nullify) {
        lock.lock();
        try {
            int row_index=computeRow(hd+1);
            if(row_index < 0 || row_index >= matrix.length)
                return null;
            T[] row=matrix[row_index];
            if(row == null)
                return null;
            int index=computeIndex(hd+1);
            if(index < 0)
                return null;
            T existing_element=row[index];
            if(existing_element != null) {
                hd++;
                size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
                if(nullify) {
                    row[index]=null;
                    if(hd - low > 0)
                        low=hd;
                }
            }
            return existing_element;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public List<T> removeMany(boolean nullify, int max_results, Predicate<T> filter) {
        return removeMany(nullify, max_results, filter, LinkedList::new, LinkedList::add);
    }


    /**
     * Removes elements from the table and adds them to the result created by result_creator. Between 0 and max_results
     * elements are removed.
     * @param nullify if true, the removed element will be nulled
     * @param max_results the max number of results to be returned, even if more elements would be removable
     * @param filter a filter which accepts (or rejects) elements into the result. If null, all elements will be accepted
     * @param result_creator a supplier required to create the result, e.g. ArrayList::new
     * @param accumulator an accumulator accepting the result and an element, e.g. ArrayList::add
     * @param <R> the type of the result
     * @return the result
     */
    @Override
    public <R> R removeMany(boolean nullify, int max_results, Predicate<T> filter,
                            Supplier<R> result_creator, BiConsumer<R,T> accumulator) {
        lock.lock();
        try {
            Remover<R> remover=new Remover<R>(max_results, filter, result_creator, accumulator);
            forEach(remover, nullify);
            return remover.getResult();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes all elements less than or equal to seqno from the buffer. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed.
     * @param seqno All elements <= seqno will be nulled
     * @param force If true, we only ensure that seqno <= hr, but don't care about hd, and set hd=low=seqno.
     * @return 0. The number of purged elements
     */
    @Override
    public int purge(long seqno, boolean force) {
        int purged=0;
        lock.lock();
        try {
            if(seqno - low <= 0)
                return 0;
            if(force) {
                if(seqno - high > 0)
                    seqno=high;
            }
            else {
                if(seqno - hd > 0) // we cannot be higher than the highest removed seqno
                    seqno=hd;
            }

            int start_row=computeRow(low), end_row=computeRow(seqno);
            if(start_row < 0) start_row=0;
            if(end_row < 0)
                return purged;
            for(int i=start_row; i < end_row; i++) // Null all rows which can be fully removed
                matrix[i]=null;

            if(matrix[end_row] != null) {
                int index=computeIndex(seqno);
                for(int i=0; i <= index; i++) { // null all elements up to and including seqno in the given row
                    if(matrix[end_row][i] != null) {
                        matrix[end_row][i]=null;
                        purged++;
                    }
                }
            }
            if(seqno - low > 0)
                low=seqno;
            if(force) {
                if(seqno - hd > 0)
                    low=hd=seqno;
                size=computeSize();
            }
            num_purges++;
            if(max_compaction_time <= 0) // see if compaction should be triggered
                return purged;

            long current_time=System.nanoTime();
            if(last_compaction_timestamp > 0) {
                if(current_time - last_compaction_timestamp >= max_compaction_time) {
                    _compact();
                    last_compaction_timestamp=current_time;
                }
            }
            else // the first time we don't do a compaction
                last_compaction_timestamp=current_time;
            return purged;
        }
        finally {
            lock.unlock();
        }
    }

    public void compact() {
        lock.lock();
        try {
            _compact();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Iterates over the matrix with range [from .. to] (including from and to), and calls
     * {@link Visitor#visit(long, Object)}. If the visit() method returns false, the iteration is terminated.
     * <br/>
     * This method must be called with the lock held
     * @param from The starting seqno
     * @param to The ending seqno, the range is [from .. to] including from and to
     * @param visitor An instance of Visitor
     * @param nullify Nulls the visited element when true
     */
    @GuardedBy("lock")
    @Override
    public void forEach(long from, long to, Visitor<T> visitor, boolean nullify) {
        if(from - to > 0) // same as if(from > to), but prevents long overflow
            return;
        int row=computeRow(from), column=computeIndex(from);
        int distance=(int)(to - from +1);
        T[] current_row=row+1 > matrix.length? null : matrix[row];

        for(int i=0; i < distance; i++) {
            T element=current_row == null? null : current_row[column];
            boolean stop=visitor != null && !visitor.visit(from, element);
            if(nullify && element != null) {
                matrix[row][column]=null;
                // if we're nulling the last element of a row, null the row as well
                if(column == elements_per_row-1)
                    matrix[row]=null;
                if(from - low > 0)
                    low=from;
            }
            if(stop)
                break;
            from++;
            if(++column >= elements_per_row) {
                column=0;
                row++;
                current_row=row+1 > matrix.length? null : matrix[row];
            }
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new TableIterator();
    }

    @Override
    public Iterator<T> iterator(long from, long to) {
        return new TableIterator(from, to);
    }

    @Override
    public Stream<T> stream() {
        Spliterator<T> sp=Spliterators.spliterator(iterator(), size(), 0);
        return StreamSupport.stream(sp, false);
    }

     @Override
     public Stream<T> stream(long from, long to) {
         Spliterator<T> sp=Spliterators.spliterator(iterator(from, to), size(), 0);
         return StreamSupport.stream(sp, false);
    }

    // list must not be null or empty
    protected long findHighestSeqno(List<LongTuple<T>> list) {
        long seqno=-1;
        for(LongTuple<T> tuple: list) {
            long val=tuple.getVal1();
            if(val - seqno > 0)
                seqno=val;
        }
        return seqno;
    }

    protected static <T> long findHighestSeqno(MessageBatch batch, Function<T,Long> seqno_getter) {
        long seqno=-1;
        for(Iterator<?> it=batch.iterator(); it.hasNext();) {
            T msg=(T)it.next();
            long val=seqno_getter.apply(msg);
            if(val < 0)
                continue;
            if(val - seqno > 0)
                seqno=val;
        }
        return seqno;
    }

    /** Moves rows down the matrix, by removing purged rows. If resizing to accommodate seqno is still needed, computes
     * a new size. Then either moves existing rows down, or copies them into a new array (if resizing took place).
     * The lock must be held by the caller of resize(). */
    @GuardedBy("lock")
    protected void resize(long seqno) {
        int num_rows_to_purge=computeRow(low);
        int row_index=computeRow(seqno) - num_rows_to_purge;
        if(row_index < 0)
            return;

        int new_size=Math.max(row_index +1, matrix.length);
        if(new_size > matrix.length) {
            T[][] new_matrix=(T[][])new Object[new_size][];
            System.arraycopy(matrix, num_rows_to_purge, new_matrix, 0, matrix.length - num_rows_to_purge);
            matrix=new_matrix;
            num_resizes++;
        }
        else if(num_rows_to_purge > 0) {
            move(num_rows_to_purge);
        }

        offset+=((long)num_rows_to_purge * elements_per_row);
    }


    /** Moves contents of matrix num_rows down. Avoids a System.arraycopy(). Caller must hold the lock. */
    @GuardedBy("lock")
    protected void move(int num_rows) {
        if(num_rows <= 0 || num_rows > matrix.length)
            return;

        int target_index=0;
        for(int i=num_rows; i < matrix.length; i++)
            matrix[target_index++]=matrix[i];

        for(int i=matrix.length - num_rows; i < matrix.length; i++)
            matrix[i]=null;
        num_moves++;
    }


    /**
     * Moves the contents of matrix down by the number of purged rows and resizes the matrix accordingly. The
     * capacity of the matrix should be size * resize_factor. Caller must hold the lock.
     */
    @GuardedBy("lock")
    protected void _compact() {
        // This is the range we need to copy into the new matrix (including from and to)
        int from=computeRow(low), to=computeRow(high);
        int range=to - from +1;  // e.g. from=3, to=5, new_size has to be [3 .. 5] (=3)

        int new_size=(int)Math.max( (double)range * resize_factor, (double) range +1 );
        new_size=Math.max(new_size, num_rows); // don't fall below the initial size defined
        if(new_size < matrix.length) {
            T[][] new_matrix=(T[][])new Object[new_size][];
            System.arraycopy(matrix, from, new_matrix, 0, range);
            matrix=new_matrix;
            offset+=(long)from * elements_per_row;
            num_compactions++;
        }
    }

    /**
     * Returns a row. Creates a new row and inserts it at index if the row at index doesn't exist
     * @param index
     * @return A row
     */
    @GuardedBy("lock")
    protected T[] getRow(int index) {
        T[] row=matrix[index];
        if(row == null) {
            row=(T[])new Object[elements_per_row];
            matrix[index]=row;
        }
        return row;
    }


    /** Computes and returns the row index for seqno. The caller must hold the lock. */
    // Note that seqno-offset is never > Integer.MAX_VALUE and thus doesn't overflow into a negative long,
    // as offset is always adjusted in resize() or compact(). Even if it was negative, callers of computeRow() will
    // ignore the result or throw an ArrayIndexOutOfBound exception
    @GuardedBy("lock")
    protected int computeRow(long seqno) {
        int diff=(int)(seqno-offset);
        if(diff < 0) return diff;
        return diff / elements_per_row;
    }


    /** Computes and returns the index within a row for seqno */
    // Note that seqno-offset is never > Integer.MAX_VALUE and thus doesn't overflow into a negative long,
    // as offset is always adjusted in resize() or compact(). Even if it was negative, callers of computeIndex() will
    // ignore the result or throw an ArrayIndexOutOfBound exception
    @GuardedBy("lock")
    protected int computeIndex(long seqno) {
        int diff=(int)(seqno - offset);
        if(diff < 0)
            return diff;
        return diff & (elements_per_row - 1); // same as mod, but (apparently, I'm told) more efficient
    }


    /**
     * Iterates through all elements of the matrix. The range (from-to) can be defined, default is [hd+1 .. hr] (incl hr).
     * Matrix compactions and resizings will lead to undefined results, as this iterator doesn't maintain a separate ref
     * of the matrix, so it is best to run this with the lock held. This iterator is also used by {@link #stream()}
     */
    protected class TableIterator implements Iterator<T> {
        protected int         row, column;
        protected T[]         current_row;
        protected long        from;
        protected final long  to;

        protected TableIterator() {
            this(hd+1, high);
        }

        protected TableIterator(final long from, final long to) {
            this.from=from;
            this.to=to;
            row=computeRow(from);
            column=computeIndex(from);
            current_row=row+1 > matrix.length? null : matrix[row];
        }

        public boolean hasNext() {
            return to - from >= 0;
        }

        public T next() {
            if(row > matrix.length)
                throw new NoSuchElementException(String.format("row (%d) is > matrix.length (%d)", row, matrix.length));
            T element=current_row == null? null : current_row[column];
            from++;
            if(++column >= elements_per_row) {
                column=0;
                row++;
                current_row=row+1 > matrix.length? null : matrix[row];
            }
            return element;
        }
    }


    /*protected class Remover<R> implements Visitor<T> {
        protected final int          max_results;
        protected int                num_results;
        protected final Predicate<T> filter;
        protected R                  result;
        protected Supplier<R>        result_creator;
        protected BiConsumer<R,T>    result_accumulator;

        public Remover(int max_results, Predicate<T> filter, Supplier<R> creator, BiConsumer<R,T> accumulator) {
            this.max_results=max_results;
            this.filter=filter;
            this.result_creator=creator;
            this.result_accumulator=accumulator;
        }

        public R getResult() {return result;}

        @GuardedBy("lock")
        public boolean visit(long seqno, T element) {
            if(element == null)
                return false;
            if(filter == null || filter.test(element)) {
                if(result == null)
                    result=result_creator.get();
                result_accumulator.accept(result, element);
                num_results++;
            }
            size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
            if(seqno - hd > 0)
                hd=seqno;
            return max_results == 0 || num_results < max_results;
        }
    }
*/
}


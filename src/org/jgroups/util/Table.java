package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A store for elements (typically messages) to be retransmitted or delivered. Used on sender and receiver side,
 * as a replacement for HashMap. Table should use less memory than HashMap, as HashMap.Entry has 4 fields,
 * plus arrays for storage.
 * <p/>
 * Table maintains a matrix (an array of arrays) of elements, which are stored in the matrix by mapping
 * their seqno to an index. E.g. when we have 10 rows of 1000 elements each, and first_seqno is 3000, then an element
 * with seqno=5600, will be stored in the 3rd row, at index 600.
 * <p/>
 * Rows are removed when all elements in that row have been received.
 * <p/>
 * Table started out as a copy of RetransmitTable, but is synchronized and maintains its own low, hd and hr
 * pointers, so it can be used as a replacement for NakReceiverWindow. The idea is to put messages into Table,
 * deliver them in order of seqnos, and periodically scan over all tables in NAKACK2 to do retransmission.
 * <p/>
 * @author Bela Ban
 * @version 3.1
 */
public class Table<T> {
    protected final int            num_rows;
    protected final int            elements_per_row;
    protected final double         resize_factor;
    protected T[][]                matrix;

    /** The first seqno, at matrix[0][0] */
    protected long                 offset;

    protected int                  size;

    /** The highest seqno purged */
    protected long                 low;

    /** The highest received seqno */
    protected long                 hr;

    /** The highest delivered (= removed) seqno */
    protected long                 hd;

    /** Time (in nanoseconds) after which a compaction should take place. 0 disables compaction */
    protected long                 max_compaction_time=TimeUnit.NANOSECONDS.convert(DEFAULT_MAX_COMPACTION_TIME, TimeUnit.MILLISECONDS);

    /** The time when the last compaction took place. If a {@link #compact()} takes place and sees that the
     * last compaction is more than max_compaction_time nanoseconds ago, a compaction will take place */
    protected long                 last_compaction_timestamp=0;

    protected final Lock           lock=new ReentrantLock();

    protected final AtomicBoolean  processing=new AtomicBoolean(false);

    protected int                  num_compactions=0, num_resizes=0, num_moves=0, num_purges=0;
    
    protected static final long    DEFAULT_MAX_COMPACTION_TIME=10000; // in milliseconds

    protected static final double  DEFAULT_RESIZE_FACTOR=1.2;




    public interface Visitor<T> {
        /**
         * Iteration over the table, used by {@link Table#forEach(long,long,org.jgroups.util.Table.Visitor)}.
         * @param seqno The current seqno
         * @param element The element at matrix[row][column]
         * @param row The current row
         * @param column The current column
         * @return True if we should continue the iteration, false if we should break out of the iteration
         */
        boolean visit(long seqno, T element, int row, int column);
    }



    public Table() {
        this(5, 10000, 0, DEFAULT_RESIZE_FACTOR);
    }

    public Table(long offset) {
        this();
        this.offset=offset;
    }

    public Table(int num_rows, int elements_per_row, long offset) {
        this(num_rows,elements_per_row, offset, DEFAULT_RESIZE_FACTOR);
    }

    public Table(int num_rows, int elements_per_row, long offset, double resize_factor) {
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
    @SuppressWarnings("unchecked")
    public Table(int num_rows, int elements_per_row, long offset, double resize_factor, long max_compaction_time) {
        this.num_rows=num_rows;
        this.elements_per_row=elements_per_row;
        this.resize_factor=resize_factor;
        this.max_compaction_time=TimeUnit.NANOSECONDS.convert(max_compaction_time, TimeUnit.MILLISECONDS);
        this.offset=this.low=this.hr=this.hd=offset;
        matrix=(T[][])new Object[num_rows][];
        if(resize_factor <= 1)
            throw new IllegalArgumentException("resize_factor needs to be > 1");
    }


    public AtomicBoolean getProcessing() {return processing;}

    public long getOffset()              {return offset;}

    /** Returns the total capacity in the matrix */
    public int capacity()                {return matrix.length * elements_per_row;}

    public int getNumCompactions()       {return num_compactions;}
    public int getNumMoves()             {return num_moves;}
    public int getNumResizes()           {return num_resizes;}
    public int getNumPurges()            {return num_purges;}

    /** Returns the numbers of elements in the table */
    public int size()                    {return size;}
    public boolean isEmpty()             {return size <= 0;}
    public long getLow()                 {return low;}
    public long getHighestDelivered()    {return hd;}
    public long getHighestReceived()     {return hr;}
    public long getMaxCompactionTime()   {return max_compaction_time;}
    public void setMaxCompactionTime(long max_compaction_time) {
        this.max_compaction_time=TimeUnit.NANOSECONDS.convert(max_compaction_time, TimeUnit.MILLISECONDS);
    }
    public int  getNumRows()             {return matrix.length;}
    public void resetStats()             {num_compactions=num_moves=num_resizes=num_purges=0;}

    /** Returns the highest deliverable (= removable) seqno. This may be higher than {@link #getHighestDelivered()},
     * e.g. if elements have been added but not yet removed */
    public long getHighestDeliverable() {
        HighestDeliverable visitor=new HighestDeliverable();
        lock.lock();
        try {
            forEach(hd+1, hr, visitor);
            long retval=visitor.getResult();
            return retval == -1? hd : retval;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Only used internally by JGroups on a state transfer. Please don't use this in application code, or you're on
     * your own !
     * @param seqno
     */
    public void setHighestDelivered(long seqno) {
        lock.lock();
        try {
            hd=seqno;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Adds an element if the element at the given index is null. Returns true if no element existed at the given index,
     * else returns false and doesn't set the element.
     * @param seqno
     * @param element
     * @return True if the element at the computed index was null, else false
     */
    public boolean add(long seqno, T element) {
        lock.lock();
        try {
            return _add(seqno, element);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Adds messages from list to the table
     * @param list
     * @return True if at least 1 message was added successfully
     */
    public boolean add(final List<Tuple<Long,T>> list) {
       return add(list, false);
    }


    /**
     * Adds messages from list to the table, removes messages from list that were not added to the table
     * @param list
     * @return True if at least 1 message was added successfully. This guarantees that the list has at least 1 message
     */
    public boolean add(final List<Tuple<Long,T>> list, boolean remove_added_msgs) {
        if(list == null)
            return false;
        boolean added=false;
        lock.lock();
        try {
            for(Iterator<Tuple<Long,T>> it=list.iterator(); it.hasNext();) {
                Tuple<Long,T> tuple=it.next();
                long seqno=tuple.getVal1();
                T element=tuple.getVal2();
                if(_add(seqno, element))
                    added=true;
                else if(remove_added_msgs)
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
    public T get(long seqno) {
        lock.lock();
        try {
            if(seqno <= low || seqno > hr)
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



    public List<T> get(long from, long to) {
        lock.lock();
        try {
            // boundary check: the get() has to be in range [low+1 .. hr-1]
            if(from <= low) from=low+1;
            if(to > hr) to=hr;
            Getter getter=new Getter();
            forEach(from, to, getter);
            return getter.getList();
        }
        finally {
            lock.unlock();
        }
    }


    public T remove() {
        return remove(true);
    }

    /** Removes the next non-null element and nulls the index if nullify=true */
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
                    if(hd > low)
                        low=hd;
                }
            }
            return existing_element;
        }
        finally {
            lock.unlock();
        }
    }


    public List<T> removeMany(boolean nullify, int max_results) {
        return removeMany(null, nullify, max_results);
    }


    public List<T> removeMany(final AtomicBoolean processing, boolean nullify, int max_results) {
        lock.lock();
        try {
            Remover remover=new Remover(nullify, max_results);
            forEach(hd+1, hr, remover);
            List<T> retval=remover.getList();
            if(processing != null && (retval == null || retval.isEmpty()))
                processing.set(false);
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes all elements less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed
     * @param seqno
     */
    public void purge(long seqno) {
        purge(seqno, false);
    }

    /**
     * Removes all elements less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed.
     * @param seqno All elements <= seqno will be nulled
     * @param force If true, we only ensure that seqno <= hr, but don't care about hd, and set hd=low=seqno.
     */
    public void purge(long seqno, boolean force) {
        lock.lock();
        try {
            if(seqno <= low)
                return;
            if(force) {
                if(seqno > hr)
                    seqno=hr;
            }
            else {
                if(seqno > hd) // we cannot be higher than the highest removed seqno
                    seqno=hd;
            }

            int start_row=computeRow(low), end_row=computeRow(seqno);
            if(start_row < 0) start_row=0;
            if(end_row < 0)
                return;
            for(int i=start_row; i < end_row; i++) // Null all rows which can be fully removed
                matrix[i]=null;

            if(matrix[end_row] != null) {
                int index=computeIndex(seqno);
                for(int i=0; i <= index; i++) // null all elements up to and including seqno in the given row
                    matrix[end_row][i]=null;
            }
            if(seqno > low)
                low=seqno;
            if(force) {
                if(seqno > hd)
                    low=hd=seqno;
                size=computeSize();
            }
            num_purges++;
            if(max_compaction_time <= 0) // see if compaction should be triggered
                return;

            long current_time=System.nanoTime();
            if(last_compaction_timestamp > 0) {
                if(current_time - last_compaction_timestamp >= max_compaction_time) {
                    _compact();
                    last_compaction_timestamp=current_time;
                }
            }
            else // the first time we don't do a compaction
                last_compaction_timestamp=current_time;
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
     * {@link Visitor#visit(long,Object,int,int)}. If the visit() method returns false, the iteration is terminated.
     * <p/>
     * This method must be called with the lock held
     * @param from The starting seqno
     * @param to The ending seqno, the range is [from .. to] including from and to
     * @param visitor An instance of Visitor
     */
    @GuardedBy("lock")
    public void forEach(long from, long to, Visitor<T> visitor) {
        int row=computeRow(from), column=computeIndex(from);
        int distance=(int)(to - from +1);
        T[] current_row=row+1 > matrix.length? null : matrix[row];

        for(int i=0; i < distance; i++) {
            T element=current_row == null? null : current_row[column];
            if(!visitor.visit(from, element, row, column))
                break;

            from++;
            if(++column >= elements_per_row) {
                column=0;
                row++;
                current_row=row+1 > matrix.length? null : matrix[row];
            }
        }
    }

    protected boolean _add(long seqno, T element) {
        if(seqno <= hd)
            return false;

        int row_index=computeRow(seqno);
        if(row_index >= matrix.length) {
            resize(seqno);
            row_index=computeRow(seqno);
        }
        T[] row=getRow(row_index);
        int index=computeIndex(seqno);
        T existing_element=row[index];
        if(existing_element == null) {
            row[index]=element;
            size++;
            if(seqno > hr)
                hr=seqno;
            return true;
        }
        return false;
    }

    /** Moves rows down the matrix, by removing purged rows. If resizing to accommodate seqno is still needed, computes
     * a new size. Then either moves existing rows down, or copies them into a new array (if resizing took place).
     * The lock must be held by the caller of resize(). */
    @SuppressWarnings("unchecked")
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

        offset+=(num_rows_to_purge * elements_per_row);
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
    @SuppressWarnings("unchecked")
    @GuardedBy("lock")
    protected void _compact() {
        // This is the range we need to copy into the new matrix (including from and to)
        int from=computeRow(low), to=computeRow(hr);
        int range=to - from +1;  // e.g. from=3, to=5, new_size has to be [3 .. 5] (=3)

        int new_size=(int)Math.max(range * resize_factor, range +1);
        new_size=Math.max(new_size, num_rows); // don't fall below the initial size defined
        if(new_size < matrix.length) {
            T[][] new_matrix=(T[][])new Object[new_size][];
            System.arraycopy(matrix, from, new_matrix, 0, range);
            matrix=new_matrix;
            offset+=from * elements_per_row;
            num_compactions++;
        }
    }



    /** Iterate from low to hr and add up non-null values. Caller must hold the lock. */
    @GuardedBy("lock")
    public int computeSize() {
        Counter non_null_counter=new Counter();
        forEach(hd+1, hr, non_null_counter);
        return non_null_counter.getResult();
    }


    /** Returns the number of null elements in the range [hd+1 .. hr-1] excluding hd and hr */
    public int getNumMissing() {
        return (int)(hr-hd-size);
    }


    /**
     * Returns a list of missing (= null) messages
     * @return
     */
    public SeqnoList getMissing() {
        lock.lock();
        try {
            Missing missing=new Missing();
            forEach(hd+1, hr, missing);
            return missing.getMissingElements();
        }
        finally {
            lock.unlock();
        }
    }

    public long[] getDigest() {
        lock.lock();
        try {
            return new long[]{hd,hr};
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[" + low + " | " + hd + " | " + hr + "] (" + size() +
                    " elements, " + getNumMissing() + " missing)");
        return sb.toString();
    }


    /** Dumps the seqnos in the table as a list */
    public String dump() {
        lock.lock();
        try {
            Dump dump=new Dump();
            forEach(low, hr, dump);
            return dump.getResult();
        }
        finally {
            lock.unlock();
        }
    }



    /**
     * Returns a row. Creates a new row and inserts it at index if the row at index doesn't exist
     * @param index
     * @return A row
     */
    @SuppressWarnings("unchecked")
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
    @GuardedBy("lock")
    protected int computeRow(long seqno) {
        int diff=(int)(seqno-offset);
        if(diff < 0) return diff;
        return diff / elements_per_row;
    }


    /** Computes and returns the index within a row for seqno */
    @GuardedBy("lock")
    protected int computeIndex(long seqno) {
        int diff=(int)(seqno - offset);
        if(diff < 0)
            return diff;
        return diff % elements_per_row;
    }

    protected class Counter implements Visitor<T> {
        protected int           result=0;

        public int getResult() {return result;}

        public boolean visit(long seqno, T element, int row, int column) {
            if(element != null)
                result++;
            return true;
        }
    }


    protected class Getter implements Visitor<T> {
        protected List<T> list;

        public List<T> getList() {return list;}

        public boolean visit(long seqno, T element, int row, int column) {
            if(element != null) {
                if(list == null)
                    list=new LinkedList<T>();
                list.add(element);
            }
            return true;
        }
    }


    protected class Remover implements Visitor<T> {
        protected final boolean nullify;
        protected final int     max_results;
        protected List<T>       list;
        protected int           num_results;

        public Remover(boolean nullify, int max_results) {
            this.nullify=nullify;
            this.max_results=max_results;
        }

        public List<T> getList() {return list;}

        @GuardedBy("lock")
        public boolean visit(long seqno, T element, int row, int column) {
            if(element != null) {
                if(list == null)
                    list=new LinkedList<T>();
                list.add(element);
                if(seqno > hd)
                    hd=seqno;
                size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
                if(nullify) {
                    matrix[row][column]=null;
                    if(seqno > low)
                        low=seqno;
                }
                return max_results == 0 || ++num_results < max_results;
            }
            return false;
        }
    }

    protected class Dump implements Visitor<T> {
        protected final StringBuilder sb=new StringBuilder();
        protected boolean             first=true;

        protected String getResult() {return sb.toString();}

        @GuardedBy("lock")
        public boolean visit(long seqno, T element, int row, int column) {
            if(element != null) {
                if(first)
                    first=false;
                else
                    sb.append(", ");
                sb.append(seqno);
            }
            return true;
        }
    }

    protected class Missing implements Visitor<T> {
        protected SeqnoList missing_elements;
        protected long      last_missing=-1; // last missing seqno

        protected SeqnoList getMissingElements() {return missing_elements;}

        public boolean visit(long seqno, T element, int row, int column) {
            if(element == null) {
                if(last_missing == -1)
                    last_missing=seqno;
                else
                    ;
            }
            else {
                if(last_missing == -1) {
                    ;
                }
                else {
                    long tmp=seqno-1;
                    if(missing_elements == null)
                        missing_elements=new SeqnoList();
                    if(tmp - last_missing > 0) {
                        missing_elements.add(last_missing, tmp);
                    }
                    else {
                        missing_elements.add(last_missing);
                    }
                    last_missing=-1;
                }
            }
            return true;
        }
    }

    protected class HighestDeliverable implements Visitor<T> {
        protected long highest_deliverable=-1;

        public long getResult() {return highest_deliverable;}

        public boolean visit(long seqno, T element, int row, int column) {
            if(element == null)
                return false;
            highest_deliverable=seqno;
            return true;
        }
    }

}


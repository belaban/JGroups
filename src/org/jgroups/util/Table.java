package org.jgroups.util;

import org.jgroups.annotations.GuardedBy;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
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
 * deliver them in order of seqnos, and periodically scan over all Tables in NAKACK2 to do retransmission.
 * <p/>
 * @author Bela Ban
 */
// todo: trigger compaction if more than N rows can be removed; check on resize()
public class Table<T> {
    protected final int    num_rows;
    protected final int    elements_per_row;
    protected final double resize_factor;
    protected T[][]        matrix;

    /** The first seqno, at matrix[0][0] */
    protected long         offset;

    protected int          size;

    /** The highest seqno purged */
    protected long         low;

    /** The highest received seqno */
    protected long         hr;

    /** The highest delivered (= removed) seqno */
    protected long         hd;

    /** Time (in ms) after which a compaction should take place. 0 disables compaction */
    protected long         max_compaction_time=DEFAULT_MAX_COMPACTION_TIME;

    /** The time when the last compaction took place. If a {@link #compact()} takes place and sees that the
     * last compaction is more than max_compaction_time ms ago, a compaction will take place */
    protected long         last_compaction_timestamp=0;

    protected final Lock   lock=new ReentrantLock();

    
    protected static final long   DEFAULT_MAX_COMPACTION_TIME=2 * 60 * 1000L;

    protected static final double DEFAULT_RESIZE_FACTOR=1.2;

    protected static final Log    log=LogFactory.getLog(Table.class);





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

    public Table(int num_rows, int elements_per_row, long offset, double resize_factor, long max_compaction_time) {
        this.num_rows=num_rows;
        this.elements_per_row=elements_per_row;
        this.resize_factor=resize_factor;
        this.max_compaction_time=max_compaction_time;
        this.offset=this.low=this.hr=this.hd=offset;
        matrix=(T[][])new Object[num_rows][];
        if(resize_factor <= 1)
            throw new IllegalArgumentException("resize_factor needs to be > 1");
    }


    public long getOffset()            {return offset;}

    /** Returns the total capacity in the matrix */
    public int capacity()              {return matrix.length * elements_per_row;}

    /** Returns the numbers of elements in the table */
    public int size()                  {return size;}
    public boolean isEmpty()           {return size <= 0;}
    public long getLow()               {return low;}
    public long getHighestDelivered()  {return hd;}
    public long getHighestReceived()   {return hr;}
    public long getMaxCompactionTime() {return max_compaction_time;}
    public void setMaxCompactionTime(long max_compaction_time) {this.max_compaction_time=max_compaction_time;}

    /** Returns the ratio between size and capacity, as a percentage */
    public double getFillFactor()      {return size == 0? 0.0 : (int)(((double)size / capacity()) * 100);}



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
        finally {
            lock.unlock();
        }
    }

    
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


    public List<T> get(long from, long to) {
        List<T> retval=null;
        lock.lock();
        try {
            for(long seqno=from; seqno <= to; seqno++) {
                if(seqno <= low || seqno > hr)
                    continue;
                int row_index=computeRow(seqno);
                if(row_index < 0 || row_index >= matrix.length)
                    continue;
                T[] row=matrix[row_index];
                if(row == null)
                    continue;
                int index=computeIndex(seqno);
                T element=index >= 0? row[index] : null;
                if(element != null) {
                    if(retval == null)
                        retval=new LinkedList<T>();
                    retval.add(element);
                }
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    /** Removes the next non-null element and nulls the index if nullify=true */
    public T remove(boolean nullify) {
        lock.lock();
        try {
            int row_index=computeRow(hd);
            if(row_index < 0 || row_index >= matrix.length)
                return null;
            T[] row=matrix[row_index];
            if(row == null)
                return null;
            int index=computeIndex(hd);
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
        return null;
    }


    /**
     * Removes all elements less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed
     * @param seqno
     */
    public void purge(long seqno) {
        lock.lock();
        try {
            int num_rows_to_remove=(int)(seqno - offset) / elements_per_row;
            for(int i=0; i < num_rows_to_remove; i++) // Null all rows which can be fully removed
                matrix[i]=null;

            int row_index=computeRow(seqno);
            if(row_index < 0 || row_index >= matrix.length)
                return;

            if(matrix[row_index] != null) {
                int index=computeIndex(seqno);
                for(int i=0; i <= index; i++) // null all elements up to and including seqno in the given row
                    matrix[row_index][i]=null;
            }
            size=computeSize();
            if(seqno > low)
                low=seqno;

            // see if compaction should be triggered
            if(max_compaction_time <= 0)
                return;

            long current_time=System.currentTimeMillis();
            if(last_compaction_timestamp > 0) {
                if(current_time - last_compaction_timestamp >= max_compaction_time) {
                    _compact();
                    last_compaction_timestamp=current_time;
                }
            }
            else
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



    /** Moves rows down the matrix, by removing purged rows. If resizing to accommodate seqno is still needed, computes
     * a new size. Then either moves existing rows down, or copies them into a new array (if resizing took place).
     * The lock must be held vy the caller of resize(). */
    @GuardedBy("lock")
    protected void resize(long seqno) {
        int num_rows_to_purge=(int)((low - offset) / elements_per_row);
        int row_index=computeRow(seqno) - num_rows_to_purge;
        if(row_index < 0)
            return;

        int new_size=Math.max(row_index +1, matrix.length);
        if(new_size > matrix.length) {
            T[][] new_matrix=(T[][])new Object[new_size][];
            System.arraycopy(matrix, num_rows_to_purge, new_matrix, 0, matrix.length - num_rows_to_purge);
            matrix=new_matrix;
        }
        else if(num_rows_to_purge > 0) {
            move(num_rows_to_purge);
        }

        offset+=(num_rows_to_purge * elements_per_row);
        size=computeSize();
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
    }


    /**
     * Moves the contents of matrix down by the number of purged rows and resizes the matrix accordingly. The
     * capacity of the matrix should be size * resize_factor. Caller must hold the lock.
     */
    @GuardedBy("lock")
    protected void _compact() {
        // This is the range we need to copy into the new matrix (including from and to)
        int from=computeRow(low), to=computeRow(hr);
        int range=to - from +1;  // e.g. from=3, to=5, new_size has to be [3 .. 5] (=3)

        int new_size=(int)Math.max(range * resize_factor, range +1);
        new_size=Math.max(new_size, num_rows); // don't fall below the initial size defined
        if(new_size < matrix.length) {
            if(log.isTraceEnabled())
                log.trace("compacting matrix from " + matrix.length + " rows to " + new_size + " rows");
            T[][] new_matrix=(T[][])new Object[new_size][];
            System.arraycopy(matrix, from, new_matrix, 0, range);
            matrix=new_matrix;
            offset+=from * elements_per_row;
            size=computeSize();
        }
    }


    /** Iterate from highest_seqno_purged to highest_seqno and add up non-null values. Caller must hold the lock. */
    @GuardedBy("lock")
    public int computeSize() {
        int retval=0;
        int from=computeRow(low), to=computeRow(hr);
        for(int i=from; i <= to; i++) {
            T[] row=matrix[i];
            if(row == null)
                continue;
            for(int j=0; j < row.length; j++) {
                if(row[j] != null)
                    retval++;
            }
        }
        return retval;
    }


    /** Returns the number of null elements in the range [hd+1 .. hr] */
    public int getNullElements() {
        int retval=0;
        lock.lock();
        try {
            for(long i=hd+1; i <= hr; i++) {
                int row_index=computeRow(i);
                if(row_index < 0 || row_index >= matrix.length)
                    continue;
                T[] row=matrix[row_index];
                if(row != null && row[computeIndex(i)] == null)
                    retval++;
            }
            return retval;
        }
        finally {
            lock.unlock();
        }
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("[" + low + " | " + hd + " | " + hr + "] (" + size() +
                    " elements, " + getNullElements() + " missing)");
        return sb.toString();
    }

    /** Dumps the seqnos in the table as a list */
    public String dump() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        lock.lock();
        try {
            for(int i=0; i < matrix.length; i++) {
                T[] row=matrix[i];
                if(row == null)
                    continue;
                for(int j=0; j < row.length; j++) {
                    if(row[j] != null) {
                        long seqno=offset + (i * elements_per_row) + j;
                        if(first)
                            first=false;
                        else
                            sb.append(", ");
                        sb.append(seqno);
                    }
                }
            }
            return sb.toString();
        }
        finally {
            lock.unlock();
        }
    }

     /** Dumps the non-null in the table in a pseudo graphic way */
    public String dumpMatrix() {
        StringBuilder sb=new StringBuilder();
        lock.lock();
        try {
            for(int i=0; i < matrix.length; i++) {
                T[] row=matrix[i];
                sb.append(i + ": ");
                if(row == null) {
                    sb.append("\n");
                    continue;
                }
                for(int j=0; j < row.length; j++) {
                    if(row[j] != null)
                        sb.append("* ");
                    else
                        sb.append("  ");
                }
                sb.append("\n");
            }
            return sb.toString();
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

    

}


package org.jgroups.util;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * A store for messages to be retransmitted or delivered. Used on sender and receiver side, as a replacement for
 * HashMap. RetransmitTable should use less memory than HashMap, as HashMap.Entry has 4 fields, plus arrays for storage.
 * <p/>
 * RetransmitTable maintains a matrix (an array of arrays) of messages. Messages are stored in the matrix by mapping
 * their seqno to an index. E.g. when we have 10 rows of 1000 messages each, and first_seqno is 3000, then a message with
 * seqno=5600, will be stored in the 3rd row, at index 600.
 * <p/>
 * Rows are removed when all messages in that row have been received.<p/>
 * This class in not synchronized; the caller has to make sure access to it is synchronized
 * @author Bela Ban
 */
@Deprecated
public class RetransmitTable {
    protected final int    num_rows;
    /** Must be a power of 2 for efficient modular arithmetic **/
    protected final int    msgs_per_row;
    protected final double resize_factor;
    protected Message[][]  matrix;

    /** The first seqno, at matrix[0][0] */
    protected long         offset;

    protected int          size=0;

    /** The highest seqno purged */
    protected long         highest_seqno_purged;

    /** The highest seqno in the table */
    protected long         highest_seqno;

    /** Time (in ms) after which a compaction should take place. 0 disables compaction */
    protected long         max_compaction_time=DEFAULT_MAX_COMPACTION_TIME;

    /** The time when the last compaction took place. If a {@link #compact()} takes place and sees that the
     * last compaction is more than max_compaction_time ms ago, a compaction will take place */
    protected long         last_compaction_timestamp=0;

    /** By default, rows are only nulled and highest_seqno_purged is adjusted when {@link #purge(long)} is called.
     * When automatic_purging is enabled (default is off), rows are purged and highest_seqno_purged is adjusted
     * on {@link #remove(long)} */
    protected boolean      automatic_purging;
    
    protected static final long DEFAULT_MAX_COMPACTION_TIME=2 * 60 * 1000L;

    protected static final double DEFAULT_RESIZE_FACTOR=1.2;

    protected static final Log log=LogFactory.getLog(RetransmitTable.class);


    public RetransmitTable() {
        this(5, 8192, 0, DEFAULT_RESIZE_FACTOR);
    }

    public RetransmitTable(int num_rows, int msgs_per_row, long offset) {
        this(num_rows, msgs_per_row, offset, DEFAULT_RESIZE_FACTOR);
    }

    public RetransmitTable(int num_rows, int msgs_per_row, long offset, double resize_factor) {
        this(num_rows, msgs_per_row, offset, resize_factor, DEFAULT_MAX_COMPACTION_TIME, false);
    }

    public RetransmitTable(int num_rows, int msgs_per_row, long offset, double resize_factor, long max_compaction_time,
                           boolean automatic_purging) {
        this.num_rows=num_rows;
        this.msgs_per_row=Util.getNextHigherPowerOfTwo(msgs_per_row);
        this.resize_factor=resize_factor;
        this.max_compaction_time=max_compaction_time;
        this.automatic_purging=automatic_purging;
        this.offset=this.highest_seqno_purged=this.highest_seqno=offset;
        matrix=new Message[num_rows][];
        if(resize_factor <= 1)
            throw new IllegalArgumentException("resize_factor needs to be > 1");
    }

    public int getLength() {
        return matrix.length;
    }

    public long getOffset() {
        return offset;
    }

    /** Returns the total capacity in the matrix */
    public int capacity() {return matrix.length * msgs_per_row;}

    /** Returns the numbers of messages in the table */
    public int size() {return size;}


    public boolean isEmpty() {return size <= 0;}


    public long getHighest() {
        return highest_seqno;
    }

    public long getHighestPurged() {
        return highest_seqno_purged;
    }

    public long getMaxCompactionTime() {
        return max_compaction_time;
    }

    public void setMaxCompactionTime(long max_compaction_time) {
        this.max_compaction_time=max_compaction_time;
    }

    public boolean isAutomaticPurging() {
        return automatic_purging;
    }

    public void setAutomaticPurging(boolean automatic_purging) {
        this.automatic_purging=automatic_purging;
    }

    /** Returns the ratio between size and capacity, as a percentage */
    public double getFillFactor() {
        return size == 0? 0.0 : (int)(((double)size / capacity()) * 100);
    }


    /**
     * Adds a new message to the index computed as a function of seqno
     * @param seqno
     * @param msg
     * @return True if the element at the computed index was null, else false
     */
    public boolean put(long seqno, Message msg) {
        return putIfAbsent(seqno, msg) == null;
    }

    /**
     * Adds a message if the element at the given index is null. Returns null if no message existed at the given index,
     * else returns the existing message and doesn't set the element.
     * @param seqno
     * @param msg
     * @return The existing message, or null if there wasn't any
     */
    public Message putIfAbsent(long seqno, Message msg) {
        int row_index=computeRow(seqno);
        if(row_index >= matrix.length) {
            resize(seqno);
            row_index=computeRow(seqno);
        }
        Message[] row=getRow(row_index);
        int index=computeIndex(seqno);
        Message existing_msg=row[index];
        if(existing_msg == null) {
            row[index]=msg;
            size++;
            if(seqno > highest_seqno)
                highest_seqno=seqno;
            return null;
        }
        else
            return existing_msg;
    }

    public Message get(long seqno) {
        int row_index=computeRow(seqno);
        if(row_index < 0 || row_index >= matrix.length)
            return null;
        Message[] row=matrix[row_index];
        if(row == null)
            return null;
        int index=computeIndex(seqno);
        return index >= 0? row[index] : null;
    }


    public List<Message> get(long from, long to) {
        List<Message> retval=null;
        for(long seqno=from; seqno <= to; seqno++) {
            Message msg=get(seqno);
            if(msg != null) {
                if(retval == null)
                    retval=new LinkedList<>();
                retval.add(msg);
            }
        }
        return retval;
    }


    /** Removes the message with seqno from the table, nulls the index */
    public Message remove(long seqno) {
        int row_index=computeRow(seqno);
        if(row_index < 0 || row_index >= matrix.length)
            return null;
        Message[] row=matrix[row_index];
        if(row == null)
            return null;
        int index=computeIndex(seqno);
        if(index < 0)
            return null;
        Message existing_msg=row[index];
        if(existing_msg != null) {
            row[index]=null;
            size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
            if(automatic_purging) {
                if(seqno > highest_seqno_purged)
                    highest_seqno_purged=seqno;
            }
        }
        return existing_msg;
    }

    /** Removes all elements. This method is usually called just before removing a retransmit table, so typically
     * it is not used anymore after returning */
    public void clear() {
        matrix=new Message[num_rows][];
        size=0;
        offset=highest_seqno_purged=highest_seqno=0;
    }



    /**
     * Removes all messages less than or equal to seqno from the table. Does this by nulling entire rows in the matrix
     * and nulling all elements < index(seqno) of the first row that cannot be removed
     * @param seqno
     */
    public void purge(long seqno) {
        int num_rows_to_remove=(int)(seqno - offset) / msgs_per_row;
        for(int i=0; i < num_rows_to_remove; i++) // Null all rows which can be fully removed
            matrix[i]=null;

        int row_index=computeRow(seqno);
        if(row_index < 0 || row_index >= matrix.length)
            return;

        Message[] row=matrix[row_index];
        if(row != null) {
            int index=computeIndex(seqno);
            for(int i=0; i <= index; i++) // null all messages up to and including seqno in the given row
                row[i]=null;
        }
        size=computeSize();
        if(seqno > highest_seqno_purged)
            highest_seqno_purged=seqno;

        // see if compaction should be triggered
        if(max_compaction_time <= 0)
            return;

        long current_time=System.currentTimeMillis();
        if(last_compaction_timestamp > 0) {
            if(current_time - last_compaction_timestamp >= max_compaction_time) {
                compact();
                last_compaction_timestamp=current_time;
            }
        }
        else
            last_compaction_timestamp=current_time;
    }



    /** Moves rows down the matrix, by removing purged rows. If resizing to accommodate seqno is still needed, computes
     * a new size. Then either moves existing rows down, or copies them into a new array (if resizing took place) */
    protected void resize(long seqno) {
        int num_rows_to_purge=(int)((highest_seqno_purged - offset) / msgs_per_row);
        int row_index=computeRow(seqno) - num_rows_to_purge;
        if(row_index < 0)
            return;

        int new_size=Math.max(row_index +1, matrix.length);
        if(new_size > matrix.length) {
            Message[][] new_matrix=new Message[new_size][];
            System.arraycopy(matrix, num_rows_to_purge, new_matrix, 0, matrix.length - num_rows_to_purge);
            matrix=new_matrix;
        }
        else if(num_rows_to_purge > 0) {
            move(num_rows_to_purge);
        }

        offset+=(num_rows_to_purge * msgs_per_row);
        size=computeSize();
    }


    /** Moves contents of matrix num_rows down. Avoids a System.arraycopy() */
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
     * capacity of the matrix should be size * resize_factor
     */
    public void compact() {
        // This is the range we need to copy into the new matrix (including from and to)
        int from=computeRow(highest_seqno_purged), to=computeRow(highest_seqno);
        int range=to - from +1;  // e.g. from=3, to=5, new_size has to be [3 .. 5] (=3)

        int new_size=(int)Math.max(range * resize_factor, range +1);
        new_size=Math.max(new_size, num_rows); // don't fall below the initial size defined
        if(new_size < matrix.length) {
            if(log.isTraceEnabled())
                log.trace("compacting matrix from " + matrix.length + " rows to " + new_size + " rows");
            Message[][] new_matrix=new Message[new_size][];
            System.arraycopy(matrix, from, new_matrix, 0, range);
            matrix=new_matrix;
            offset+=from * msgs_per_row;
            size=computeSize();
        }
    }



    /** Iterate from highest_seqno_purged to highest_seqno and add up non-null values */
    public int computeSize() {
        int retval=0;
        int from=computeRow(highest_seqno_purged), to=computeRow(highest_seqno);
        for(int i=from; i <= to; i++) {
            Message[] row=matrix[i];
            if(row == null)
                continue;
            for(int j=0; j < row.length; j++) {
                if(row[j] != null)
                    retval++;
            }
        }
        return retval;
    }


    /** Returns the number of null elements in the range [from .. to], excluding 'from' and 'to' */
    public int getNullMessages(long from, long to) {
        int retval=0;
        for(long i=from+1; i < to; i++) {
            int row_index=computeRow(i);
            if(row_index < 0 || row_index >= matrix.length)
                continue;
            Message[] row=matrix[row_index];
            if(row == null || row[computeIndex(i)] == null)
                retval++;
        }
        return retval;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("size=" + size + ", capacity=" + capacity() + ", highest=" + highest_seqno +
                    ", highest_purged=" + highest_seqno_purged);
        return sb.toString();
    }

    /** Dumps the seqnos in the table as a list */
    public String dump() {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(int i=0; i < matrix.length; i++) {
            Message[] row=matrix[i];
            if(row == null)
                continue;
            for(int j=0; j < row.length; j++) {
                if(row[j] != null) {
                    long seqno=offset + (i * msgs_per_row) + j;
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

     /** Dumps the non-null in the table in a pseudo graphic way */
    public String dumpMatrix() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < matrix.length; i++) {
            Message[] row=matrix[i];
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

    /**
     * Computes the size of all messages currently in the table. This includes messages that haven't been purged or
     * compacted yet.
     * @param include_headers If true, {@link org.jgroups.Message#size()} is used, which will include the size of all
     * headers and the dest and src addresses. Else {@link org.jgroups.Message#getLength()} is used to compute.
     * Note that the latter is way more efficient.
     * @return Number of bytes of all messages.
     */
    public long sizeOfAllMessages(boolean include_headers) {
        long retval=0;
        for(int i=0; i < matrix.length; i++) {
            Message[] row=matrix[i];
            if(row == null)
                continue;
            for(int j=0; j < row.length; j++) {
                Message msg=row[j];
                if(msg != null)
                    retval+=include_headers? msg.size() : msg.getLength();
            }
        }
        return retval;
    }


    /**
     * Returns a row. Creates a new row and inserts it at index if the row at index doesn't exist
     * @param index
     * @return A row
     */
    protected Message[] getRow(int index) {
        Message[] row=matrix[index];
        if(row == null) {
            row=new Message[msgs_per_row];
            matrix[index]=row;
        }
        return row;
    }


    /** Computes and returns the row index for seqno */
    protected int computeRow(long seqno) {
        int diff=(int)(seqno-offset);
        if(diff < 0) return diff;
        return diff / msgs_per_row;
    }


    /** Computes and returns the index within a row for seqno */
    protected int computeIndex(long seqno) {
        int diff=(int)(seqno - offset);
        if(diff < 0)
            return diff;

        // Same as diff % msgs_per_row
        return diff & (msgs_per_row - 1);
    }

    

}


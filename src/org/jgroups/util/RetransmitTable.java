package org.jgroups.util;

import org.jgroups.Message;

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
public class RetransmitTable {
    protected final int    num_rows;
    protected final int    msgs_per_row;
    protected Message[][]  matrix;

    /** The first seqno, at matrix[0][0] */
    protected long         offset;

    protected volatile int size=0;


    public RetransmitTable() {
        this(5, 10000, 0);
    }

    public RetransmitTable(int num_rows, int msgs_per_row, long offset) {
        this.num_rows=num_rows;
        this.msgs_per_row=msgs_per_row;
        this.offset=offset;
        matrix=new Message[num_rows][];
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
        Message[] row=getRow(row_index);
        int index=computeIndex(seqno);
        Message existing_msg=row[index];
        if(existing_msg == null) {
            row[index]=msg;
            size++;
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
        return row[index];
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
        Message existing_msg=row[index];
        if(existing_msg != null) {
            row[index]=null;
            size=Math.max(size-1, 0); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
        }
        return existing_msg;
    }

    /** Removes all elements. This method is usually called just before removing a retransmit table, so typically
     * it is not used anymore after returning */
    public void clear() {
        matrix=new Message[num_rows][];
        size=0;
        offset=1;
    }


    /**
     * Removes all messages less than or equal to seqno from the table. Adjusts offset and moves rows down by the
     * number of removed rows. This method should be used when a number of messages can be removed at once, instead
     * of individually removing them with remove().
     * @param seqno
     */
    public void purge(long seqno) {
        long diff=seqno - offset;
        if(diff < msgs_per_row)
            return;
        int num_rows_to_remove=(int)(diff / msgs_per_row);
        System.arraycopy(matrix, num_rows_to_remove, matrix, 0, matrix.length - num_rows_to_remove);
        for(int i=matrix.length - num_rows_to_remove; i < matrix.length; i++)
            matrix[i]=null;

        offset+=(num_rows_to_remove * msgs_per_row);
        size=computeSize();
    }


    /** Returns the total capacity in the matrix */
    public int capacity() {return matrix.length * msgs_per_row;}

    /** Returns the numbers of messages in the table */
    public int size() {return size;}

    public boolean isEmpty() {return size <= 0;}

    /** A more expensive way to compute the size, done by iterating through the entire table and adding up non-null values */
    public int computeSize() {
        int retval=0;
        for(int i=0; i < matrix.length; i++) {
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

    /** Returns the number of null elements up to 'to' */
    public int getNullMessages(long to) {
        int retval=0;
        for(long i=offset; i <= to; i++) {
            int row_index=computeRow(i);
            if(row_index < 0 || row_index >= matrix.length)
                continue;
            Message[] row=matrix[row_index];
            if(row != null && row[computeIndex(i)] == null)
                retval++;
        }
        return retval;
    }


    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("size=" + size + ", capacity=" + capacity());
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
     * Returns a row. Creates a new row and inserts it at index if the row at index doesn't exist
     * @param index
     * @return A row
     */
    protected Message[] getRow(int index) {
        if(index >= matrix.length)
            resize(index +1);
        Message[] row=matrix[index];
        if(row == null) {
            row=new Message[msgs_per_row];
            matrix[index]=row;
        }
        return row;
    }

    /** Resizes the matrix to the new size */
    protected void resize(int new_capacity) {
        Message[][] new_matrix=new Message[new_capacity][];
        System.arraycopy(matrix, 0, new_matrix, 0, matrix.length);
        matrix=new_matrix;
    }



    /** Computes and returns the row index for seqno */
    protected int computeRow(long seqno) {
        return (int)(((seqno- offset) / msgs_per_row));
    }


    /** Computes and returns the index within a row for seqno */
    protected int computeIndex(long seqno) {
        return (int)(seqno - offset) % msgs_per_row;
    }

    

}


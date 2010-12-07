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
    protected int msgs_per_row;

    protected Message[][] matrix;

    /** The first seqno, at matrix[0][0] */
    protected int offset;

    protected volatile int size=0;


    public RetransmitTable() {
        this(10, 50000);
    }

    public RetransmitTable(int num_rows, int msgs_per_row) {
        this.msgs_per_row=msgs_per_row;
        matrix=new Message[num_rows][];
    }

    /**
     * Adds a new message to the index computed as a function of seqno
     * @param seqno
     * @param msg
     * @return True if the element at the computed index was null, else false
     */
    public boolean add(long seqno, Message msg) {
        int[] row_and_index=computeRowAndIndex(seqno);
        if(row_and_index == null)
            return false;
        Message[] row=getRow(row_and_index[0]);
        if(row[row_and_index[1]] == null) {
            row[row_and_index[1]]=msg;
            size++;
            return true;
        }
        return false;
    }

    public Message get(long seqno) {
        int[] row_and_index=computeRowAndIndex(seqno);
        if(row_and_index == null)
            return null;
        Message[] row=getRow(row_and_index[0]);
        return row[row_and_index[1]]; // todo: what do we do when we have an index out of bound exception ?
    }

    /** Returns the total capacity in the matrix */
    public int capacity() {
        return matrix.length * msgs_per_row;
    }

    /** Returns the numbers of messages in the table */
    public int size() {
        return size;
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

    /** Computes and returns the row index and the index within that row for seqno */
    protected int[] computeRowAndIndex(long seqno) {
        if(seqno < offset)
            return null;
        int[] retval=new int[2];
        int row_index=(int)(((seqno- offset) / msgs_per_row));
        int index=(int)(seqno - offset) % msgs_per_row;
        retval[0]=row_index;
        retval[1]=index;
        return retval;
    }

    

}


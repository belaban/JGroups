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
public class RetransmitTableArray {
    protected final int    initial_size;
    protected Message[]    array;

    /** The first seqno, at matrix[0][0] */
    protected long         offset;

    protected volatile int size=0;


    public RetransmitTableArray() {
        this(50000, 0);
    }

    public RetransmitTableArray(int initial_size, long offset) {
        this.initial_size=initial_size;
        this.offset=offset;
        array=new Message[initial_size];
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
        int index=computeIndex(seqno);
        if(index >= array.length)
            resize(index +1); // todo: maybe resize to a higher value ? But not double the prev capacity !
        Message existing=array[index];
        if(existing == null) {
            array[index]=msg;
            size++;
            return null;
        }
        else {
            return existing;
        }
    }

    
    public Message get(long seqno) {
        int index=computeIndex(seqno);
        return (index < 0 || index >= array.length)? null : array[index];
    }


    /** Removes the message with seqno from the table, nulls the index */
    public Message remove(long seqno) { // todo: purge if we can !
        int index=computeIndex(seqno);
        if(index < 0 || index >= array.length)
            return null;
        Message existing=array[index];
        if(existing != null)
            size=Math.max(0, size -1); // cannot be < 0 (well that would be a bug, but let's have this 2nd line of defense !)
        array[index]=null;
        return existing;
    }

    /** Removes all elements. This method is usually called just before removing a retransmit table, so typically
     * it is not used anymore after returning */
    public void clear() {
        array=new Message[initial_size];
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
        int diff=(int)(seqno - offset);
        if(diff < array.length / 10) // todo: currently hard-coded, needs to be dynamically adjusted
            return;
        System.arraycopy(array, diff, array, 0, array.length - diff);
        for(int i=array.length - diff; i < array.length; i++)
            array[i]=null;
        offset=seqno;
        size=computeSize();
    }


    @Deprecated
    public boolean containsKey(long seqno) {return get(seqno) != null;}

    /** Returns the total capacity in the matrix */
    public int capacity() {return array.length;}

    /** Returns the numbers of messages in the table */
    public int size() {return size;}

    public boolean isEmpty() {return size <= 0;}

    /** A more expensive way to compute the size, done by iterating through the entire table and adding up non-null values */
    public int computeSize() {
        int retval=0;
        for(Message msg: array)
            if(msg != null)
                retval++;
        return retval;
    }

    /** Returns the number of null elements up to 'to' */
    public int getNullMessages(long to) {
        int retval=0;
        for(long i=offset; i <= to; i++) {
            if(array[((int)i)] == null)
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
        for(int i=0; i < array.length; i++) {
            if(array[i] != null) {
                long seqno=offset + i;
                if(first)
                    first=false;
                else
                    sb.append(", ");
                sb.append(seqno);
            }
        }
        return sb.toString();
    }


     /** Dumps the non-null in the table in a pseudo graphic way */
    public String dumpMatrix() {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < array.length; i++) {
            sb.append(i + ": ");
            if(array[i] != null)
                sb.append("* ");
            else
                sb.append("  ");
            sb.append("\n");
        }
        return sb.toString();
    }




    /** Resizes the matrix to the new size */
    protected void resize(int new_capacity) {
        Message[] new_array=new Message[new_capacity];
        System.arraycopy(array, 0, new_array, 0, array.length);
        array=new_array;
    }

    /** Computes and returns the row index and the index within that row for seqno */
    protected int computeIndex(long seqno) {
        return (int)(seqno - offset);
    }



}


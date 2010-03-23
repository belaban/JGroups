package org.jgroups.stack;


import org.jgroups.Message;
import org.jgroups.util.Tuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;


/**
 * Counterpart of AckSenderWindow. Simple FIFO buffer.
 * Every message received is ACK'ed (even duplicates) and added to a hashmap
 * keyed by seqno. The next seqno to be received is stored in <code>next_to_remove</code>. When a message with
 * a seqno less than next_to_remove is received, it will be discarded. The <code>remove()</code> method removes
 * and returns a message whose seqno is equal to next_to_remove, or null if not found.<br>
 * Change May 28 2002 (bela): replaced TreeSet with HashMap. Keys do not need to be sorted, and adding a key to
 * a sorted set incurs overhead.
 *
 * @author Bela Ban
 * @version $Id: AckReceiverWindow.java,v 1.51 2010/03/23 16:09:54 belaban Exp $
 */
public class AckReceiverWindow {
    private final AtomicLong                   next_to_remove;
    private final AtomicBoolean                processing=new AtomicBoolean(false);
    private final ConcurrentMap<Long,Segment>  segments=new ConcurrentHashMap<Long,Segment>(64, 0.75F, 64);
    private volatile Segment                   current_segment=null;
    private volatile Segment                   current_remove_segment=null;
    private final int                          segment_capacity;
    private long                               highest_segment_created=0;

    public static final Message                TOMBSTONE=new Message(false) {
        public String toString() {
            return "tombstone";
        }
    };


    public AckReceiverWindow(long initial_seqno) {
        this(initial_seqno, 20000);
    }
    

    public AckReceiverWindow(long initial_seqno, int segment_capacity) {
        next_to_remove=new AtomicLong(initial_seqno);
        this.segment_capacity=segment_capacity;
        long index=next_to_remove.get() / segment_capacity;
        long first_seqno=(next_to_remove.get() / segment_capacity) * segment_capacity;
        this.segments.put(index, new Segment(first_seqno, segment_capacity));
        Segment initial_segment=findOrCreateSegment(next_to_remove.get());
        current_segment=initial_segment;
        current_remove_segment=initial_segment;
        for(long i=0; i < next_to_remove.get(); i++) {
            initial_segment.add(i, TOMBSTONE);
            initial_segment.remove(i);
        }
    }

    public AtomicBoolean getProcessing() {
        return processing;
    }



    /** Adds a new message. Message cannot be null
     * @return True if the message was added, false if not (e.g. duplicate, message was already present)
     */
    public boolean add(long seqno, Message msg) {
        return add2(seqno, msg) == 1;
    }


    /**
     * Adds a message if not yet received
     * @param seqno
     * @param msg
     * @return -1 if not added because seqno < next_to_remove, 0 if not added because already present,
     *          1 if added successfully
     */
    public byte add2(long seqno, Message msg) {
        Segment segment=current_segment;
        if(segment == null || !segment.contains(seqno)) {
            segment=findOrCreateSegment(seqno);
            if(segment != null)
                current_segment=segment;
        }
        if(segment == null)
            return -1;
        return segment.add(seqno, msg);
    }


    /**
     * Removes a message whose seqno is equal to <code>next_to_remove</code>, increments the latter. Returns message
     * that was removed, or null, if no message can be removed. Messages are thus removed in order.
     */
    public Message remove() {
        long next=next_to_remove.get();
        Segment segment=current_remove_segment;
        if(segment == null || !segment.contains(next)) {
            segment=findSegment(next);
            if(segment != null)
                current_remove_segment=segment;
        }
        if(segment == null)
            return null;
        Message retval=segment.remove(next);
        if(retval != null) {
            next_to_remove.compareAndSet(next, next +1);
            if(segment.allRemoved())
                segments.remove(next / segment_capacity);
        }
        return retval;
    }



    /**
     * Removes as many messages as possible (in sequence, without gaps)
     * @param max Max number of messages to be removed
     * @return Tuple<List<Message>,Long>: a tuple of the message list and the highest seqno removed
     */
    public Tuple<List<Message>,Long> removeMany(final int max) {
        List<Message> list=null; // we remove msgs.size() messages *max*
        Tuple<List<Message>,Long> retval=null;

        int count=0;
        boolean looping=true;
        while(count < max && looping) {
            long next=next_to_remove.get();
            Segment segment=current_remove_segment;
            if(segment == null || !segment.contains(next)) {
                segment=findSegment(next);
                if(segment != null)
                    current_remove_segment=segment;
            }
            if(segment == null)
                return retval;

            long segment_id=next;
            long end=segment.getEndIndex();
            while(next < end && count < max) {
                Message msg=segment.remove(next);
                if(msg == null) {
                    looping=false;
                    break;
                }
                if(list == null) {
                    list=new LinkedList<Message>(); // we remove msgs.size() messages *max*
                    retval=new Tuple<List<Message>,Long>(list, 0L);
                }
                list.add(msg);
                count++;
                retval.setVal2(next);
                next_to_remove.compareAndSet(next, ++next);
                if(segment.allRemoved())
                    segments.remove(segment_id / segment_capacity);
            }
        }

        return retval;
    }

    public List<Message> removeManyAsList(int max) {
        Tuple<List<Message>, Long> tuple=removeMany(max);
        return tuple != null? tuple.getVal1() : null;
    }

    
    public void reset() {
        segments.clear();
    }

    public int size() {
        int retval=0;
        for(Segment segment: segments.values())
            retval+=segment.size();
        return retval;
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        int size=size();
        sb.append(size + " messages");
        if(size <= 100)
            sb.append(" in " + segments.size() + " segments");
        return sb.toString();
    }

    public String printMessages() {
        StringBuilder sb=new StringBuilder();
        List<Long> keys=new LinkedList<Long>(segments.keySet());
        Collections.sort(keys);
        for(long key: keys) {
            Segment segment=segments.get(key);
            if(segment == null)
                continue;
            for(long i=segment.getStartIndex(); i < segment.getEndIndex(); i++) {
                Message msg=segment.get(i);
                if(msg == null)
                    continue;
                if(msg == TOMBSTONE)
                    sb.append("T ");
                else
                    sb.append(i + " ");
            }
        }

        return sb.toString();
    }


    private Segment findOrCreateSegment(long seqno) {
        long index=seqno / segment_capacity;
        if(index > highest_segment_created) {
            long start_seqno=seqno / segment_capacity * segment_capacity;
            Segment segment=new Segment(start_seqno, segment_capacity);
            Segment tmp=segments.putIfAbsent(index, segment);
            if(tmp != null) // segment already exists
                segment=tmp;
            else
                highest_segment_created=index;
            return segment;
        }

        return segments.get(index);
    }

    private Segment findSegment(long seqno) {
        long index=seqno / segment_capacity;
        return segments.get(index);
    }



    private static class Segment {
        final long                          start_index; // e.g. 5000. Then seqno 5100 would be at index 100
        final int                           capacity;
        final AtomicReferenceArray<Message> array;
        final AtomicInteger                 num_tombstones=new AtomicInteger(0);

        public Segment(long start_index, int capacity) {
            this.start_index=start_index;
            this.capacity=capacity;
            this.array=new AtomicReferenceArray<Message>(capacity);
        }

        public long getStartIndex() {
            return start_index;
        }

        public long getEndIndex() {
            return start_index + capacity;
        }

        public boolean contains(long seqno) {
            return seqno >= start_index && seqno < getEndIndex();
        }

        public Message get(long seqno) {
            int index=index(seqno);
            if(index < 0 || index >= array.length())
                return null;
            return array.get(index);
        }

        public byte add(long seqno, Message msg) {
            int index=index(seqno);
            if(index < 0)
                return -1;
            boolean success=array.compareAndSet(index, null, msg);
            if(success) {
                return 1;
            }
            else
                return 0;
        }

        public Message remove(long seqno) {
            int index=index(seqno);
            if(index < 0)
                return null;
            Message retval=array.get(index);
            if(retval != null && retval != TOMBSTONE && array.compareAndSet(index, retval, TOMBSTONE)) {
                num_tombstones.incrementAndGet();
                return retval;
            }
            return null;
        }

        public boolean allRemoved() {
            return num_tombstones.get() >= capacity;
        }

        public String toString() {
            return start_index + " - " + (start_index + capacity -1) + " (" + size() + " elements)";
        }

        public int size() {
            int retval=0;
            for(int i=0; i < capacity; i++) {
                Message tmp=array.get(i);
                if(tmp != null && tmp != TOMBSTONE)
                    retval++;
            }
            return retval;
        }

        private int index(long seqno) {
            if(seqno < start_index)
                return -1;

            int index=(int)(seqno - start_index);
            if(index < 0 || index >= capacity)
                throw new IndexOutOfBoundsException("index=" + index + ", start_index=" + start_index + ", seqno=" + seqno);
            return index;
        }

    }

}
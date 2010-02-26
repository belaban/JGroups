package org.jgroups.stack;


import org.jgroups.Message;
import org.jgroups.util.Tuple;
import org.jgroups.annotations.GuardedBy;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
 * @version $Id: AckReceiverWindow.java,v 1.43 2010/02/26 11:03:58 belaban Exp $
 */
public class AckReceiverWindow {
    @GuardedBy("lock")
    private long                    next_to_remove=0;
    @GuardedBy("lock")
    private final Map<Long,Message> msgs=new HashMap<Long,Message>();
    private final AtomicBoolean     processing=new AtomicBoolean(false);
    private final Lock              lock=new ReentrantLock();


    public AckReceiverWindow(long initial_seqno) {
        next_to_remove=initial_seqno;
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
        if(msg == null)
            throw new IllegalArgumentException("msg must be non-null");
        lock.lock();
        try {
            if(seqno < next_to_remove)
                return -1;
            if(!msgs.containsKey(seqno)) {
                msgs.put(seqno, msg);
                return 1;
            }
            else
                return 0;
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Removes a message whose seqno is equal to <code>next_to_remove</code>, increments the latter. Returns message
     * that was removed, or null, if no message can be removed. Messages are thus removed in order.
     */
    public Message remove() {
        lock.lock();
        try {
            Message retval=msgs.remove(next_to_remove);
            if(retval != null)
                next_to_remove++;
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

   

    /**
     * Removes as many messages as possible (in sequence, without gaps)
     * @param max Max number of messages to be removed
     * @return Tuple<List<Message>,Long>: a tuple of the message list and the highest seqno removed
     */
    public Tuple<List<Message>,Long> removeMany(int max) {
        List<Message> list=new LinkedList<Message>(); // we remove msgs.size() messages *max*
        Tuple<List<Message>,Long> retval=new Tuple<List<Message>,Long>(list, 0L);

        Message msg;
        long highest=0;
        int count=0;

        lock.lock();
        try {
            while((msg=msgs.remove(next_to_remove)) != null) {
                highest=next_to_remove;
                next_to_remove++;
                list.add(msg);
                if(++count > max)
                    break;
            }
            retval.setVal2(highest);
            return retval;
        }
        finally {
            lock.unlock();
        }
    }
    
   

    public void reset() {
        msgs.clear();
    }

    public int size() {
        return msgs.size();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(msgs.size()).append(" msgs (").append("next=").append(next_to_remove).append(")");
        TreeSet<Long> s=new TreeSet<Long>(msgs.keySet());
        if(!s.isEmpty()) {
            sb.append(" [").append(s.first()).append(" - ").append(s.last()).append("]");
            sb.append(": ").append(s);
        }
        return sb.toString();
    }


    public String printDetails() {
        StringBuilder sb=new StringBuilder();
        sb.append(msgs.size()).append(" msgs (").append("next=").append(next_to_remove).append(")").
                append(", msgs=" ).append(new TreeSet<Long>(msgs.keySet()));
        return sb.toString();
    }


}

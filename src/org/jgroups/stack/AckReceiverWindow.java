package org.jgroups.stack;


import org.jgroups.Message;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


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
 * @version $Id: AckReceiverWindow.java,v 1.34 2009/10/31 06:42:18 belaban Exp $
 */
public class AckReceiverWindow {
    private long                    next_to_remove=0;
    private final Map<Long,Message> msgs=new HashMap<Long,Message>();  // keys: seqnos (Long), values: Messages
    private final AtomicBoolean     processing=new AtomicBoolean(false);


    public AckReceiverWindow(long initial_seqno) {
        this.next_to_remove=initial_seqno;
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
     *
     * @param seqno
     * @param msg
     * @return -1 if not added because seqno < next_to_remove, 0 if not added because already present,
     *          1 if added successfully
     */
    public byte add2(long seqno, Message msg) {
        if(msg == null)
            throw new IllegalArgumentException("msg must be non-null");
        synchronized(msgs) {
            if(seqno < next_to_remove)
                return -1;
            if(!msgs.containsKey(seqno)) {
                msgs.put(seqno, msg);
                return 1;
            }
            else
                return 0;
            }
        }


    /**
     * Removes a message whose seqno is equal to <code>next_to_remove</code>, increments the latter.
     * Returns message that was removed, or null, if no message can be removed. Messages are thus
     * removed in order.
     */
    public Message remove() {
        Message retval=null;

        synchronized(msgs) {
            long seqno=next_to_remove;
            try {
                retval=msgs.remove(seqno);
            }
            finally {
                if(retval != null)
                    next_to_remove=++seqno;
            }
        }
        return retval;
    }

    /**
     * We need to have the lock on 'msgs' while we're setting processing to false (if no message is available), because
     * this prevents some other thread from adding a message. Use case:
     * <ol>
     * <li>Thread 1 calls msgs.remove() --> returns null
     * <li>Thread 2 calls add()
     * <li>Thread 2 checks the CAS and returns because Thread1 hasn't yet released it
     * <li>Thread 1 releases the CAS
     * </ol>
     * The result here is that Thread 2 didn't go into the remove() processing and returned, and Thread 1 didn't see
     * the new message and therefore returned as well. Result: we have an unprocessed message in 'msgs' !
     * @param processing
     * @return
     */
    public Message remove(AtomicBoolean processing) {
        Message retval=null;

        synchronized(msgs) {
            long seqno=next_to_remove;
            try {
                retval=msgs.remove(seqno);
            }
            finally {
                if(retval != null)
                    next_to_remove=++seqno;
                else
                    processing.set(false);
            }
        }
        return retval;
    }

    /**
     * Removes as many messages as possible (in sequence, without gaps)
     * @return
     */
    public List<Message> removeMany(AtomicBoolean processing) {
        List<Message> retval;
        Message msg;

        synchronized(msgs) {
            retval=new ArrayList<Message>(msgs.size()); // we remove msgs.size() messages *max*
            while((msg=msgs.remove(next_to_remove)) != null) {
                next_to_remove++;
                retval.add(msg);
            }
            if(retval.isEmpty())
                processing.set(false);
        }
        return retval;
    }

    public Message removeOOBMessage() {
        Message retval;

        synchronized(msgs) {
            retval=msgs.get(next_to_remove);
            if(retval != null) {
                if(!retval.isFlagSet(Message.OOB)) {
                    return null;
                }
                retval=msgs.remove(next_to_remove);
                next_to_remove++;
            }
        }
        return retval;
    }


    public boolean hasMessagesToRemove() {
        synchronized(msgs) {
            return msgs.containsKey(next_to_remove);
        }
    }


    public void reset() {
        synchronized(msgs) {
            msgs.clear();
        }
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

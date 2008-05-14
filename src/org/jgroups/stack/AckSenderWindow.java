// $Id: AckSenderWindow.java,v 1.29 2008/05/14 12:00:29 belaban Exp $

package org.jgroups.stack;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.TimeScheduler;

import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * ACK-based sliding window for a sender. Messages are added to the window keyed by seqno
 * When an ACK is received, the corresponding message is removed. The Retransmitter
 * continously iterates over the entries in the hashmap, retransmitting messages based on their
 * creation time and an (increasing) timeout. When there are no more messages in the retransmission
 * table left, the thread terminates. It will be re-activated when a new entry is added to the
 * retransmission table.
 * @author Bela Ban
 */
public class AckSenderWindow implements Retransmitter.RetransmitCommand {
    RetransmitCommand       retransmit_command = null;                            // called to request XMIT of msg
    final ConcurrentMap<Long,Message> msgs=new ConcurrentHashMap<Long,Message>(); 
    Interval                interval=new StaticInterval(400,800,1200,1600);
    final Retransmitter     retransmitter;
    static final Log        log=LogFactory.getLog(AckSenderWindow.class);


    public interface RetransmitCommand {
        void retransmit(long seqno, Message msg);
    }


    /**
     * Creates a new instance. Thre retransmission thread has to be started separately with
     * <code>start()</code>.
     * @param com If not null, its method <code>retransmit()</code> will be called when a message
     *            needs to be retransmitted (called by the Retransmitter).
     */
    public AckSenderWindow(RetransmitCommand com) {
        retransmit_command = com;
        retransmitter = new Retransmitter(null, this);
        retransmitter.setRetransmitTimeouts(interval);
    }



    public AckSenderWindow(RetransmitCommand com, Interval interval, TimeScheduler sched) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new Retransmitter(null, this, sched);
        retransmitter.setRetransmitTimeouts(interval);
    }

    public AckSenderWindow(RetransmitCommand com, Interval interval, TimeScheduler sched, Address sender) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new Retransmitter(sender, this, sched);
        retransmitter.setRetransmitTimeouts(interval);
    }



    public void reset() {
        msgs.clear();

        // moved out of sync scope: Retransmitter.reset()/add()/remove() are sync'ed anyway
        // Bela Jan 15 2003
        retransmitter.reset();
    }


    /**
     * Adds a new message to the retransmission table. If the message won't have received an ack within
     * a certain time frame, the retransmission thread will retransmit the message to the receiver. If
     * a sliding window protocol is used, we only add up to <code>window_size</code> messages. If the table is
     * full, we add all new messages to a queue. Those will only be added once the table drains below a certain
     * threshold (<code>min_threshold</code>)
     */
    public void add(long seqno, Message msg) {
        msgs.putIfAbsent(seqno, msg);
        retransmitter.add(seqno, seqno);
    }


    /**
     * Removes the message from <code>msgs</code>, removing them also from retransmission. If
     * sliding window protocol is used, and was queueing, check whether we can resume adding elements.
     * Add all elements. If this goes above window_size, stop adding and back to queueing. Else
     * set queueing to false.
     */
    public void ack(long seqno) {
        msgs.remove(new Long(seqno));
        retransmitter.remove(seqno);
    }

    public int size() {
        return msgs.size();
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(msgs.size()).append(" msgs (").append(retransmitter.size()).append(" to retransmit): ");
        TreeSet<Long> keys=new TreeSet<Long>(msgs.keySet());
        if(!keys.isEmpty())
            sb.append(keys.first()).append(" - ").append(keys.last());
        else
            sb.append("[]");
        return sb.toString();
    }


    public String printDetails() {
        StringBuilder sb=new StringBuilder();
        sb.append(msgs.size()).append(" msgs (").append(retransmitter.size()).append(" to retransmit): ").
                append(new TreeSet<Long>(msgs.keySet()));
        return sb.toString();
    }

    /* -------------------------------- Retransmitter.RetransmitCommand interface ------------------- */
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        Message msg;

        if(retransmit_command != null) {
            if(log.isTraceEnabled())
                log.trace(new StringBuilder("retransmitting messages ").append(first_seqno).
                          append(" - ").append(last_seqno).append(" from ").append(sender));
            for(long i = first_seqno; i <= last_seqno; i++) {
                if((msg=msgs.get(i)) != null) { // find the message to retransmit
                    retransmit_command.retransmit(i, msg);
                }
            }
        }
    }
    /* ----------------------------- End of Retransmitter.RetransmitCommand interface ---------------- */





    /* ---------------------------------- Private methods --------------------------------------- */

    /* ------------------------------ End of Private methods ------------------------------------ */




    /** Struct used to store message alongside with its seqno in the message queue */
    static class Entry {
        final long seqno;
        final Message msg;

        Entry(long seqno, Message msg) {
            this.seqno = seqno;
            this.msg = msg;
        }
    }


    static class Dummy implements RetransmitCommand {
        static final long last_xmit_req = 0;
        long curr_time;


        public void retransmit(long seqno, Message msg) {
            if(log.isDebugEnabled()) log.debug("seqno=" + seqno);
            curr_time = System.currentTimeMillis();
        }
    }




}

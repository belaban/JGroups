// $Id: AckSenderWindow.java,v 1.36 2009/09/21 09:57:24 belaban Exp $

package org.jgroups.stack;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Global;
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
    private RetransmitCommand       retransmit_command = null;                            // called to request XMIT of msg
    private final ConcurrentMap<Long,Message> msgs=new ConcurrentHashMap<Long,Message>();
    private Interval                interval=new StaticInterval(400,800,1200,1600);
    private final Retransmitter     retransmitter;
    private long                    lowest=Global.DEFAULT_FIRST_UNICAST_SEQNO; // lowest seqno, used by ack()


    public interface RetransmitCommand {
        void retransmit(long seqno, Message msg);
    }


    public AckSenderWindow(RetransmitCommand com) {
        retransmit_command = com;
        retransmitter = new Retransmitter(null, this, null);
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


    /** Only to be used for testing purposes */
    public synchronized long getLowest() {
        return lowest;
    }

    public void reset() {
        msgs.clear();
        retransmitter.reset();
        lowest=Global.DEFAULT_FIRST_UNICAST_SEQNO;
    }


    /**
     * Adds a new message to the retransmission table. The message will be retransmitted (based on timeouts passed into
     * AckSenderWindow until (1) an ACK is received or (2) the AckSenderWindow is stopped (@link{#reset})
     */
    public void add(long seqno, Message msg) {
        msgs.putIfAbsent(seqno, msg);
        retransmitter.add(seqno, seqno);
    }


    /**
     * Removes all messages <em>less than or equal</em> to seqno from <code>msgs</code>, and cancels their retransmission
     */
    public void ack(long seqno) {
        long prev_lowest;
        synchronized(this) {
            if(seqno < lowest) return; // not really needed, but we can avoid the max() call altogether...
            prev_lowest=lowest;
            lowest=Math.max(lowest, seqno +1);
        }

        for(long i=prev_lowest; i <= seqno; i++) {
            msgs.remove(i);
            retransmitter.remove(i);
        }
    }

    /** Returns the message with the lowest seqno */
    public synchronized Message getLowestMessage() {
        return msgs.get(lowest);
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
            for(long i = first_seqno; i <= last_seqno; i++) {
                if((msg=msgs.get(i)) != null) { // find the message to retransmit
                    retransmit_command.retransmit(i, msg);
                }
            }
        }
    }
    /* ----------------------------- End of Retransmitter.RetransmitCommand interface ---------------- */


    }

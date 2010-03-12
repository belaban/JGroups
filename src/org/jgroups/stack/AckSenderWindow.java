// $Id: AckSenderWindow.java,v 1.41 2010/03/12 09:07:17 belaban Exp $

package org.jgroups.stack;


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
    private RetransmitCommand                 retransmit_command = null;                 // called to request XMIT of msgs
    private final ConcurrentMap<Long,Message> msgs=new ConcurrentHashMap<Long,Message>();
    private Interval                          interval=new StaticInterval(400,800,1200,1600);
    private final Retransmitter               retransmitter;
    private long                              lowest=Global.DEFAULT_FIRST_UNICAST_SEQNO; // lowest seqno, used by ack()
    private long                              highest=0;


    public interface RetransmitCommand {
        void retransmit(long seqno, Message msg);
    }


    public AckSenderWindow(RetransmitCommand com) {
        retransmit_command = com;
        retransmitter = new DefaultRetransmitter(null, this, null);
        retransmitter.setRetransmitTimeouts(interval);
    }

    public AckSenderWindow(RetransmitCommand com, Interval interval, TimeScheduler sched) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new DefaultRetransmitter(null, this, sched);
        retransmitter.setRetransmitTimeouts(interval);
    }

    public AckSenderWindow(RetransmitCommand com, Interval interval, TimeScheduler sched, Address sender) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter = new DefaultRetransmitter(sender, this, sched);
        retransmitter.setRetransmitTimeouts(interval);
    }

    /**
     * Creates an instance <em>without</em> retransmitter
     */
    public AckSenderWindow() {
        retransmitter=null;
    }


    /** Only to be used for testing purposes */
    public synchronized long getLowest() {
        return lowest;
    }

    public long getHighest() {
        return highest;
    }

    public void reset() {
        msgs.clear();
        if(retransmitter != null)
            retransmitter.reset();
        lowest=Global.DEFAULT_FIRST_UNICAST_SEQNO;
    }


    public Message get(long seqno) {
        return msgs.get(seqno);
    }

    /**
     * Adds a new message to the retransmission table. The message will be retransmitted (based on timeouts passed into
     * AckSenderWindow until (1) an ACK is received or (2) the AckSenderWindow is stopped (@link{#reset})
     */
    public void add(long seqno, Message msg) {
        msgs.putIfAbsent(seqno, msg);
        if(retransmitter != null)
            retransmitter.add(seqno, seqno);
        highest=Math.max(highest, seqno);
    }

    public void addToMessages(long seqno, Message msg) {
        msgs.putIfAbsent(seqno, msg);
        highest=Math.max(highest, seqno);
    }

    public void addToRetransmitter(long seqno, Message msg) {
        if(retransmitter != null)
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
        removeRange(prev_lowest, seqno);
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
        int num_msgs=msgs.size();
        sb.append(num_msgs).append(" msgs");
        if(retransmitter != null)
            sb.append(" (").append(retransmitter.size()).append(" to retransmit)");
        if(num_msgs > 0) {
            sb.append(": ");
            TreeSet<Long> keys=new TreeSet<Long>(msgs.keySet());
            if(!keys.isEmpty())
                sb.append(keys.first()).append(" - ").append(keys.last());
            else
                sb.append("[]");
        }
        return sb.toString();
    }


    public String printDetails() {
        StringBuilder sb=new StringBuilder();
        sb.append(msgs.size()).append(" msgs");
        if(retransmitter != null)
            sb.append(" (").append(retransmitter.size()).append(" to retransmit)");
        sb.append(":\n");
        sb.append(new TreeSet<Long>(msgs.keySet()));
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

    private void removeRange(long from, long to) {
        for(long i=from; i <= to; i++) {
            msgs.remove(i);
            if(retransmitter != null)
                retransmitter.remove(i);
        }
    }

}

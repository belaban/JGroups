// $Id: AckSenderWindow.java,v 1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.log.Trace;
import org.jgroups.util.Queue;
import org.jgroups.util.Util;

import java.util.HashMap;


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
    RetransmitCommand retransmit_command = null;   // called to request XMIT of msg
    HashMap           msgs = new HashMap();        // keys: seqnos (Long), values: Messages
    long[]            interval = new long[]{1000, 2000, 3000, 4000};
    Retransmitter     retransmitter = new Retransmitter(null, this);
    Queue             msg_queue = new Queue(); // for storing messages if msgs is full
    int               window_size = -1;   // the max size of msgs, when exceeded messages will be queued

    /** when queueing, after msgs size falls below this value, msgs are added again (queueing stops) */
    int               min_threshold = -1;
    boolean           use_sliding_window = false, queueing = false;
    Protocol          transport = null; // used to send messages


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
        retransmitter.setRetransmitTimeouts(interval);
    }


    public AckSenderWindow(RetransmitCommand com, long[] interval) {
        retransmit_command = com;
        this.interval = interval;
        retransmitter.setRetransmitTimeouts(interval);
    }

    /**
     * This constructor whould be used when we want AckSenderWindow to send the message added
     * by add(), rather then ourselves.
     */
    public AckSenderWindow(RetransmitCommand com, long[] interval, Protocol transport) {
        retransmit_command = com;
        this.interval = interval;
        this.transport = transport;
        retransmitter.setRetransmitTimeouts(interval);
    }


    public void setWindowSize(int window_size, int min_threshold) {
        this.window_size = window_size;
        this.min_threshold = min_threshold;

        // sanity tests for the 2 values:
        if (min_threshold > window_size) {
            this.min_threshold = window_size;
            this.window_size = min_threshold;
            Trace.warn("AckSenderWindow.setWindowSize()", "min_threshold (" + min_threshold +
                    ") has to be less than window_size ( " + window_size + "). Values are swapped");
        }
        if (this.window_size <= 0) {
            this.window_size = this.min_threshold > 0 ? (int) (this.min_threshold * 1.5) : 500;
            Trace.warn("AckSenderWindow.setWindowSize()", "window_size is <= 0, setting it to " + this.window_size);
        }
        if (this.min_threshold <= 0) {
            this.min_threshold = this.window_size > 0 ? (int) (this.window_size * 0.5) : 250;
            Trace.warn("AckSenderWindow.setWindowSize()", "min_threshold is <= 0, setting it to " + this.min_threshold);
        }

        if (Trace.trace)
            Trace.info("AckSenderWindow.setWindowSize()", "window_size=" + this.window_size +
                    ", min_threshold=" + this.min_threshold);
        use_sliding_window = true;
    }


    public void reset() {
        synchronized (msgs) {
            msgs.clear();
        }

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
        Long tmp = new Long(seqno);

        synchronized (msgs) {
            if (msgs.containsKey(tmp))
                return;

            //System.out.println("### add: " + seqno + "(msg size=" + msgs.size() +
            //	       ", queue size=" + msg_queue.size() +
            //	       "). tstamp=" + System.currentTimeMillis() + ")"); // <remove>

            if (!use_sliding_window) {
                addMessage(seqno, tmp, msg);
            } else {  // we use a sliding window
                if (queueing)
                    addToQueue(seqno, msg);
                else {
                    if (msgs.size() + 1 > window_size) {
                        queueing = true;
                        addToQueue(seqno, msg);
                        if (Trace.debug)
                            Trace.info("AckSenderWindow.add()", "window_size (" + window_size + ") was exceeded, " +
                                    "starting to queue messages until window size falls under " + min_threshold);
                    } else {
                        addMessage(seqno, tmp, msg);
                    }
                }
            }
        }
    }


    /**
     * Removes the message from <code>msgs</code>, removing them also from retransmission. If
     * sliding window protocol is used, and was queueing, check whether we can resume adding elements.
     * Add all elements. If this goes above window_size, stop adding and back to queueing. Else
     * set queueing to false.
     */
    public void ack(long seqno) {
        Long tmp = new Long(seqno);
        Entry entry;

        synchronized (msgs) {
            msgs.remove(tmp);
            retransmitter.remove(seqno);

            if (use_sliding_window && queueing) {
                if (msgs.size() < min_threshold) { // we fell below threshold, now we can resume adding msgs
                    if (Trace.debug)
                        Trace.info("AckSenderWindow.ack()", "number of messages in table fell " +
                                "under min_threshold (" + min_threshold + "): adding " +
                                msg_queue.size() + " messages on queue");


                    while (msgs.size() < window_size) {
                        if ((entry = removeFromQueue()) != null)
                            addMessage(entry.seqno, new Long(entry.seqno), entry.msg);
                        else
                            break;
                    }

                    if (msgs.size() + 1 > window_size) {
                        if (Trace.debug)
                            Trace.info("AckSenderWindow.ack()", "exceded window_size (" + window_size +
                                    ") again, will still queue");
                        return; // still queueuing
                    } else
                        queueing = false; // allows add() to add messages again
                    if (Trace.debug)
                        Trace.info("AckSenderWindow.ack()",
                                "set queueing to false (table size=" + msgs.size() + ")");
                }
            }
        }
    }


    public String toString() {
        return msgs.keySet().toString() + " (retransmitter: " + retransmitter.toString() + ")";
    }

    /* -------------------------------- Retransmitter.RetransmitCommand interface ------------------- */
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        Message msg;

        if (retransmit_command != null) {
            for (long i = first_seqno; i <= last_seqno; i++) {
                if ((msg = (Message) msgs.get(new Long(i))) != null) { // find the message to retransmit
                    retransmit_command.retransmit(i, msg);
                    //System.out.println("### retr(" + first_seqno + "): tstamp=" + System.currentTimeMillis());
                }
            }
        }
    }
    /* ----------------------------- End of Retransmitter.RetransmitCommand interface ---------------- */





    /* ---------------------------------- Private methods --------------------------------------- */
    void addMessage(long seqno, Long tmp, Message msg) {
        if (transport != null)
            transport.passDown(new Event(Event.MSG, msg));
        msgs.put(tmp, msg);
        retransmitter.add(seqno, seqno);
    }

    void addToQueue(long seqno, Message msg) {
        try {
            msg_queue.add(new Entry(seqno, msg));
        } catch (Exception ex) {
            Trace.error("AckSenderWindow.addToQueue()", "exception=" + ex);
        }
    }

    Entry removeFromQueue() {
        try {
            return msg_queue.size() == 0 ? null : (Entry) msg_queue.remove();
        } catch (Exception ex) {
            Trace.error("AckSenderWindow.removeFromQueue()", "exception=" + ex);
            return null;
        }
    }
    /* ------------------------------ End of Private methods ------------------------------------ */




    /** Struct used to store message alongside with its seqno in the message queue */
    class Entry {
        long seqno;
        Message msg;

        Entry(long seqno, Message msg) {
            this.seqno = seqno;
            this.msg = msg;
        }
    }


    static class Dummy implements RetransmitCommand {
        long last_xmit_req = 0, curr_time;


        public void retransmit(long seqno, Message msg) {
            if (Trace.trace)
                Trace.info("Dummy.retransmit()", "seqno=" + seqno);

            curr_time = System.currentTimeMillis();
        }
    }


    public static void main(String[] args) {
        long[] xmit_timeouts = {1000, 2000, 3000, 4000};
        AckSenderWindow win = new AckSenderWindow(new Dummy(), xmit_timeouts);

        Trace.init();

        final int NUM = 1000;

        for (int i = 1; i < NUM; i++)
            win.add(i, new Message());


        System.out.println(win);
        Util.sleep(5000);

        for (int i = 1; i < NUM; i++) {
            if (i % 2 == 0) // ack the even seqnos
                win.ack(i);
        }

        System.out.println(win);
        Util.sleep(4000);

        for (int i = 1; i < NUM; i++) {
            if (i % 2 != 0) // ack the odd seqnos
                win.ack(i);
        }
        System.out.println(win);

        if (true) {
            Util.sleep(4000);
            System.out.println("--done--");
            return;
        }


        win.add(3, new Message());
        win.add(5, new Message());
        win.add(4, new Message());
        win.add(8, new Message());
        win.add(9, new Message());
        win.add(6, new Message());
        win.add(7, new Message());
        win.add(3, new Message());
        System.out.println(win);


        try {
            Thread.sleep(5000);
            win.ack(5);
            System.out.println("ack(5)");
            win.ack(4);
            System.out.println("ack(4)");
            win.ack(6);
            System.out.println("ack(6)");
            win.ack(7);
            System.out.println("ack(7)");
            win.ack(8);
            System.out.println("ack(8)");
            win.ack(6);
            System.out.println("ack(6)");
            win.ack(9);
            System.out.println("ack(9)");
            System.out.println(win);

            Thread.sleep(5000);
            win.ack(3);
            System.out.println("ack(3)");
            System.out.println(win);

            Thread.sleep(3000);
            win.add(10, new Message());
            win.add(11, new Message());
            System.out.println(win);
            Thread.sleep(3000);
            win.ack(10);
            System.out.println("ack(10)");
            win.ack(11);
            System.out.println("ack(11)");
            System.out.println(win);

            win.add(12, new Message());
            win.add(13, new Message());
            win.add(14, new Message());
            win.add(15, new Message());
            win.add(16, new Message());
            System.out.println(win);

            Util.sleep(1000);
            win.ack(12);
            System.out.println("ack(12)");
            win.ack(13);
            System.out.println("ack(13)");

            win.ack(15);
            System.out.println("ack(15)");
            System.out.println(win);

            Util.sleep(5000);
            win.ack(16);
            System.out.println("ack(16)");
            System.out.println(win);

            Util.sleep(1000);

            win.ack(14);
            System.out.println("ack(14)");
            System.out.println(win);
        } catch (Exception e) {
            System.err.println(e);
        }
    }

}

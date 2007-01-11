// $Id: TOTAL_OLD.java,v 1.13 2007/01/11 12:57:14 belaban Exp $

package org.jgroups.protocols;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;


/**
 * class SavedMessages
 * <p/>
 * Stores a set of messages along with their sequence id (assigned by the sequencer).
 */
class SavedMessages {

    final Log log=LogFactory.getLog(SavedMessages.class);

    /**
     * class Entry  (inner class)
     * <p/>
     * object type to store in the messages Vector (need to store sequence id in addition to message)
     */
    class Entry {
        private final Message msg;
        private final long seq;

        Entry(Message msg, long seq) {
            this.msg=msg;
            this.seq=seq;
        }

        public Message getMsg() {
            return msg;
        }

        public long getSeq() {
            return seq;
        }
    } // class Entry


    private final Vector messages;  // vector of "Entry"s to store "Message"s, sorted by sequence id



    /**
     * Constructor - creates an empty space to store messages
     */
    SavedMessages() {
        messages=new Vector();
    }

    /**
     * inserts the specified message and sequence id into the "list" of stored messages
     * if the sequence id given is already stored, then nothing is stored
     */
    public void insertMessage(Message msg, long seq) {
        synchronized(messages) {
            int size=messages.size();
            int index=0;
            long this_seq=-1;  // used to prevent duplicate messages being stored

            // find the index where this message should be inserted
            try {
                while((index < size) &&
                        ((this_seq=((Entry)(messages.elementAt(index))).getSeq()) < seq)) {
                    index++;
                }
            }
            catch(java.lang.ClassCastException e) {
                log.error("Error: (TOTAL_OLD) SavedMessages.insertMessage() - ClassCastException: could not cast element of \"messages\" to an Entry (index " + index + ')');
                return;
            }

            // check that the sequences aren't the same (don't want duplicates)
            if(this_seq == seq) {
                log.error("SavedMessages.insertMessage() - sequence " + seq + " already exists in saved messages. Message NOT saved.");
                return;
            }

            messages.insertElementAt(new Entry(msg, seq), index);
        } // synchronized( messages )
    }

    /**
     * returns a copy of the stored message with the given sequence id
     * if delete_msg is true, then the message is removed from the
     * the list of stored messages, otherwise the message is not
     * removed from the list
     * if no message is stored with this sequence id, null is returned
     */
    private Message getMessage(long seq, boolean delete_msg) {
        synchronized(messages) {
            int size=messages.size();
            int index=0;
            long this_seq=-1;
            try {
                while((index < size) &&
                        ((this_seq=(((Entry)(messages.elementAt(index))).getSeq())) < seq)) {
                    index++;
                }
            }
            catch(java.lang.ClassCastException e) {
                log.error("Error: (TOTAL_OLD) SavedMessages.getMessage() - ClassCastException: could not cast element of \"messages\" to an Entry (index " + index + ')');
                return null;
            }
            // determine if we found the specified sequence
            if(this_seq == seq) {
                // we found the message at index
                Object temp_obj=messages.elementAt(index);
                if(temp_obj instanceof Entry) {
                    Message ret_val=((Entry)temp_obj).getMsg().copy();

                    // should we delete
                    if(delete_msg) {
                        messages.removeElementAt(index);
                    }

                    return ret_val;
                }
                else {
                    log.error("Error: (TOTAL_OLD) SavedMessages.getMessage() - could not cast element of \"messages\" to an Entry (index " + index + ')');
                    return null;
                } // if ( temp_obj instanceof Entry )
            }
            else {
                // we didn't find this sequence number in the messages
                return null;
            }
        } // synchronized( messages )
    }

    /**
     * returns a stored message with the given sequence id
     * the message is then removed from the list of stored messages
     * if no message is stored with this sequence id, null is returned
     */
    public Message getMessage(long seq) {
        return getMessage(seq, true);
    }

    /**
     * similar to GetMessage, except a copy of the message is returned
     * and the message is not removed from the list
     */
    public Message peekMessage(long seq) {
        return getMessage(seq, false);
    }

    /**
     * returns a copy of the stored message with the lowest sequence id
     * if delete_msg is true, then the message is removed from the
     * the list of stored messages, otherwise the message is not
     * removed from the list
     * if their are no messages stored, null is returned
     */
    private Message getFirstMessage(boolean delete_msg) {
        synchronized(messages) {
            if(isEmpty()) {
                return null;
            }
            else {
                Object temp_obj=messages.firstElement();
                if(temp_obj instanceof Entry) {
                    Message ret_val=((Entry)temp_obj).getMsg().copy();
                    messages.removeElementAt(0);
                    return ret_val;
                }
                else {
                    log.error("Error: (TOTAL_OLD) SavedMessages.getFirstMessage() - could not cast element of \"messages\" to an Entry");
                    return null;
                } // if ( temp_obj instanceof Entry )
            }
        } // synchronized( messages )
    }

    /**
     * returns the stored message with the lowest sequence id;
     * the message is then removed from the list of stored messages
     * if their are no messages stored, null is returned
     */
    public synchronized Message getFirstMessage() {
        return getFirstMessage(true);
    }

    /**
     * similar to GetFirstMessage, except a copy of the message is returned
     * and the message is not removed from the list
     */
    public Message peekFirstMessage() {
        return getFirstMessage(false);
    }

    /**
     * returns the lowest sequence id of the messages stored
     * if no messages are stored, -1 is returned
     */
    public long getFirstSeq() {
        synchronized(messages) {
            if(isEmpty()) {
                return -1;
            }
            else {
                Object temp_obj=messages.firstElement();
                if(temp_obj instanceof Entry) {
                    return ((Entry)temp_obj).getSeq();
                }
                else {
                    log.error("Error: (TOTAL_OLD) SavedMessages.getFirstSeq() - could not cast element of \"messages\" to an Entry ");
                    return -1;
                }
            }
        } // synchronized( messages )
    }

    /**
     * returns true if there are messages stored
     * returns false if there are no messages stored
     */
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    /**
     * returns the number of messages stored
     */
    public int getSize() {
        return messages.size();
    }

    /**
     * clears all of the stored messages
     */
    public void clearMessages() {
        synchronized(messages) {
            messages.removeAllElements();
        }
    }
} // class SavedMessages


/**
 * class MessageAcks
 * <p/>
 * Used by sequencer to store cumulative acknowledgements of broadcast messages
 * sent to the group in this view
 */
class MessageAcks {

    final Log log=LogFactory.getLog(MessageAcks.class);

    // TODO: may also want to store some sort of timestamp in each Entry (maybe)
    /**
     * class Entry  (inner class)
     * <p/>
     * object type to store cumulative acknowledgements using a member's Address
     * and the sequence id of a message
     */
    class Entry {
        public final Address addr;
        public long seq;

        Entry(Address addr, long seq) {
            this.addr=addr;
            this.seq=seq;
        }

        Entry(Address addr) {
            this.addr=addr;
            this.seq=-1;  // means that no acknowledgements have been made yet
        }
    } // class Entry

    // Vector of "Entry"s representing cumulative acknowledgements for each member of the group
    private final Vector acks;

    private final SavedMessages message_history;  // history of broadcast messages sent


    /**
     * Constructor - creates a Vector of "Entry"s given a Vector of "Address"es for the members
     */
    MessageAcks(Vector members) {
        acks=new Vector();

        // initialize the message history to contain no messages
        message_history=new SavedMessages();

        // insert slots for each member in the acknowledgement Vector
        reset(members);
    }

    /**
     * resets acknowledgement Vector with "Entry"s using the given Vector of "Address"es
     * also clears the message history
     */
    public synchronized void reset(Vector members) {
        clear();

        // initialize Vector of acknowledgements (no acks for any member)
        int num_members=members.size();
        for(int i=0; i < num_members; i++) {
            Object temp_obj=members.elementAt(i);
            if(temp_obj instanceof Address) {
                acks.addElement(new Entry((Address)temp_obj));
            }
            else {
                log.error("Error: (TOTAL_OLD) MessageAcks.reset() - could not cast element of \"members\" to an Address object");
                return;
            }
        }
    }

    /**
     * clear all acknowledgements and the message history
     */
    private void clear() {
        acks.removeAllElements();
        message_history.clearMessages();
    }

    /**
     * returns the Entry from the acknowledgement Vector with the given Address
     * returns null if an Entry with the given Address is not found
     */
    private Entry getEntry(Address addr) {
        synchronized(acks) {
            // look for this addreess in the acknowledgement Vector
            int size=acks.size();
            for(int i=0; i < size; i++) {
                Object temp_obj=acks.elementAt(i);
                if(temp_obj instanceof Entry) {
                    Entry this_entry=(Entry)temp_obj;
                    if((this_entry.addr).equals(addr)) {
                        // the given Address matches this entry
                        return this_entry;
                    }
                }
                else {
                    log.error("Error: (TOTAL_OLD) MessageAcks.getEntry() - could not cast element of \"acks\" to an Entry");
                } // if ( temp_obj instanceof Entry )
            }

            // if we get here, we didn't find this Address
            return null;
        }
    }

    /**
     * sets the sequence id for the given Address to the given value
     * note: if the current sequence value for this host is greater than
     * the given value, the sequence for this member is NOT changed
     * (i.e. it will only set it to a larger value)
     * if the given Address is not found in the member list,
     * nothing is changed
     */
    public void setSeq(Address addr, long seq) {
        Entry this_entry=getEntry(addr);
        if((this_entry != null) && (this_entry.seq < seq)) {
            this_entry.seq=seq;

            // try to remove any messages that we don't need anymore
            truncateHistory();
        }
    }

    /**
     * returns the sequence id of the "latest" cumulative acknowledgement
     * for the specified Address
     * if the Address is not found in the member list, a negative value
     * is returned
     * note: the value returned may also be negative if their have been
     * no acknowledgements from the given address
     */
    public long getSeq(Address addr) {
        Entry this_entry=getEntry(addr);
        if(this_entry == null) {
            return -2;  // TODO: change this to something else (e.g. constant) later  (maybe)
        }
        else {
            return this_entry.seq;
        }
    }

    /**
     * returns the message in the history that matches the given sequence id
     * returns null if no message exists in the history with this sequence id
     */
    public Message getMessage(long seq) {
        return message_history.peekMessage(seq);
    }

    /**
     * adds the given message (with the specified sequence id) to the
     * message history
     * if the given sequence id already exists in the message history,
     * the message is NOT added
     */
    public void addMessage(Message msg, long seq) {
        message_history.insertMessage(msg, seq);
    }

    /**
     * returns the minimum cumulative acknowledged sequence id from all the members
     * (i.e. the greatest sequence id cumulatively acknowledged by all members)
     */
    private long getLowestSeqAck() {
        synchronized(acks) {
            long ret_val=-10;  // start with a negative value

            int size=acks.size();
            for(int i=0; i < size; i++) {
                Object temp_obj=acks.elementAt(i);
                if(temp_obj instanceof Entry) {
                    long this_seq=((Entry)temp_obj).seq;
                    if(this_seq < ret_val) {
                        ret_val=this_seq;
                    }
                }
                else {
                    log.error("Error: (TOTAL_OLD) MessageAcks.getLowestSeqAck() - could not cast element of \"acks\" to an Entry (index=" + i + ')');
                    return -1;
                }
            }

            return ret_val;
        }
    }

    /**
     * removes messages from the history that have been acknowledged
     * by all the members of the group
     */
    private synchronized void truncateHistory() {
        long lowest_ack_seq=getLowestSeqAck();
        if(lowest_ack_seq < 0) {
            // either no members, or someone has not received any messages yet
            //   either way, do nothing
            return;
        }

        // don't want message_history being altered during this operation
        synchronized(message_history) {
            long lowest_stored_seq;
            // keep deleting the oldest stored message for as long as we can
            while(((lowest_stored_seq=message_history.getFirstSeq()) >= 0) &&
                    (lowest_stored_seq > lowest_ack_seq)) {
                // we can delete the oldest stored message
                message_history.getFirstMessage();
            }
        } // synchronized( message_history )
    }
} // class MessageAcks


/**
 * **************************************************************************
 * class TotalRetransmissionThread
 * <p/>
 * thread that handles retransmission for the TOTAL_OLD protocol
 * **************************************************************************
 */
class TotalRetransmissionThread extends Thread {
    // state variables to determine when and what to request
    private long last_retrans_request_time;  // last time (in milliseconds) that we sent a resend request
    private long last_requested_seq;         // latest sequence id that we have requested

    // retransmission constants
    final private static long polling_delay=1000;  // how long (in milliseconds) to sleep before rechecking for resend
    final private static long resend_timeout=2000; // amount of time (in milliseconds) to wait on a resend request before resending another request
    final private static int max_request=10;      // maximum number of resend request to send out in one iteration

    // reference to the parent TOTAL_OLD protocol instance
    private TOTAL_OLD prot_ptr;

    // flag to specify if the thread should continue running
    private boolean is_running;

    final Log log=LogFactory.getLog(TotalRetransmissionThread.class);


    /**
     * constructor
     * <p/>
     * creates and initializes a retransmission thread for the
     * specified instance of a TOTAL_OLD protocol
     */
    TotalRetransmissionThread(TOTAL_OLD parent_prot) {
        super(Util.getGlobalThreadGroup(), "retransmission thread");
        if(parent_prot != null) {
            prot_ptr=parent_prot;
        }
        else {
            // parent thread not specified
            log.fatal("given parent protocol reference is null\n  (FATAL ERROR - TOTAL_OLD protocol will not function properly)");

            // prevent the run method from doing any work
            is_running=false;
        }

        // initialize the state variables
        reset();

        // let the thread make resend requests
        is_running=true;
    }

    /**
     * resets the state of the thread as if it was just started
     * the thread will assume that there were no resend requests make
     */
    public void reset() {
        // we have not made any resend requests for any messages
        last_retrans_request_time=-1;
        last_requested_seq=-1;
    }


    /**
     * send a resend request to the given sequencer (from the given local_addr)
     * for the given sequence id
     */
    private void sendResendRequest(Address sequencer, Address local_addr, long seq_id) {
        Message resend_msg=new Message(sequencer, local_addr, null);
        resend_msg.putHeader(getName(), new TOTAL_OLD.TotalHeader(TOTAL_OLD.TotalHeader.TOTAL_RESEND, seq_id));
        prot_ptr.passDown(new Event(Event.MSG, resend_msg));

        // debug
        log.error("TotalRetransmissionThread.resend() - resend requested for message " + seq_id);
    }


    /**
     * checks if a resend request should be made to the sequencer. if a request needs
     * to be made, it makes the appropriate requests with the parameters specified
     * by the constants in this class
     */
    private void checkForResend() {
        long first_seq_id=prot_ptr.getFirstQueuedSeqID(); // sequence id of first queued message
        /*
        // begin debug
        System.out.println( "DEBUG (TotalRetransmissionThread) - first_seq_id = " + first_seq_id );
        // end debug
        */
        if(first_seq_id >= 0) {
            // there is at least one message in the queue

            long next_seq_id=prot_ptr.getNextSeqID();     // next sequence id expected from the group
            if((next_seq_id < first_seq_id)) {  // TODO: handle case to resend TOTAL_NEW_VIEW message
                // there are messages that we received out of order
                //log.error( "DEBUG (TotalRetransmissionThread) - there are messages queued" ); // debug

                // see if it is time to send a request
                long time_now=System.currentTimeMillis();
                if((next_seq_id > last_requested_seq) ||
                        (time_now > (last_retrans_request_time + resend_timeout)) ||
                        (last_retrans_request_time < 0)) {
                    // send a resend request to the sequencer
                    //log.error( "DEBUG (TotalRetransmissionThread) - sending resend requests" ); // debug
                    Address sequencer=prot_ptr.getSequencer();
                    if(sequencer == null) {
                        System.out.println("Error: (TOTAL_OLD) TotalRetransmissionThread.checkForResend() - could not determine sequencer to send a TOTAL_RESEND request");

                        return;
                    }

                    Address local_addr=prot_ptr.getLocalAddr();
                    if(local_addr == null) {
                        System.out.println("Warning: (TOTAL_OLD) TotalRetransmissionThread.checkForResend() - local address not specified in TOTAL_RESEND request... attempting to send requests anyway");
                    }

                    long temp_long=(next_seq_id + max_request);   // potential max seq id to request (exclusive)
                    long last_resend_seq_id=(temp_long > first_seq_id) ? first_seq_id : temp_long;
                    for(long resend_seq=next_seq_id; resend_seq < last_resend_seq_id; resend_seq++) {
                        sendResendRequest(sequencer, local_addr, resend_seq);
                    }
                    // update state for this set of resend requests
                    last_retrans_request_time=time_now;
                    last_requested_seq=last_resend_seq_id;
                }
            } // if ( (next_seq_id < first_seq_id) )
        } // if ( first_seq_id >= 0 )
        // else there are no messages to request
    }


    /**
     * overloaded from Thread
     * method that executes when the thread is started
     */
    public void run() {
        while(is_running) {
            // resend any requests if necessary
            //log.error( "DEBUG (TotalRetransmissionThread) - heartbeat" ); // debug
            checkForResend();

            // wait before check again
            try {
                sleep(polling_delay);
            }
            catch(InterruptedException e) {
            }    // do nothing if interrupted
        }
    }

    /**
     * stops the thread from making any further resend requests
     * note: the thread may not die immediately
     */
    public void stopResendRequests() {
        is_running=false;
    }
} // class TotalRetransmissionThread

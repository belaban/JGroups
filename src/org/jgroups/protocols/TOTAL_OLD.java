// $Id: TOTAL_OLD.java,v 1.5 2004/07/05 14:17:16 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Vector;





/**
 * class SavedMessages
 *
 * Stores a set of messages along with their sequence id (assigned by the sequencer).
 */
class SavedMessages {

    /**
     * class Entry  (inner class)
     *
     * object type to store in the messages Vector (need to store sequence id in addition to message)
     */
    class Entry {
	private Message msg;
	private long seq;

	public Entry( Message msg, long seq ) {
	    this.msg = msg;
	    this.seq = seq;
	}

	public Message getMsg() {
	    return msg;
	}

	public long getSeq() {
	    return seq;
	}
    } // class Entry


    private Vector messages;  // vector of "Entry"s to store "Message"s, sorted by sequence id


    /**
     * Constructor - creates an empty space to store messages
     */
    public SavedMessages() {
	messages = new Vector();
    }

    /**
     * inserts the specified message and sequence id into the "list" of stored messages
     * if the sequence id given is already stored, then nothing is stored
     */
    public void insertMessage( Message msg, long seq ) {
	synchronized( messages ) {
	    int size = messages.size();
	    int index = 0;
	    long this_seq = -1;  // used to prevent duplicate messages being stored

	    // find the index where this message should be inserted
	    try {
		while( (index < size) &&
		       ((this_seq = ((Entry) (messages.elementAt(index))).getSeq()) < seq) ) {
		    index++;
		}
	    } catch ( java.lang.ClassCastException e ) {
		System.err.println( "Error: (TOTAL_OLD) SavedMessages.insertMessage() - ClassCastException: could not cast element of \"messages\" to an Entry (index " + index + ')' );
		return;
	    }

	    // check that the sequences aren't the same (don't want duplicates)
	    if ( this_seq == seq ) {
		System.err.println( "SavedMessages.insertMessage() - sequence " + seq + " already exists in saved messages. Message NOT saved." );
		return;
	    }

	    messages.insertElementAt( new Entry( msg, seq ), index );
	} // synchronized( messages )
    }

    /**
     * returns a copy of the stored message with the given sequence id
     * if delete_msg is true, then the message is removed from the
     *   the list of stored messages, otherwise the message is not
     *   removed from the list
     * if no message is stored with this sequence id, null is returned
     */
    private Message getMessage( long seq, boolean delete_msg ) {
	synchronized( messages ) {
	    int size = messages.size();
	    int index = 0;
	    long this_seq = -1;
	    try {
		while( (index < size) &&
		       ((this_seq = (((Entry) (messages.elementAt(index))).getSeq())) < seq) ) {
		    index++;
		}
	    } catch ( java.lang.ClassCastException e ) {
		System.err.println( "Error: (TOTAL_OLD) SavedMessages.getMessage() - ClassCastException: could not cast element of \"messages\" to an Entry (index " + index + ')' );
		return null;
	    }
	    // determine if we found the specified sequence
	    if ( this_seq == seq ) {
		// we found the message at index
		Object temp_obj = messages.elementAt(index);
		if ( temp_obj instanceof Entry ) {
		    Message ret_val = ((Entry) temp_obj).getMsg().copy();

		    // should we delete 
		    if ( delete_msg ) {
			messages.removeElementAt(index);
		    }

		    return ret_val;
		} else {
		    System.err.println( "Error: (TOTAL_OLD) SavedMessages.getMessage() - could not cast element of \"messages\" to an Entry (index " + index + ')' );
		    return null;
		} // if ( temp_obj instanceof Entry )
	    } else {
		// we didn't find this sequence number in the messages
		return null;
	    }
	} // synchronized( messages )
    }

    /**
     * returns a stored message with the given sequence id
     *   the message is then removed from the list of stored messages
     * if no message is stored with this sequence id, null is returned
     */
    public Message getMessage( long seq ) {
	return getMessage( seq, true );
    }

    /**
     * similar to GetMessage, except a copy of the message is returned
     *   and the message is not removed from the list
     */
    public Message peekMessage( long seq ) {
	return getMessage( seq, false );
    }

    /**
     * returns a copy of the stored message with the lowest sequence id
     * if delete_msg is true, then the message is removed from the
     *   the list of stored messages, otherwise the message is not
     *   removed from the list
     * if their are no messages stored, null is returned
     */
    private Message getFirstMessage( boolean delete_msg ) {
	synchronized( messages ) {
	    if ( isEmpty() ) {
		return null;
	    } else {
		Object temp_obj = messages.firstElement();
		if ( temp_obj instanceof Entry ) {
		    Message ret_val = ((Entry) temp_obj).getMsg().copy();
		    messages.removeElementAt(0);
		    return ret_val;
		} else {
		    System.err.println( "Error: (TOTAL_OLD) SavedMessages.getFirstMessage() - could not cast element of \"messages\" to an Entry" );
		    return null;
		} // if ( temp_obj instanceof Entry )
	    }
	} // synchronized( messages )
    }

    /**
     * returns the stored message with the lowest sequence id;
     *   the message is then removed from the list of stored messages
     * if their are no messages stored, null is returned
     */
    public synchronized Message getFirstMessage() {
	return getFirstMessage( true );
    }

    /**
     * similar to GetFirstMessage, except a copy of the message is returned
     *   and the message is not removed from the list
     */
    public Message peekFirstMessage() {
	return getFirstMessage( false );
    }

    /**
     * returns the lowest sequence id of the messages stored
     * if no messages are stored, -1 is returned
     */
    public long getFirstSeq() {
	synchronized( messages ) {
	    if ( isEmpty() ) {
		return -1;
	    } else {
		Object temp_obj = messages.firstElement();
		if ( temp_obj instanceof Entry ) {
		    return ((Entry) temp_obj).getSeq();
		} else {
		    System.err.println( "Error: (TOTAL_OLD) SavedMessages.getFirstSeq() - could not cast element of \"messages\" to an Entry " );
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
	synchronized( messages ) {
	    messages.removeAllElements();
	}
    }
} // class SavedMessages


/**
 * class MessageAcks
 *
 * Used by sequencer to store cumulative acknowledgements of broadcast messages
 *   sent to the group in this view
 */
class MessageAcks {

    // TODO: may also want to store some sort of timestamp in each Entry (maybe)
    /**
     * class Entry  (inner class)
     *
     * object type to store cumulative acknowledgements using a member's Address
     *   and the sequence id of a message
     */
    class Entry {
	public  Address        addr;
	public  long           seq;

	public Entry( Address addr, long seq ) {
	    this.addr = addr;
	    this.seq = seq;
	}

	public Entry ( Address addr ) {
	    this.addr = addr;
	    this.seq = -1;  // means that no acknowledgements have been made yet
	}
    } // class Entry

    // Vector of "Entry"s representing cumulative acknowledgements for each member of the group
    private Vector acks;

    private SavedMessages  message_history;  // history of broadcast messages sent


    /**
     * Constructor - creates a Vector of "Entry"s given a Vector of "Address"es for the members
     */
    public MessageAcks( Vector members ) {
	acks = new Vector();

	// initialize the message history to contain no messages
	message_history = new SavedMessages();

	// insert slots for each member in the acknowledgement Vector
	reset( members );
    }

    /**
     * resets acknowledgement Vector with "Entry"s using the given Vector of "Address"es
     * also clears the message history
     */
    public synchronized void reset( Vector members ) {
	clear();

	// initialize Vector of acknowledgements (no acks for any member)
	int num_members = members.size();
	for( int i=0; i<num_members; i++ ) {
	    Object temp_obj = members.elementAt(i);
	    if ( temp_obj instanceof Address ) {
		acks.addElement( new Entry( (Address) temp_obj ) );
	    } else {
		System.err.println( "Error: (TOTAL_OLD) MessageAcks.reset() - could not cast element of \"members\" to an Address object" );
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
    private Entry getEntry( Address addr ) {
	synchronized( acks ) {
	    // look for this addreess in the acknowledgement Vector
	    int size = acks.size();
	    for( int i=0; i<size; i++ ) {
		Object temp_obj = acks.elementAt(i);
		if ( temp_obj instanceof Entry ) {
		    Entry this_entry = (Entry) temp_obj;
		    if ( (this_entry.addr).equals(addr) ) {
			// the given Address matches this entry
			return this_entry;
		    }
		} else {
		    System.err.println( "Error: (TOTAL_OLD) MessageAcks.getEntry() - could not cast element of \"acks\" to an Entry" );
		} // if ( temp_obj instanceof Entry )
	    }

	    // if we get here, we didn't find this Address
	    return null;
	}
    }

    /**
     * sets the sequence id for the given Address to the given value
     * note: if the current sequence value for this host is greater than
     *   the given value, the sequence for this member is NOT changed
     *   (i.e. it will only set it to a larger value)
     * if the given Address is not found in the member list,
     *   nothing is changed
     */
    public void setSeq( Address addr, long seq ) {
	Entry this_entry = getEntry( addr );
	if ( (this_entry != null) && (this_entry.seq < seq) ) {
	    this_entry.seq = seq;

	    // try to remove any messages that we don't need anymore
	    truncateHistory();
	}
    }

    /**
     * returns the sequence id of the "latest" cumulative acknowledgement
     *   for the specified Address
     * if the Address is not found in the member list, a negative value
     *   is returned
     * note: the value returned may also be negative if their have been
     *       no acknowledgements from the given address
     */
    public long getSeq( Address addr ) {
	Entry this_entry = getEntry( addr );
	if ( this_entry == null ) {
	    return -2;  // TODO: change this to something else (e.g. constant) later  (maybe)
	} else {
	    return this_entry.seq;
	}
    }

    /**
     * returns the message in the history that matches the given sequence id
     * returns null if no message exists in the history with this sequence id
     */
    public Message getMessage( long seq ) {
	return message_history.peekMessage( seq );
    }

    /**
     * adds the given message (with the specified sequence id) to the
     *   message history
     * if the given sequence id already exists in the message history,
     *   the message is NOT added
     */
    public void addMessage( Message msg, long seq ) {
	message_history.insertMessage( msg, seq );
    }

    /**
     * returns the minimum cumulative acknowledged sequence id from all the members
     *   (i.e. the greatest sequence id cumulatively acknowledged by all members)
     */
    private long getLowestSeqAck() {
	synchronized( acks ) {
	    long ret_val = -10;  // start with a negative value

	    int size = acks.size();
	    for( int i=0; i<size; i++ ) {
		Object temp_obj = acks.elementAt(i);
		if ( temp_obj instanceof Entry ) {
		    long this_seq = ((Entry) temp_obj).seq;
		    if ( this_seq < ret_val ) {
			ret_val = this_seq;
		    }
		} else {
		    System.err.println( "Error: (TOTAL_OLD) MessageAcks.getLowestSeqAck() - could not cast element of \"acks\" to an Entry (index=" + i + ')' );
		    return -1;
		}
	    }

	    return ret_val;
	}
    }

    /**
     * removes messages from the history that have been acknowledged
     *   by all the members of the group
     */
    private synchronized void truncateHistory() {
	long lowest_ack_seq = getLowestSeqAck();
	if ( lowest_ack_seq < 0 ) {
	    // either no members, or someone has not received any messages yet
	    //   either way, do nothing
	    return;
	}

	// don't want message_history being altered during this operation
	synchronized( message_history ) {
	    long lowest_stored_seq;
	    // keep deleting the oldest stored message for as long as we can
	    while( ((lowest_stored_seq = message_history.getFirstSeq()) >=0) &&
		   (lowest_stored_seq > lowest_ack_seq) ) {
		// we can delete the oldest stored message
		message_history.getFirstMessage();
	    }
	} // synchronized( message_history )
    }
} // class MessageAcks


/*****************************************************************************
 * class TOTAL_OLD extends Protocol
 *
 * TODO: (more comments)
 * Sequencer based total ordering protocol layer
 *   - requires the following layers "below" it in the stack
 *     (or layers with equivalent functionality):
 *       GMS, FD, PING, UDP, ...
 @author Manish Sambhu mms21@cornell.edu Spring 1999
 ****************************************************************************/
public class TOTAL_OLD extends Protocol {
    // the unique name of the protocol
    private final static String PROTOCOL_NAME = "TOTAL_OLD";

    private Address        local_addr = null;
    private Vector         members = new Vector();  // note: members should never be null
                                                    //   (because of synchronized blocks)

    /**
     * next_seq_id
     *   the sequence id of the next message we expect to receive
     *   note: this value is only meaningful when non-negative
     */
    private long           next_seq_id = -1;

    /**
     * next_seq_id_to_assign
     *   used only by the sequencer to assign sequence ids to requests
     *     and resend them to the group
     *   note: this value is only meaningful when non-negative
     */
    private long           next_seq_id_to_assign = -1;

    private final static long INIT_SEQ_ID = 10;  // this value is pretty much arbitrary (should be positive though)

    /**
     * queued_messages
     *   broadcast messages that we received that we are storing so that we can
     *   deterministically order the messages based on their sequence ids
     */
    private SavedMessages  queued_messages = new SavedMessages();

    /**
     * ack_history
     *   used only by the sequencer
     *   stores the cumulative acks for each member of the group
     *   also stores messages that may be needed for resend requests
     *     (i.e. messages that have not been acked by all group members)
     */
    private MessageAcks    ack_history = null;

    /**
     * retrans_thread
     *   thread that handles sending requests to the sequencer for messages
     *     that may not have been received but were expected to arrive
     */
    private TotalRetransmissionThread   retrans_thread = new TotalRetransmissionThread( this );


    /**
     * returns the unique name of this protocol
     */
    public String  getName() {
	return PROTOCOL_NAME;
    }




    public void start() throws Exception {
        // Start work
        retrans_thread.start();
    }

    public void stop() {
        // stop the retransmission thread
        retrans_thread.stopResendRequests();
    }


    /** Just remove if you don't need to reset any state */
    public void reset() {
	// TODO: find out when this would be called, maybe do more here

	// don't accept any messages until we receive a TOTAL_NEW_VIEW message from the sequencer
	next_seq_id = -1;
	// clear (i.e. delete) any messages that did not get propagated up
	queued_messages.clearMessages();

	// reset the retransmission thread state
	retrans_thread.reset();
    }


    /**
     * returns the next sequence id expected to be received in this view
     */
    protected long getNextSeqID() {
	return next_seq_id;
    }


    /**
     * returns the sequence id of the "first" queued message
     *   (i.e. the lowest seq id queued)
     * returns -1 if no messages are queued
     */
    protected long getFirstQueuedSeqID() {
	return queued_messages.getFirstSeq();
    }


    /**
     * handles an Event coming up the Protocol Stack
     */
    public void up(Event evt) {
        Message msg;

        //System.out.println("UP: " + evt);

	Object temp_obj;  // used for type checking before performing casts
        switch( evt.getType() ) {

	case Event.SET_LOCAL_ADDRESS:
	    temp_obj = evt.getArg();
	    if ( temp_obj instanceof Address ) {
		local_addr = (Address) temp_obj;
	    } else {
		System.err.println( "Error: Total.up() - could not cast local address to an Address object" );
	    }
	    break;

        case Event.MSG:
	    // get the message and the header for the TOTAL_OLD layer
	    temp_obj = evt.getArg();
	    if ( temp_obj instanceof Message ) {
		msg = (Message) temp_obj;
		temp_obj = msg.removeHeader(getName());
		if ( temp_obj instanceof TotalHeader ) {
		    TotalHeader hdr = (TotalHeader) temp_obj;

		    // switch on the "command" defined by the header
		    switch( hdr.total_header_type ) {

		    case TotalHeader.TOTAL_UNICAST:
			// don't process this message, just pass it up (TotalHeader header already removed)
			passUp(evt);
			return;

		    case TotalHeader.TOTAL_BCAST:
			handleBCastMessage( msg, hdr.seq_id );
			break;

		    case TotalHeader.TOTAL_REQUEST:
			// if we are the sequencer, respond to this request
			if ( isSequencer() ) {
			    handleRequestMessage( msg );
			}
			break;

		    case TotalHeader.TOTAL_NEW_VIEW:
			// store the sequence id that we should expect next
			next_seq_id = hdr.seq_id;

			// TODO: need to send some sort of ACK or something to the sequencer (maybe)
			break;

		    case TotalHeader.TOTAL_CUM_SEQ_ACK:
			// if we are the sequencer, update state
			if ( isSequencer() ) {
			    temp_obj = msg.getSrc();
			    if ( temp_obj instanceof Address ) {
				ack_history.setSeq( (Address) temp_obj, hdr.seq_id );
			    } else {
				System.err.println( "Error: TOTAL_OLD.Up() - could not cast source of message to an Address object (case TotalHeader.TOTAL_CUM_SEQ_ACK)" );
			    }
			}
			break;

		    case TotalHeader.TOTAL_RESEND:
			// if we are the sequencer, respond to this request
			if ( isSequencer() ) {
			    handleResendRequest( msg, hdr.seq_id );
			}
			break;

		    default:
			// unrecognized header type - discard message
			System.err.println( "Error: TOTAL_OLD.up() - unrecognized TotalHeader in message - " + hdr.toString() );
			return;  // don't let it call passUp()
		    } // switch( hdr.total_header_type )
		} else {
		    System.err.println( "Error: TOTAL_OLD.up() - could not cast message header to TotalHeader (case Event.MSG)" );
		}  // if ( temp_obj instanceof TotalHeader )
	    } else {
		System.err.println( "Error: TOTAL_OLD.up() - could not cast argument of Event to a Message (case Event.MSG)" );
	    }  // if ( temp_obj instanceof Address )

            //System.out.println("The message is " + msg);
            return;  // don't blindly pass up messages immediately (if at all)

        // begin mms21
            /*
        case Event.BECOME_SERVER:
            System.out.println( "Become Server event passed up to TOTAL_OLD (debug - mms21)" );
            break;
            */

	case Event.TMP_VIEW:  // TODO: this may be temporary
        case Event.VIEW_CHANGE:
            System.out.println( "View Change event passed up to TOTAL_OLD (debug - mms21)" ); 
            View new_view = (View) evt.getArg();
	    members = new_view.getMembers();
	     {
		// print the members of this new view
		System.out.println( "New view members (printed in TOTAL_OLD):" );
		int view_size = members.size();
		for( int i=0; i<view_size; i++ ) {
		    System.out.println( "  " + members.elementAt(i).toString() );
		}
	    }

	    // reset the state for total ordering for this new view
	    reset();

	    // if we are the sequencer in this new view, send a new
	    //   TOTAL_NEW_VIEW message to the group
	    if ( isSequencer() ) {
		// we are the sequencer in this new view
		 System.err.println( "TOTAL_OLD.up() - I am the sequencer of this new view" );

		// we need to keep track of acknowledgements messages
		ack_history = new MessageAcks( members );

		// start assigning messages with this sequence id
		next_seq_id_to_assign = INIT_SEQ_ID;

		// send a message to the group with the initial sequence id to expect
		Message new_view_msg = new Message( null, local_addr, null );
		new_view_msg.putHeader(getName(),  new TotalHeader( TotalHeader.TOTAL_NEW_VIEW, next_seq_id_to_assign ) );
		passDown( new Event( Event.MSG, new_view_msg ) );
	    }

            break;
        // end mms21

        default:
            break;
        } // switch( evt.getType() )

        passUp(evt);            // Pass up to the layer above us
    }


    /**
     * passes up (calling passUp()) any stored messages eligible according to
     *   the total ordering property
     */
    private synchronized int passUpMessages() {
	if ( next_seq_id < 0 ) {
	    // don't know what to pass up so don't pass up anything
	    return 0;
	}

	long lowest_seq_stored = queued_messages.getFirstSeq();
	if ( lowest_seq_stored < 0 ) {
	    // there are no messages stored
	    return 0;
	}
	if ( lowest_seq_stored < next_seq_id ) {
	    // it is bad to have messages stored that have a lower sequence id than what
	    //   we are expecting
	    System.err.println( "Error: TOTAL_OLD.passUpMessages() - next expected sequence id (" + next_seq_id + ") is greater than the sequence id of a stored message (" + lowest_seq_stored + ')' );
	    return 0;
	} else if ( next_seq_id == lowest_seq_stored ) {
	    // we can pass this first message up the Protocol Stack
	    Message msg = queued_messages.getFirstMessage();
	    if ( msg == null ) {
		System.err.println( "Error: TOTAL_OLD.passUpMessages() - unexpected null Message retrieved from stored messages" );
		return 0;
	    }
	    passUp( new Event( Event.MSG, msg ) );

	    // increment the next expected sequence id
	    next_seq_id++;

	    return (1 + passUpMessages());
	} else {
	    /* don't drop messages, it should be requesting resends
	    // all messages stored have sequence ids greater than expected
	    if ( queued_messages.getSize() > 10 ) {
		 {
		    System.err.println( "WARNING: TOTAL_OLD.passUpMessages() - more than 10 messages saved" );
		    System.err.println( "Dropping sequence id: " + next_seq_id );
		}
		next_seq_id++;
		return passUpMessages();
	    }
	    */
	    return 0;
	}
    }


    private long last_request_time = -1;
    /**
     * stores the message in the list of messages. also passes up any messages
     *   if it can (i.e. if it satisfies total ordering).
     * if the sequence for the next expected message is unknown, the message is
     *   discarded without being stored
     */
    private synchronized void handleBCastMessage( Message msg, long seq ) {
	/* store the message anyway, hopefully we'll get a TOTAL_NEW_VIEW message later
	if ( next_seq < 0 ) {
	    // don't know what sequence id to expect
	     System.err.println( "TOTAL_OLD.handleBCastMessage() - received broadcast message but don't know what sequence id to expect" );
	    return;
	}
	*/

	if ( seq < next_seq_id ) {
	    // we're expecting a message with a greater sequence id
	    //   hopefully, we've already seen this message so just ignore it
	    return;
	}

	// save this message in the list of received broadcast messages
	queued_messages.insertMessage( msg, seq );

	// try to pass up any messages
	int num_passed = passUpMessages();
// TODO: this if is temporary (debug)
if ( num_passed > 1 )
	 System.err.println( "TOTAL_OLD.handleBCastMessage() - " + num_passed + " message(s) passed up the Protocol Stack" );

        /* this is handles by the retransmission thread now
	// see if we may need to issue any resend requests
	if ( queued_messages.getSize() > 1 ) {  // TODO: magical constant N?
	    Address sequencer = getSequencer();
	    //Object sequencer = msg.makeReply().getSrc();  // test (debug)
	    if ( sequencer == null ) {
		// couldn't get the sequencer of the group
		System.err.println( "TOTAL_OLD.handleBCastMessage() - couldn't determine sequencer to send a TOTAL_RESEND request" );
		return;
	    }

	    if ( local_addr == null ) {
		// don't know local address, can't set source of message
		System.err.println( "TOTAL_OLD.handleBCastMessage() - do not know local address so cannot send resend request for message " + seq );
		return;
	    }

	    long time_now = System.currentTimeMillis();
	    if ( (last_request_time >= 0) && ((time_now - last_request_time) < 1000) ) {
		return;
	    } else {
		last_request_time = time_now;
	    }
	    // request a resend request for all missing sequence ids
	    //   from the next one expected up to the "earliest" queued one
	    // TODO: (works a little different now)
	    long first_queued_seq = queued_messages.getFirstSeq();
	    long max_resend_seq = ((next_seq_id + 10) > first_queued_seq) ? first_queued_seq : (next_seq_id + 10);
	    for( long resend_seq=next_seq_id; resend_seq<=max_resend_seq ; resend_seq++ ) {
		Message resend_msg = new Message( sequencer, local_addr, null );
		resend_msg.putHeader(getName(),  new TotalHeader( TotalHeader.TOTAL_RESEND, resend_seq ) );
		passDown( new Event( Event.MSG, resend_msg ) );
		 System.err.println( "TOTAL_OLD.handleBCastMessage() - resend requested for message " + resend_seq );
	    }
	}
	*/
    }


    /**
     * respond to a request message by broadcasting a copy of the message to the group
     *   with the next sequence id assigned to it
     * if we do not know what the next sequence id is to assign, discard the message
     */
    private synchronized void handleRequestMessage( Message msg ) {
	if ( next_seq_id_to_assign < 0 ) {
	    // we cannot assign a valid sequence id
	    System.err.println( "Error: TOTAL_OLD.handleRequestMessage() - cannot handle request... do not know what sequence id to assign" );
	    return;
	}

	// make the message a broadcast message to the group
	msg.setDest( null );

	// set the source of the message to be me
	msg.setSrc( local_addr );

	// add the sequence id to the message
	msg.putHeader(getName(),  new TotalHeader( TotalHeader.TOTAL_BCAST, next_seq_id_to_assign ) );

	// store a copy of this message is the history
	Message msg_copy = msg.copy();
	ack_history.addMessage( msg_copy, next_seq_id_to_assign );

	// begin debug
	 {
	    Object header = msg_copy.getHeader(getName());
	    if ( !(header instanceof TotalHeader) ) {
		System.err.println( "Error: TOTAL_OLD.handleRequestMessage() - BAD: stored message that did not contain a TotalHeader - " + next_seq_id_to_assign );
	    }
	} //
	// end debug

	// increment the next sequence id to use
	next_seq_id_to_assign++;

	// pass this new Message (wrapped in an Event) down the Protocol Stack
	passDown( new Event( Event.MSG, msg ) );
    }


    /**
     * respond to a request to resend a message with the specified sequence id
     */
    private synchronized void handleResendRequest( Message msg, long seq ) {
	 System.err.println( "TOTAL_OLD.handleRequestMessage() - received resend request for message " + seq );

	/* just rebroadcast for now because i can't get the source - this is bad (TODO: fix this) 
	Object requester = msg.makeReply().getSrc();  // Address? of requester - test (debug)
	/*
	Object temp_obj = msg.getSrc();
	if ( temp_obj instanceof Address ) {
	    Address requester = (Address) temp_obj;
	} else {
	    System.err.println( "Error: TOTAL_OLD.handleResendRequest() - could not cast source of message to an Address" );
	    return;
	}
	* /
	if ( requester == null ) {
	    // don't know who to send this back to
	    System.err.println( "TOTAL_OLD.handleResendRequest() - do not know who requested this resend request for sequence " + seq );
	    return;
	}
	*/
	Address requester = null;
// System.err.println( "TOTAL_OLD: got here - 1" );
	Message resend_msg = ack_history.getMessage( seq );
// System.err.println( "TOTAL_OLD: got here - 2" );
	if ( resend_msg == null ) {
	    // couldn't find this message in the history
	    System.err.println( "TOTAL_OLD.handleResendRequest() - could not find the message " + seq + " in the history to resend" );
	    return;
	}
	resend_msg.setDest( requester );

	// note: do not need to add a TotalHeader because it should already be a
	//       TOTAL_BCAST message
	// begin debug
	 {
	    Object header = resend_msg.getHeader(getName());
	    if ( header instanceof TotalHeader ) {
		//System.err.println( "TOTAL_OLD: resend msg GOOD (header is TotalHeader) - " + seq );
	    } else {
		System.err.println( "TOTAL_OLD: resend msg BAD (header is NOT a TotalHeader) - " + seq );
	    }
	} //
	// end debug

	passDown( new Event( Event.MSG, resend_msg ) );
	 System.err.println( "TOTAL_OLD.handleResendRequest() - responded to resend request for message " + seq );
    }


    /**
     * handles an Event coming down the Protocol Stack
     */
    public void down(Event evt) {
        Message msg;

        //System.out.println("DOWN: " + evt);

        switch( evt.getType() ) {

        case Event.VIEW_CHANGE:
	    // this will probably never happen
	    System.err.println( "NOTE: VIEW_CHANGE Event going down through " + PROTOCOL_NAME );

            Vector new_members=((View)evt.getArg()).getMembers();
            synchronized(members) {
                members.removeAllElements();
                if(new_members != null && new_members.size() > 0)
                    for(int i=0; i < new_members.size(); i++)
                        members.addElement(new_members.elementAt(i));
            }
            break;

        case Event.MSG:
	    Object temp_obj = evt.getArg();
	    if ( temp_obj instanceof Message ) {
		msg = (Message) temp_obj;

		// note: a TotalHeader is added to every message (Event.MSG)
		//   that is sent

		// check if this is a broadcast message
		if ( msg.getDest() == null ) {
		    // yes, this is a broadcast message

		    // send out a request for a message to be broadcast
		    //   (the sequencer will handle this)
		    Address sequencer = getSequencer();
		    if ( sequencer != null ) {
			// we only need to send the request to the sequencer (who will broadcast it)
			msg.setDest( sequencer );
		    } else {
			// couldn't find sequencer of the group
			// for now, just send it to the original destination
			//   (don't need to do anything here)
		    }

		    //msg.putHeader(getName(),  TotalHeader.getRequestHeader() );
		    msg.putHeader(getName(), new TotalHeader(TotalHeader.TOTAL_REQUEST, -1));


		} else {
		    // this is a point to point unicast message so just send it to its original destination
		    msg.putHeader(getName(),  new TotalHeader( TotalHeader.TOTAL_UNICAST, -1 ) );  // sequence id in header is irrelevant
		}
	    } else {
		System.err.println( "Error: TOTAL_OLD.down() - could not cast argument of Event to a Message (case Event.MSG)" );
	    } // if ( temp_obj instanceof Message )
            break;
            
        default:
            break;
        } // switch( evt.getType() )

        passDown(evt);          // Pass on to the layer below us

    }


    /**
     * returns true if we are currently the sequencer of the group;
     *   returns false otherwise
     * note: returns false if our local address is unknown, or the list of members is
     *   empty
     */
    private boolean isSequencer() {
	if ( local_addr == null ) {
	    // don't know my own local address
	    System.err.println( "TOTAL_OLD.isSequencer() - local address unknown!" );
	    return false;
	}

	synchronized( members ) {
	    if ( members.size() == 0 ) {
		// there are no members listed for the group (not even myself)
		System.err.println( "TOTAL_OLD.isSequencer() - no members!" );
		return false;
	    }

	    Object temp_obj = members.elementAt(0);
	    if ( temp_obj instanceof Address ) {
		Address seq_addr = (Address) temp_obj;
		return local_addr.equals(seq_addr);
	    } else {
		System.err.println( "Error: TOTAL_OLD.isSequencer() - could not cast element of \"members\" to an Address" );
		return false;
	    } // if ( temp_obj instanceof Address )
	}
    }


    /**
     * returns the Address of the local machine
     * returns null if it is not known yet
     */
    protected Address getLocalAddr() {
	return local_addr;
    }


    /**
     * returns the address of the current sequencer of the group
     *   returns null if the list of members is empty
     */
    protected Address getSequencer() {
	synchronized( members ) {
	    if ( members.size() == 0 ) {
		System.err.println( "TOTAL_OLD.getSequencer() - no members" );
		return null;
	    } else {
		Object temp_obj = members.elementAt(0);
		if ( temp_obj instanceof Address ) {
		    return (Address) temp_obj;
		} else {
		    System.err.println( "Error: TOTAL_OLD.getSequencer() - could not cast first element of \"members\" to an Address" );
		    return null;
		}
	    }
	}
    }





    /**
 * class TotalHeader
 *
 * The header that is prepended to every message passed down through the TOTAL_OLD layer
 *   and removed (and processed) from every message passed up through the TOTAL_OLD layer
 */
    public static class TotalHeader extends Header {
	// Total message types
	public final static int TOTAL_UNICAST      = 0;   // a point to point unicast message that should not be processed by TOTAL_OLD
	public final static int TOTAL_BCAST        = 1;   // message broadcast by the sequencer
	public final static int TOTAL_REQUEST      = 2;   // request for a message to be broadcast
	public final static int TOTAL_NEW_VIEW     = 3;   // reset with a view change, sequence number also reset
	public final static int TOTAL_NEW_VIEW_ACK = 4;   // acknowledgement of new view and sequence id
	public final static int TOTAL_CUM_SEQ_ACK  = 5;   // cumulatively acknowledge the reception of messages up to a sequence id
	public final static int TOTAL_SEQ_ACK      = 6;   // acknowledge the reception of a message with a certain sequence id  (probably won't be used)
	public final static int TOTAL_RESEND       = 7;   // request the message with a certain sequence id

	public int total_header_type;

	// TODO: finish commenting meaning of seq_id for different header types
	/**
	 * seq_id
	 *   for TOTAL_BCAST messages, seq_id is used to determine the order of messages
	 *     in the view. seq_id is expected to increment by one for each new message
	 *     sent in the current view. this sequence id is reset with each new view.
	 *     the GMS layer should make sure that messages sent in one view are not
	 *     received in another view.
	 *   for TOTAL_REQUEST messages, seq_id is not used
	 *   for TOTAL_NEW_VIEW, seq_id is the sequence id that the sequencer of this
	 *     view will use for the first message broadcast to the group
	 *     (i.e. the expected sequence id is "reset" to this value)
	 *   for TOTAL_NEW_VIEW_ACK, 
	 *   for TOTAL_CUM_SEQ_ACK messages, the seq_id is the cumulative sequence id
	 *     that the sender has received
	 *   for TOTAL_SEQ_ACK messages, seq_id is the sequence id that is being acknowledged
	 *   for TOTAL_RESEND, seq_id is the sequence id to be sent again
	 */
	public long seq_id;  // see use above (varies between types of headers)


	public TotalHeader() {} // used for externalization

	public TotalHeader( int type, long seq ) {
	    switch( type ) {
	    case TOTAL_UNICAST:
	    case TOTAL_BCAST:
	    case TOTAL_REQUEST:
	    case TOTAL_NEW_VIEW:
	    case TOTAL_NEW_VIEW_ACK:
	    case TOTAL_CUM_SEQ_ACK:
	    case TOTAL_SEQ_ACK:
	    case TOTAL_RESEND:
		// the given type is a known one
		total_header_type = type;
		break;

	    default:
		// this type is unknown
		System.err.println( "Error: TotalHeader.TotalHeader() - unknown TotalHeader type given: " + type );
		total_header_type = -1;
		break;
	    }

	    seq_id = seq;
	}

	//static TotalHeader getRequestHeader() {
	//return new TotalHeader( TOTAL_REQUEST, -1 );  // sequence id is irrelevant
	//}

	public String toString() {
	    String type = "";
	    switch( total_header_type ) {
	    case TOTAL_UNICAST:
		type = "TOTAL_UNICAST";
		break;

	    case TOTAL_BCAST:
		type = "TOTAL_BCAST";
		break;

	    case TOTAL_REQUEST:
		type = "TOTAL_REQUEST";
		break;

	    case TOTAL_NEW_VIEW:
		type = "NEW_VIEW";
		break;

	    case TOTAL_NEW_VIEW_ACK:
		type = "NEW_VIEW_ACK";
		break;

	    case TOTAL_CUM_SEQ_ACK:
		type = "TOTAL_CUM_SEQ_ACK";
		break;

	    case TOTAL_SEQ_ACK:
		type = "TOTAL_SEQ_ACK";
		break;

	    case TOTAL_RESEND:
		type = "TOTAL_RESEND";
		break;

	    default:
		type = "UNKNOWN TYPE (" + total_header_type + ')';
		break;
	    }

	    return "[ TOTAL_OLD: type=" + type + ", seq=" + seq_id + " ]";
	}



	public void writeExternal(ObjectOutput out) throws IOException {
	    out.writeInt(total_header_type);
	    out.writeLong(seq_id);
	}
    
    

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    total_header_type=in.readInt();
	    seq_id=in.readLong();
	}



    } // class TotalHeader



} // class TOTAL_OLD


/*****************************************************************************
 * class TotalRetransmissionThread
 *
 * thread that handles retransmission for the TOTAL_OLD protocol
 ****************************************************************************/
class TotalRetransmissionThread extends Thread {
    // state variables to determine when and what to request
    private long    last_retrans_request_time;  // last time (in milliseconds) that we sent a resend request
    private long    last_requested_seq;         // latest sequence id that we have requested

    // retransmission constants
    final private static long   polling_delay = 1000;  // how long (in milliseconds) to sleep before rechecking for resend
    final private static long   resend_timeout = 2000; // amount of time (in milliseconds) to wait on a resend request before resending another request
    final private static int    max_request = 10;      // maximum number of resend request to send out in one iteration

    // reference to the parent TOTAL_OLD protocol instance
    private TOTAL_OLD   prot_ptr;

    // flag to specify if the thread should continue running
    private boolean is_running;


    /**
     * constructor
     *
     * creates and initializes a retransmission thread for the
     *   specified instance of a TOTAL_OLD protocol
     */
    public TotalRetransmissionThread( TOTAL_OLD parent_prot ) {
	if ( parent_prot != null ) {
	    prot_ptr = parent_prot;
	} else {
	    // parent thread not specified
	    System.err.println( "Error: TotalRetransmissionThread.TotalRetransmissionThread() - given parent protocol reference is null\n  (FATAL ERROR - TOTAL_OLD protocol will not function properly)" );

	    // prevent the run method from doing any work
	    is_running = false;
	}

	// initialize the state variables
	reset();

	// let the thread make resend requests
	is_running = true;
    }

    /**
     * resets the state of the thread as if it was just started
     * the thread will assume that there were no resend requests make
     */
    public void reset() {
	// we have not made any resend requests for any messages
	last_retrans_request_time = -1;
	last_requested_seq = -1;
    }


    /**
     * send a resend request to the given sequencer (from the given local_addr)
     *   for the given sequence id
     */
    private void sendResendRequest( Address sequencer, Address local_addr, long seq_id ) {
	Message resend_msg = new Message( sequencer, local_addr, null );
	resend_msg.putHeader(getName(),  new TOTAL_OLD.TotalHeader( TOTAL_OLD.TotalHeader.TOTAL_RESEND, seq_id ) );
	prot_ptr.passDown( new Event( Event.MSG, resend_msg ) );

	// debug
	System.err.println( "TotalRetransmissionThread.resend() - resend requested for message " + seq_id );
    }


    /**
     * checks if a resend request should be made to the sequencer. if a request needs
     *   to be made, it makes the appropriate requests with the parameters specified
     *   by the constants in this class
     */
    private void checkForResend() {
	long first_seq_id = prot_ptr.getFirstQueuedSeqID(); // sequence id of first queued message
	/*
	// begin debug
	System.out.println( "DEBUG (TotalRetransmissionThread) - first_seq_id = " + first_seq_id );
	// end debug
	*/
	if ( first_seq_id >= 0 ) {
	    // there is at least one message in the queue

	    long next_seq_id = prot_ptr.getNextSeqID();     // next sequence id expected from the group
	    if ( (next_seq_id < first_seq_id) ) {  // TODO: handle case to resend TOTAL_NEW_VIEW message
		// there are messages that we received out of order
		//System.err.println( "DEBUG (TotalRetransmissionThread) - there are messages queued" ); // debug

		// see if it is time to send a request
		long time_now = System.currentTimeMillis();
		if ( (next_seq_id > last_requested_seq) ||
		     (time_now > (last_retrans_request_time + resend_timeout)) ||
		     (last_retrans_request_time < 0) ) {
		    // send a resend request to the sequencer
		    //System.err.println( "DEBUG (TotalRetransmissionThread) - sending resend requests" ); // debug
		    Address sequencer = prot_ptr.getSequencer();
		    if ( sequencer == null ) {
			System.out.println( "Error: (TOTAL_OLD) TotalRetransmissionThread.checkForResend() - could not determine sequencer to send a TOTAL_RESEND request" );

			return;
		    }

		    Address local_addr = prot_ptr.getLocalAddr();
		    if ( local_addr == null ) {
			System.out.println( "Warning: (TOTAL_OLD) TotalRetransmissionThread.checkForResend() - local address not specified in TOTAL_RESEND request... attempting to send requests anyway" );
		    }

		    long temp_long = (next_seq_id + max_request);   // potential max seq id to request (exclusive)
		    long last_resend_seq_id = (temp_long > first_seq_id) ? first_seq_id : temp_long;
		    for( long resend_seq=next_seq_id; resend_seq<last_resend_seq_id ; resend_seq++ ) {
			sendResendRequest( sequencer, local_addr, resend_seq );
		    }
		    // update state for this set of resend requests
		    last_retrans_request_time = time_now;
		    last_requested_seq = last_resend_seq_id;
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
	while( is_running ) {
	    // resend any requests if necessary
	    //System.err.println( "DEBUG (TotalRetransmissionThread) - heartbeat" ); // debug
	    checkForResend();

	    // wait before check again
	    try {
		sleep( polling_delay );
	    } catch( InterruptedException e ) {}    // do nothing if interrupted
	}
    }

    /**
     * stops the thread from making any further resend requests
     * note: the thread may not die immediately
     */
    public void stopResendRequests() {
	is_running = false;
    }
} // class TotalRetransmissionThread

// $Id: UNICAST.java,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

package org.jgroups.protocols;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Enumeration;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.log.Trace;



/**
 * Reliable unicast layer. Uses acknowledgement scheme similar to TCP to provide lossless transmission
 * of unicast messages (for reliable multicast see NAKACK layer). When a message is sent to a peer for
 * the first time, we add the pair <peer_addr, Entry> to the hashtable (peer address is the key). All
 * messages sent to that peer will be added to hashtable.peer_addr.sent_msgs. When we receive a
 * message from a peer for the first time, another entry will be created and added to the hashtable
 * (unless already existing). Msgs will then be added to hashtable.peer_addr.received_msgs.<p> This
 * layer is used to reliably transmit point-to-point messages, that is, either messages sent to a
 * single receiver (vs. messages multicast to a group) or for example replies to a multicast message. The 
 * sender uses an <code>AckSenderWindow</code> which retransmits messages for which it hasn't received
 * an ACK, the receiver uses <code>AckReceiverWindow</code> which keeps track of the lowest seqno
 * received so far, and keeps messages in order.<p>
 * Messages in both AckSenderWindows and AckReceiverWindows will be removed. A message will be removed from
 * AckSenderWindow when an ACK has been received for it and messages will be removed from AckReceiverWindow
 * whenever a message is received: the new message is added and then we try to remove as many messages as
 * possible (until we stop at a gap, or there are no more messages).
 * @author Bela Ban
 */
public class UNICAST extends Protocol implements AckSenderWindow.RetransmitCommand {
    boolean       operational=false;
    Vector        members=new Vector();
    Hashtable     connections=new Hashtable();   // Object (sender or receiver) -- Entries
    long[]        timeout={800,1600,3200,6400};  // for AckSenderWindow: max time to wait for missing acks
    Address       local_addr=null;
    TimeScheduler timer=null;                    // used for retransmissions (passed to AckSenderWindow)

    // if UNICAST is used without GMS, don't consult the membership on retransmit() if use_gms=false
    // default is true
    boolean       use_gms=true;
    int           window_size=-1;                // sliding window: max number of msgs in table
    int           min_threshold=-1;              // num under which table has to fall before we resume adding msgs
    



    class Entry {
        AckReceiverWindow  received_msgs=null;  // stores all msgs rcvd by a certain peer in seqno-order
        AckSenderWindow    sent_msgs=null;      // stores (and retransmits) msgs sent by us to a certain peer
        long               sent_msgs_seqno=getInitialSeqno();  // seqno for msgs sent by us


        void reset() {
            if(sent_msgs != null)
                sent_msgs.reset();
            if(received_msgs != null)
                received_msgs.reset();
        }


        public String toString() {
            StringBuffer sb=new StringBuffer();
            if(sent_msgs != null)
                sb.append("sent_msgs=" + sent_msgs + "\n");
            if(received_msgs != null)
                sb.append("received_msgs=" + received_msgs + "\n");	    
            return sb.toString();
        }
    }




    /** All protocol names have to be unique ! */
    public String  getName() {return "UNICAST";}


    public boolean setProperties(Properties props) {
        String     str;
	long[]     tmp;

        str=props.getProperty("timeout");
        if(str != null) {
	    tmp=Util.parseCommaDelimitedLongs(str);
	    if(tmp != null && tmp.length > 0)
		timeout=tmp;
            props.remove("timeout");
        }

        str=props.getProperty("window_size");
        if(str != null) {
            window_size=Integer.parseInt(str);
            props.remove("window_size");
        }

        str=props.getProperty("min_threshold");
        if(str != null) {
            min_threshold=Integer.parseInt(str);
            props.remove("min_threshold");
        }

	str=props.getProperty("use_gms");
        if(str != null) {
	    use_gms=new Boolean(str).booleanValue();
            props.remove("use_gms");
        }

        if(props.size() > 0) {
            System.err.println("UNICAST.setProperties(): these properties are not recognized:");
            props.list(System.out);
            return false;
        }

	// Some sanity checks
	if((window_size > 0 && min_threshold <= 0) || (window_size <= 0 && min_threshold > 0)) {
	    Trace.error("UNICAST.setProperties()",
			"window_size and min_threshold have to be both set if one of them is set");
	    return false;
	}
	if(window_size > 0 && min_threshold > 0 && window_size < min_threshold) {
	    Trace.error("UNICAST.setProperties()", "min_threshold (" + min_threshold + 
			") has to be less than window_size (" + window_size + ")");
	    return false;
	}
        return true;
    }

    public void start() throws Exception {
        timer=stack != null ? stack.timer : null;
        if(timer == null)
            throw new Exception("UNICAST.start(): timer is null");
    }

    public void stop() {
        removeAllConnections();
        operational=false;
    }


    public void up(Event evt) {
        Message        msg;
        Address        dst, src;
        UnicastHeader  hdr;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                dst=msg.getDest();
                src=msg.getSrc();
                if(dst == null || dst.isMulticastAddress())  // only handle unicast messages
                    break;  // pass up

                hdr=(UnicastHeader)msg.removeHeader(getName());
		if(hdr == null) break;
                switch(hdr.type) {
                    case UnicastHeader.DATA:      // received regular message
                        sendAck(src, hdr.seqno);
                        handleDataReceived(src, hdr.seqno, hdr.first, msg);
                        break;
                    case UnicastHeader.DATA_ACK:  // received ACK for previously sent message
                        handleAckReceived(src, hdr.seqno);
                        break;
                    default:
                        Trace.error("UNICAST.up()", "UnicastHeader type " + hdr.type + " not known !");
                        break;
                }
                return;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        passUp(evt);   // Pass up to the layer above us
    }





    public void down(Event evt) {
        Message msg;
        Object dst, mbr;
        Entry entry;
        UnicastHeader hdr;

        switch (evt.getType()) {

            case Event.MSG: // Add UnicastHeader, add to AckSenderWindow and pass down
                msg = (Message) evt.getArg();
                dst = msg.getDest();

                /* only handle unicast messages */
                if (dst == null || ((Address) dst).isMulticastAddress())
                    break;

                entry = (Entry) connections.get(dst);
                if (entry == null) {
                    entry = new Entry();
                    connections.put(dst, entry);
                }

                hdr = new UnicastHeader(UnicastHeader.DATA, entry.sent_msgs_seqno);
                if (entry.sent_msgs == null) { // first msg to peer 'dst'
                    hdr.first = true;
                    entry.sent_msgs = new AckSenderWindow(this, timeout, this);
                    if (window_size > 0)
                        entry.sent_msgs.setWindowSize(window_size, min_threshold);
                }
                msg.putHeader(getName(), hdr);

                if (Trace.debug)
                    Trace.info("UNICAST.down()", "[" + local_addr + "] --> DATA(" + dst + ": #" +
                            entry.sent_msgs_seqno + ", first=" + hdr.first + ")");

                if (Trace.copy)
                    entry.sent_msgs.add(entry.sent_msgs_seqno, msg.copy());  // add *including* UnicastHeader
                else
                    entry.sent_msgs.add(entry.sent_msgs_seqno, msg);         // add *including* UnicastHeader

                entry.sent_msgs_seqno++;
                return; // AckSenderWindow will send message for us

            case Event.BECOME_SERVER:
                operational = true;
                break;

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                Vector new_members = ((View) evt.getArg()).getMembers();
                Vector left_members;
                synchronized (members) {
                    left_members = Util.determineLeftMembers(members, new_members);
                    members.removeAllElements();
                    if (new_members != null)
                        members.addAll(new_members);
                }
	    
                // Remove all connections for members that left between the current view and the new view
                // See DESIGN for details
                if (use_gms && left_members.size() > 0) {
                    synchronized (connections) {
                        for (int i = 0; i < left_members.size(); i++) {
                            mbr = left_members.elementAt(i);
                            removeConnection(mbr);
                        }
                    }
                }
                break;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    /** Removes and resets from connection table (which is already locked) */
    void removeConnection(Object mbr) {
        Entry entry=(Entry)connections.get(mbr);
        if(entry != null) {
            entry.reset();
            if(Trace.trace)
                Trace.info("UNICAST.removeConnection()", "removed " + mbr + " from connection table");
        }
        connections.remove(mbr);
    }


    void removeAllConnections() {
        Entry entry;

        synchronized(connections) {
            for(Enumeration e=connections.elements(); e.hasMoreElements();) {
                entry=(Entry)e.nextElement();
                entry.reset();
            }
            connections.clear();
        }
    }




    /** Returns random initial sequence number between 1 and 100 */
    long getInitialSeqno() {
        long ret=(long)((Math.random() * 100) % 100);
        return ret;
    }



    /** Called by AckSenderWindow to resend messages for which no ACK has been received yet */
    public void retransmit(long seqno, Message msg) {
        Object  dst=msg.getDest();

        // bela Dec 23 2002:
        // this will remove a member on a MERGE request, e.g. A and B merge: when A sends the unicast
        // request to B and there's a retransmit(), B will be removed !

        //          if(use_gms && !members.contains(dst) && !prev_members.contains(dst)) {
        //              if(Trace.trace)
        //                  Trace.warn("UNICAST.retransmit()", "seqno=" + seqno + ":  dest " + dst + 
        //                             " is not member any longer; removing entry !");
        
        //              synchronized(connections) {
        //                  removeConnection(dst);
        //              }
        //              return;
        //          }
	
        if(Trace.debug)
            Trace.info("UNICAST.retransmit()", "[" + local_addr + "] --> XMIT(" + dst + ": #" + seqno + ")");

	if(Trace.copy)
	    passDown(new Event(Event.MSG, msg.copy()));
	else
	    passDown(new Event(Event.MSG, msg));
    }





    /**
     * Check whether the hashtable contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message. If first is false,
     * but we don't yet have hashtable.received_msgs, then just discard the message. If first is true, but
     * hashtable.received_msgs already exists, also discard the message (redundant message).
     */
    void handleDataReceived(Object sender, long seqno, boolean first, Message msg) {
        Entry    entry;
        Message  m;  

	if(Trace.debug) Trace.info("UNICAST.handleDataReceived()", "[" + local_addr +
				   "] <-- DATA(" + sender + ": #" + seqno + ", first=" + first);
	
        entry=(Entry)connections.get(sender);
        if(entry == null) {
            entry=new Entry();
            connections.put(sender, entry);
        }

        if(entry.received_msgs == null) {
            if(first)
                entry.received_msgs=new AckReceiverWindow(seqno);
            else {
                if(operational) {
                    if(Trace.trace)
                        Trace.warn("UNICAST.handleDataReceived()", "[" + local_addr + "] seqno " + seqno + " from " + 
                                   sender + " is not tagged as the first message sent by " + sender + 
                                   "; however, the table for received messages from " + sender + 
                                   " is still null ! We probably haven't received the first message from " 
                                   + sender + " ! Discarding message (operational=" + operational + ")");
                    return;
                }
            }
        }

        if(entry.received_msgs != null) {
            entry.received_msgs.add(seqno, msg);
        
            // Try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
            while((m=entry.received_msgs.remove()) != null)
                passUp(new Event(Event.MSG, m));
        }
    }




    /** Add the ACK to hashtable.sender.sent_msgs */
    void handleAckReceived(Object sender, long seqno) {
        Entry           entry;
        AckSenderWindow win;

        if(Trace.debug) Trace.info("UNICAST.handleAckReceived()", "[" + local_addr +
				   "] <-- ACK(" + sender + ": #" + seqno + ")");

        entry=(Entry)connections.get(sender);
        if(entry == null || entry.sent_msgs == null) {
            return;
        }
        win=entry.sent_msgs;
        win.ack(seqno); // removes message from retransmission
    }



    void sendAck(Address dst, long seqno) {
        Message ack=new Message(dst, null, null);
        ack.putHeader(getName(), new UnicastHeader(UnicastHeader.DATA_ACK, seqno));
        if(Trace.debug) Trace.info("UNICAST.sendAck()", "[" + local_addr + "] --> ACK(" + dst + ": #" + seqno + ")");
        passDown(new Event(Event.MSG, ack));
    }






    public static class UnicastHeader extends Header {
        static final int DATA=0;
        static final int DATA_ACK=1;
	
        int     type=DATA;
        long    seqno=0;   // First msg is 0
        boolean first=false;


        public UnicastHeader() {} // used for externalization
	
        public UnicastHeader(int type, long seqno) {
            this.type=type == DATA_ACK ? DATA_ACK : DATA;
            this.seqno=seqno;
        }
	
        public String toString() {
            return "[UNICAST: " + type2Str(type) + ", seqno=" + seqno + "]";
        }
	
        public String type2Str(int t) {
            switch(t) {
                case DATA: return "DATA";
                case DATA_ACK: return "DATA_ACK";
                default: return "<unknown>";
            }
        }
	
	
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(type);
            out.writeLong(seqno);
            out.writeBoolean(first);
        }
	
	
	
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	    type=in.readInt();
            seqno=in.readLong();
            first=in.readBoolean();
        }
    }
    
    
}

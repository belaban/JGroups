
package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.AckMcastSenderWindow;
import org.jgroups.stack.AckReceiverWindow;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;


/**
 * Simple Multicast ACK protocol. A positive acknowledgment-based protocol for reliable delivery of
 * multicast messages, which does not need any group membership service.
 * Basically works as follows:
 * <ul>
 * <li>Sender S sends multicast message M</li>
 * <li>When member P receives M, it sends back a unicast ack to S</li>
 * <li>When S receives the ack from P, it checks whether P is in its
 *     membership list. If not, P will be added. This is necessary to retransmit the next message
 *     sent to P.</li>
 * <li>When S sends a multicast message M, all members are added to a
 *     retransmission entry (containing all members to which the message
 *     was sent), which is added to a hashmap (keyed by seqno). Whenever
 *     an ack is received from receiver X, X will be removed from the
 *     retransmission list for the given seqno. When the retransmission
 *     list is empty, the seqno will be removed from the hashmap.</li>
 * <li>A retransmitter thread in the sender periodically retransmits
 *     (either via unicast, or multicast) messages for which no ack has
 *     been received yet</li>
 * <li>When a max number of (unsuccessful) retransmissions have been
 *     exceeded, all remaining members for that seqno are removed from
 *     the local membership, and the seqno is removed from the hashmap,
 *     ceasing all retransmissions</li>
 * </ul>
 * Advantage of this protocol: no group membership necessary, fast.
 * @author Bela Ban Aug 2002
 * @version $Id: SMACK.java,v 1.29 2008/05/14 12:29:07 belaban Exp $
 * <BR> Fix membershop bug: start a, b, kill b, restart b: b will be suspected by a.
 */
@Experimental @Unsupported
public class SMACK extends Protocol implements AckMcastSenderWindow.RetransmitCommand {
    long[]                 timeout=new long[]{1000,2000,3000};  // retransmit timeouts (for AckMcastSenderWindow)
    @Property
    int                    max_xmits=10;              // max retransmissions (if still no ack, member will be removed)
    final Set<Address>     members=new LinkedHashSet<Address>();      // contains Addresses
    AckMcastSenderWindow   sender_win=null;
    final Map<Address,AckReceiverWindow> receivers=new HashMap<Address,AckReceiverWindow>();   // keys=sender (Address), values=AckReceiverWindow
    final Map<Address,Integer>          xmit_table=new HashMap<Address,Integer>();  // keeps track of num xmits / member (keys: mbr, val:num)
    Address                local_addr=null;           // my own address
    long                   seqno=1;                   // seqno for msgs sent by this sender
    long                   vid=1;                     // for the fake view changes
    @Property
    boolean                print_local_addr=true;
    static final String    name="SMACK";
    
    



    public SMACK() {
    }

    public String getName() {
        return name;
    }


    public boolean setProperties(Properties props) {               
        super.setProperties(props);
       
        String str=props.getProperty("timeout");
        long[] tmp;
        if(str != null) {
            tmp=Util.parseCommaDelimitedLongs(str);
            props.remove("timeout");
            if(tmp != null && tmp.length > 0)
                timeout=tmp;
        }


        return true;
    }


    public void stop() {
        AckReceiverWindow win;
        if(sender_win != null) {
            sender_win.stop();
            sender_win=null;
        }
        for(Iterator it=receivers.values().iterator(); it.hasNext();) {
            win=(AckReceiverWindow)it.next();
            win.reset();
        }
        receivers.clear();
    }


    public Object up(Event evt) {
        Address sender;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                addMember(local_addr);
                if(print_local_addr) {
                    System.out.println("\n-------------------------------------------------------\n" +
                                       "GMS: address is " + local_addr +
                                       "\n-------------------------------------------------------");
                }
                break;

            case Event.SUSPECT:
                if(log.isInfoEnabled()) log.info("removing suspected member " + evt.getArg());
                removeMember((Address)evt.getArg());
                break;

            case Event.MSG:
                Message msg=(Message)evt.getArg(), tmp_msg;
                if(msg == null) break;
                sender=msg.getSrc();
                SmackHeader hdr=(SmackHeader)msg.getHeader(name);
                if(hdr == null) // is probably a unicast message
                    break;
                switch(hdr.type) {
                    case SmackHeader.MCAST: // send an ack, then pass up (if not already received)
                        if(log.isTraceEnabled())
                            log.trace("received #" + hdr.seqno + " from " + sender);

                        AckReceiverWindow win=receivers.get(sender);
                        if(win == null) {
                            addMember(sender);
                            win=new AckReceiverWindow(hdr.seqno);
                            receivers.put(sender, win);
                        }

                        boolean added=win.add(hdr.seqno, msg);

                        Message ack_msg=new Message(sender);
                        ack_msg.putHeader(name, new SmackHeader(SmackHeader.ACK, hdr.seqno));
                        down_prot.down(new Event(Event.MSG, ack_msg));

                        // message is passed up if OOB. Later, when remove() is called, we discard it. This affects ordering !
                        // http://jira.jboss.com/jira/browse/JGRP-379
                        if(msg.isFlagSet(Message.OOB) && added) {
                            up_prot.up(new Event(Event.MSG, msg));
                        }

                        // now remove as many messages as possible
                        while((tmp_msg=win.remove()) != null) {
                            // discard OOB msg as it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-379)
                            if(tmp_msg.isFlagSet(Message.OOB)) {
                                continue;
                            }
                            up_prot.up(new Event(Event.MSG, tmp_msg));
                        }
                        return null;

                    case SmackHeader.ACK:
                        addMember(msg.getSrc());
                        sender_win.ack(hdr.seqno, msg.getSrc());
                        sender_win.clearStableMessages();
                        if(log.isTraceEnabled())
                            log.trace("received ack for #" + hdr.seqno + " from " + msg.getSrc());
                        return null;

                    case SmackHeader.JOIN_ANNOUNCEMENT:
                        if(log.isInfoEnabled()) log.info("received join announcement by " + msg.getSrc());
                        if(!containsMember(sender)) {
                            Message join_rsp=new Message(sender);
                            join_rsp.putHeader(name, new SmackHeader(SmackHeader.JOIN_ANNOUNCEMENT, -1));
                            down_prot.down(new Event(Event.ENABLE_UNICASTS_TO, sender));
                            down_prot.down(new Event(Event.MSG, join_rsp));
                        }
                        addMember(sender);
                        return null;

                    case SmackHeader.LEAVE_ANNOUNCEMENT:
                        if(log.isInfoEnabled()) log.info("received leave announcement by " + msg.getSrc());
                        removeMember(sender);
                        return null;

                    default:
                        if(log.isWarnEnabled()) log.warn("detected SmackHeader with invalid type: " + hdr);
                        break;
                }
                break;
        }

        return up_prot.up(evt);
    }


    public Object down(Event evt) {
        Message leave_msg;

        switch(evt.getType()) {

            case Event.DISCONNECT:
                leave_msg=new Message();
                leave_msg.putHeader(name, new SmackHeader(SmackHeader.LEAVE_ANNOUNCEMENT, -1));
                down_prot.down(new Event(Event.MSG, leave_msg));
                sender_win.stop();
                break;

            case Event.CONNECT:
                Object ret=down_prot.down(evt);
                sender_win=new AckMcastSenderWindow(this, new StaticInterval(timeout));

                // send join announcement
                Message join_msg=new Message();
                join_msg.putHeader(name, new SmackHeader(SmackHeader.JOIN_ANNOUNCEMENT, -1));
                down_prot.down(new Event(Event.MSG, join_msg));
                return ret;


            // add a header with the current sequence number and increment seqno
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg == null) break;
                if(msg.getDest() == null || msg.getDest().isMulticastAddress()) {
                    msg.putHeader(name, new SmackHeader(SmackHeader.MCAST, seqno));
                    sender_win.add(seqno, msg, new Vector<Address>(members));
                    if(log.isTraceEnabled()) log.trace("sending mcast #" + seqno);
                    seqno++;
                }
                break;
        }

        return down_prot.down(evt);
    }



    /* ----------------------- Interface AckMcastSenderWindow.RetransmitCommand -------------------- */

    public void retransmit(long seqno, Message msg, Address dest) {
        msg.setDest(dest);

        if(log.isInfoEnabled()) log.info(seqno + ", msg=" + msg);
        down_prot.down(new Event(Event.MSG, msg));
    }

    /* -------------------- End of Interface AckMcastSenderWindow.RetransmitCommand ---------------- */




    public static class SmackHeader extends Header implements Streamable {
        public static final byte MCAST=1;
        public static final byte ACK=2;
        public static final byte JOIN_ANNOUNCEMENT=3;
        public static final byte LEAVE_ANNOUNCEMENT=4;

        byte type=0;
        long seqno=-1;
        private static final long serialVersionUID=7605481696520929774L;

        public SmackHeader() {
        }

        public SmackHeader(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            seqno=in.readLong();
        }

        public int size() {
            return Global.LONG_SIZE + Global.BYTE_SIZE;
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            seqno=in.readLong();
        }


        public String toString() {
            switch(type) {
                case MCAST:
                    return "MCAST";
                case ACK:
                    return "ACK";
                case JOIN_ANNOUNCEMENT:
                    return "JOIN_ANNOUNCEMENT";
                case LEAVE_ANNOUNCEMENT:
                    return "LEAVE_ANNOUNCEMENT";
                default:
                    return "<unknown>";
            }
        }
    }


    /* ------------------------------------- Private methods --------------------------------------- */
    void addMember(Address mbr) {
        Vector<Address> tmp=null;
        synchronized(members) {
            if(members.add(mbr)) {
                tmp=new Vector<Address>(members);
            }
        }
        if(tmp != null) {
            if(log.isTraceEnabled())
                log.trace("added " + mbr + ", members=" + tmp);
            View new_view=new View(new ViewId(local_addr, vid++), tmp);
            up_prot.up(new Event(Event.VIEW_CHANGE, new_view));
            down_prot.down(new Event(Event.VIEW_CHANGE, new_view));
        }
    }

    void removeMember(Address mbr) {
        Vector<Address> tmp=null;
        synchronized(members) {
            if(members.remove(mbr))
                tmp=new Vector<Address>(members);
        }
        if(tmp != null) {
            if(log.isTraceEnabled())
                log.trace("removed " + mbr + ", members=" + tmp);
            View new_view=new View(new ViewId(local_addr, vid++), tmp);
            up_prot.up(new Event(Event.VIEW_CHANGE, new_view));
            down_prot.down(new Event(Event.VIEW_CHANGE, new_view));
            if(sender_win != null)
                sender_win.remove(mbr); // causes retransmissions to mbr to stop
        }
    }


    boolean containsMember(Address mbr) {
        synchronized(members) {
            return members.contains(mbr);
        }
    }

    /* --------------------------------- End of Private methods ------------------------------------ */

}

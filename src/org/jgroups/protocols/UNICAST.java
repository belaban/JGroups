package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.AckReceiverWindow;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


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
 * @version $Id: UNICAST.java,v 1.133 2009/04/29 11:00:20 belaban Exp $
 */
@MBean(description="Reliable unicast layer")
@DeprecatedProperty(names={"immediate_ack", "use_gms", "enabled_mbrs_timeout", "eager_lock_release"})
public class UNICAST extends Protocol implements AckSenderWindow.RetransmitCommand, AgeOutCache.Handler<Address> {


    private static final String name="UNICAST";

    private static final long DEFAULT_FIRST_SEQNO=1;


    /* ------------------------------------------ Properties  ------------------------------------------ */


    @Property(description="Whether to loop back messages sent to self. Default is false")
    private boolean loopback=false;


    private long[] timeout= { 400, 800, 1600, 3200 }; // for AckSenderWindow: max time to wait for missing acks

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    private long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;
    private long num_acks_sent=0, num_acks_received=0, num_xmit_requests_received=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */



    private final Vector<Address> members=new Vector<Address>(11);

    private final HashMap<Address,Entry> connections=new HashMap<Address,Entry>(11);

    private Address local_addr=null;

    private TimeScheduler timer=null; // used for retransmissions (passed to AckSenderWindow)

    private boolean started=false;

    /** <em>Regular</em> messages which have been added, but not removed */
    private final AtomicInteger undelivered_msgs=new AtomicInteger(0);

    private long last_conn_id=0;

    @Property(description="Max number of milliseconds we try to retransmit a message to any given member. After that, " +
            "the connection is removed. Any new connection to that member will start with seqno #1 again. 0 disables this")
    protected long max_retransmit_time=60 * 1000L;

    private AgeOutCache<Address> cache=null;


    @ManagedAttribute
    public int getUndeliveredMessages() {
        return undelivered_msgs.get();
    }

    /** All protocol names have to be unique ! */
    public String  getName() {return name;}

    public long[] getTimeout() {return timeout;}

    @Property(name="timeout",converter=PropertyConverters.LongArray.class)
    public void setTimeout(long[] val) {
        if(val != null)
            timeout=val;
    }

    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute
    public String getMembers() {return members != null? members.toString() : "[]";}
    @ManagedOperation
    public String printConnections() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Entry> entry: connections.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    @ManagedAttribute
    public long getNumMessagesSent() {
        return num_msgs_sent;
    }

    @ManagedAttribute
    public long getNumMessagesReceived() {
        return num_msgs_received;
    }

    @ManagedAttribute
    public long getNumBytesSent() {
        return num_bytes_sent;
    }

    @ManagedAttribute
    public long getNumBytesReceived() {
        return num_bytes_received;
    }

    @ManagedAttribute
    public long getNumAcksSent() {
        return num_acks_sent;
    }

    @ManagedAttribute
    public long getNumAcksReceived() {
        return num_acks_received;
    }

    @ManagedAttribute
    public long getNumberOfRetransmitRequestsReceived() {
        return num_xmit_requests_received;
    }

    @ManagedAttribute(writable=true)
    public long getMaxRetransmitTime() {
        return max_retransmit_time;
    }

    public void setMaxRetransmitTime(long max_retransmit_time) {
        this.max_retransmit_time=max_retransmit_time;
        if(cache != null && max_retransmit_time > 0)
            cache.setTimeout(max_retransmit_time);
    }

    @ManagedAttribute
    public int getAgeOutCacheSize() {
        return cache != null? cache.size() : 0;
    }

    @ManagedOperation
    public String printAgeOutCache() {
        return cache != null? cache.toString() : "n/a";
    }

    public AgeOutCache getAgeOutCache() {
        return cache;
    }

    /** The number of messages in all Entry.sent_msgs tables (haven't received an ACK yet) */
    @ManagedAttribute
    public int getNumberOfUnackedMessages() {
        int num=0;
        synchronized(connections) {
            for(Entry entry: connections.values()) {
                if(entry.sent_msgs != null)
                    num+=entry.sent_msgs.size();
            }
        }
        return num;
    }

    @ManagedOperation
    public String printUnackedMessages() {
        StringBuilder sb=new StringBuilder();
        Entry e;
        Object member;
        synchronized(connections) {
            for(Map.Entry<Address,Entry> entry: connections.entrySet()) {
                member=entry.getKey();
                e=entry.getValue();
                sb.append(member).append(": ");
                if(e.sent_msgs != null)
                    sb.append(e.sent_msgs.toString()).append("\n");
            }
        }
        return sb.toString();
    }


    @ManagedAttribute
    public int getNumberOfMessagesInReceiveWindows() {
        int num=0;
        synchronized(connections) {
            for(Entry entry: connections.values()) {
                if(entry.received_msgs != null)
                    num+=entry.received_msgs.size();
            }
        }
        return num;
    }

    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=num_acks_sent=num_acks_received=0;
        num_xmit_requests_received=0;
    }


    public Map<String, Object> dumpStats() {
        Map<String, Object> m=super.dumpStats();
        m.put("unacked_msgs", printUnackedMessages());
        m.put("num_msgs_sent", num_msgs_sent);
        m.put("num_msgs_received", num_msgs_received);
        m.put("num_bytes_sent", num_bytes_sent);
        m.put("num_bytes_received", num_bytes_received);
        m.put("num_acks_sent", num_acks_sent);
        m.put("num_acks_received", num_acks_received);
        m.put("num_xmit_requests_received", num_xmit_requests_received);
        m.put("num_unacked_msgs", getNumberOfUnackedMessages());
        m.put("num_msgs_in_recv_windows", getNumberOfMessagesInReceiveWindows());
        return m;
    }



    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache(timer, max_retransmit_time, this);
        started=true;
    }

    public void stop() {
        started=false;
        removeAllConnections();
        undelivered_msgs.set(0);
    }


    public Object up(Event evt) {
        Message        msg;
        Address        dst, src;
        UnicastHeader  hdr;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                dst=msg.getDest();

                if(dst == null || dst.isMulticastAddress())  // only handle unicast messages
                    break;  // pass up

                // changed from removeHeader(): we cannot remove the header because if we do loopback=true at the
                // transport level, we will not have the header on retransmit ! (bela Aug 22 2006)
                hdr=(UnicastHeader)msg.getHeader(name);
                if(hdr == null)
                    break;
                src=msg.getSrc();
                switch(hdr.type) {
                    case UnicastHeader.DATA:      // received regular message
                        handleDataReceived(src, hdr.seqno, hdr.conn_id, hdr.first, msg);
                        return null; // we pass the deliverable message up in handleDataReceived()
                    case UnicastHeader.ACK:  // received ACK for previously sent message
                        handleAckReceived(src, hdr.seqno);
                        break;
                    case UnicastHeader.SEND_FIRST_SEQNO:
                        handleResendingOfFirstMessage(src);
                        break;
                    default:
                        log.error("UnicastHeader type " + hdr.type + " not known !");
                        break;
                }
                return null;
        }

        return up_prot.up(evt);   // Pass up to the layer above us
    }



    public Object down(Event evt) {
        switch (evt.getType()) {

            case Event.MSG: // Add UnicastHeader, add to AckSenderWindow and pass down
                Message msg=(Message)evt.getArg();
                Address dst=msg.getDest();

                /* only handle unicast messages */
                if (dst == null || dst.isMulticastAddress()) {
                    break;
                }

                if(!started) {
                    if(log.isTraceEnabled())
                        log.trace("discarded message as start() has not yet been called, message: " + msg);
                    return null;
                }

                // if the dest is self --> pass the message back up
                if(loopback && local_addr != null && local_addr.equals(dst)) {
                    msg.setSrc(local_addr);
                    up_prot.up(evt);
                    num_msgs_sent++;
                    num_bytes_sent+=msg.getLength();
                    return null;
                }

                Entry entry;
                synchronized(connections) {
                    entry=connections.get(dst);
                    if(entry == null) {
                        entry=new Entry();
                        connections.put(dst, entry);
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": created connection to " + dst);
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }

                long seqno=-2;
                synchronized(entry) { // threads will only sync if they access the same entry
                    try {
                        seqno=entry.sent_msgs_seqno;
                        if(seqno == DEFAULT_FIRST_SEQNO) // only happens on the first message
                            entry.send_conn_id=getNewConnectionId();
                        if(entry.sent_msgs == null) // first msg to peer 'dst'
                            entry.sent_msgs=new AckSenderWindow(this, new StaticInterval(timeout), timer, this.local_addr);
                        UnicastHeader hdr=new UnicastHeader(UnicastHeader.DATA, seqno, entry.send_conn_id,
                                                            seqno == DEFAULT_FIRST_SEQNO);
                        msg.putHeader(name, hdr);
                        if(log.isTraceEnabled()) {
                            StringBuilder sb=new StringBuilder();
                            sb.append(local_addr).append(" --> DATA(").append(dst).append(": #").append(seqno).
                                    append(", conn_id=").append(entry.send_conn_id).append(')');
                            log.trace(sb);
                        }
                        entry.sent_msgs.add(seqno, msg);  // add *including* UnicastHeader, adds to retransmitter
                        entry.sent_msgs_seqno++;
                    }
                    catch(Throwable t) {
                        entry.sent_msgs.ack(seqno); // remove seqno again, so it is not transmitted
                        throw new RuntimeException("failure adding msg " + msg + " to the retransmit table", t);
                    }
                }
                // moved passing down of message out of the synchronized block: similar to NAKACK, we do *not* need
                // to send unicast messages in order of sequence numbers because they will be sorted into the correct
                // order at the receiver anyway. Of course, most of the time, the order will be correct (FIFO), so
                // the cost of reordering is minimal. This is part of http://jira.jboss.com/jira/browse/JGRP-303
                try { // we catch the exception in this case because the msg is in the XMIT table and will be retransmitted
                    send(msg, evt);
                }
                catch(Throwable t) {
                    log.warn("failed sending the message", t);
                }
                return null; // we already passed the msg down

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                View view=(View)evt.getArg();
                Vector<Address> new_members=view.getMembers();
                Set<Address> non_members=new HashSet<Address>(connections.keySet());
                synchronized(members) {
                    members.clear();
                    if(new_members != null)
                        members.addAll(new_members);
                    non_members.removeAll(members);
                    if(cache != null) {
                        cache.removeAll(members);
                    }
                }

                if(!non_members.isEmpty()) {
                    if(log.isTraceEnabled())
                        log.trace("removing non members " + non_members);
                    for(Address non_mbr: non_members)
                        removeConnection(non_mbr);
                }
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }


    private void send(Message msg, Event evt) {
        down_prot.down(evt);
        num_msgs_sent++;
        num_bytes_sent+=msg.getLength();
    }

    /**
     * Removes and resets from connection table (which is already locked). Returns true if member was found,
     * otherwise false. This method is public only so it can be invoked by unit testing, but should not otherwise be
     * used ! 
     */
    public boolean removeConnection(Address mbr) {
        Entry entry;

        synchronized(connections) {
            entry=connections.remove(mbr);
        }
        if(entry != null) {
            entry.reset();
            return true;
        }
        else
            return false;
    }


    private void removeAllConnections() {
        synchronized(connections) {
            for(Entry entry: connections.values()) {
                entry.reset();
            }
            connections.clear();
        }
    }



    /** Called by AckSenderWindow to resend messages for which no ACK has been received yet */
    public void retransmit(long seqno, Message msg) {
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> XMIT(" + msg.getDest() + ": #" + seqno + ')');
        down_prot.down(new Event(Event.MSG, msg));
        num_xmit_requests_received++;
    }

    /**
     * Called by AgeOutCache, to removed expired connections
     * @param key
     */
    public void expired(Address key) {
        if(key != null) {
            if(log.isDebugEnabled())
                log.debug("removing connection to " + key + " because it expired");
            removeConnection(key);
        }
    }




    /**
     * Check whether the hashtable contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    private void handleDataReceived(Address sender, long seqno, long conn_id,  boolean first, Message msg) {
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            sb.append(')');
            log.trace(sb);
        }

        AckReceiverWindow win;
        synchronized(connections) {
            Entry entry=connections.get(sender);
            win=entry != null? entry.received_msgs : null;

            if(first) {
                if(entry == null || win == null) {
                    win=createReceiverWindow(sender, entry, seqno, conn_id);
                }
                else {  // entry != null && win != null
                    if(conn_id != entry.recv_conn_id) {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": conn_id=" + conn_id + " != " + entry.recv_conn_id + "; resetting receiver window");
                        win=createReceiverWindow(sender, entry, seqno, conn_id);
                    }
                    else {
                        ;
                    }
                }
            }
            else { // entry == null && win == null OR entry != null && win == null OR entry != null && win != null
                if(win == null || entry.recv_conn_id != conn_id) {
                    sendRequestForFirstSeqno(sender);
                    return; // drop message
                }
            }
        }

        boolean added=win.add(seqno, msg); // entry.received_msgs is guaranteed to be non-null if we get here
        num_msgs_received++;
        num_bytes_received+=msg.getLength();

        if(added && !msg.isFlagSet(Message.OOB))
            undelivered_msgs.incrementAndGet();

        if(win.smallerThanNextToRemove(seqno))
            sendAck(msg.getSrc(), seqno);

        // message is passed up if OOB. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(msg.isFlagSet(Message.OOB)) {
            if(added)
                up_prot.up(new Event(Event.MSG, msg));
            Message oob_msg=win.removeOOBMessage();
            if(oob_msg != null)
                sendAckForMessage(oob_msg);
            
            if(!(win.hasMessagesToRemove() && undelivered_msgs.get() > 0))
                return;
        }

        if(!added && !win.hasMessagesToRemove()) { // no ack if we didn't add the msg (e.g. duplicate)
            return;
        }

        final AtomicBoolean processing=win.getProcessing();
        if(!processing.compareAndSet(false, true)) {
            return;
        }

        // Try to remove (from the AckReceiverWindow) as many messages as possible as pass them up
        boolean released_processing=false;
        int num_regular_msgs_removed=0;

        // Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
        // this is all the more important once we have a threadless stack (http://jira.jboss.com/jira/browse/JGRP-181),
        // where lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time
        // We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
        // delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered only in the
        // order in which they were sent by their senders
        try {
            while(true) {
                Message m=win.remove(processing);
                if(m == null) {
                    released_processing=true;
                    return;
                }

                // discard OOB msg as it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-377)
                if(m.isFlagSet(Message.OOB)) {
                    continue;
                }
                num_regular_msgs_removed++;
                sendAckForMessage(m);
                up_prot.up(new Event(Event.MSG, m));
            }
        }
        finally {
            // We keep track of regular messages that we added, but couldn't remove (because of ordering).
            // When we have such messages pending, then even OOB threads will remove and process them
            // http://jira.jboss.com/jira/browse/JGRP-781
            undelivered_msgs.addAndGet(-num_regular_msgs_removed);

            if(!released_processing)
                processing.set(false);
        }
    }


    private AckReceiverWindow createReceiverWindow(Address sender, Entry entry, long seqno, long conn_id) {
        if(entry == null) {
            entry=new Entry();
            connections.put(sender, entry);
        }
        if(entry.received_msgs != null)
            entry.received_msgs.reset();
        entry.received_msgs=new AckReceiverWindow(seqno);
        entry.recv_conn_id=conn_id;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": created receiver window for " + sender + " at seqno=#" + seqno + " for conn-id=" + conn_id);
        return entry.received_msgs;
    }



    /** Add the ACK to hashtable.sender.sent_msgs */
    private void handleAckReceived(Address sender, long seqno) {
        Entry           entry;
        AckSenderWindow win;

        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- ACK(").append(sender).
                    append(": #").append(seqno).append(')'));
        synchronized(connections) {
            entry=connections.get(sender);
        }
        if(entry == null || entry.sent_msgs == null)
            return;
        win=entry.sent_msgs;
        win.ack(seqno); // removes message from retransmission
        num_acks_received++;
    }


    /**
     * We need to resend our first message with our conn_id
     * @param sender
     */
    private void handleResendingOfFirstMessage(Address sender) {
        Entry entry;
        AckSenderWindow sender_win;
        Message rsp;
        if(log.isTraceEnabled())
            log.trace(local_addr + " <-- SEND_FIRST_SEQNO(" + sender + ")");

        synchronized(connections) {
            entry=connections.get(sender);
            sender_win=entry != null? entry.sent_msgs : null;
            if(sender_win == null) {
                if(log.isErrorEnabled())
                    log.error("sender window for " + sender + " not found");
                return;
            }
            rsp=sender_win.getLowestMessage();
        }
        if(rsp == null) {
            if(log.isWarnEnabled())
                log.warn("didn't find any messages in my sender window for " + sender);
            return;
        }
        // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
        // the headers and therefore we'd modify the original message in the sender retransmission window
        // (https://jira.jboss.org/jira/browse/JGRP-965)
        Message copy=rsp.copy();
        UnicastHeader hdr=(UnicastHeader)copy.getHeader(name);
        UnicastHeader newhdr=new UnicastHeader(hdr.type, hdr.seqno, entry.send_conn_id, true);
        copy.putHeader(name, newhdr);
        down_prot.down(new Event(Event.MSG, copy));
    }


    private void sendAck(Address dst, long seqno) {
        Message ack=new Message(dst);
        // commented Jan 23 2008 (bela): TP.enable_unicast_bundling should decide whether we bundle or not, and *not*
        // the OOB flag ! Bundling UNICAST ACKs should be really fast
        ack.setFlag(Message.OOB);
        ack.putHeader(name, new UnicastHeader(UnicastHeader.ACK, seqno));
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" --> ACK(").append(dst).
                      append(": #").append(seqno).append(')'));
        down_prot.down(new Event(Event.MSG, ack));
        num_acks_sent++;
    }


    private void sendAckForMessage(Message msg) {
        UnicastHeader hdr=msg != null? (UnicastHeader)msg.getHeader(name) : null;
        Address sender=msg != null? msg.getSrc() : null;
        if(hdr != null && sender != null) {
            sendAck(sender, hdr.seqno);
        }
    }

    private long getNewConnectionId() {
        long retval=System.currentTimeMillis();
        synchronized(this) {
            if(last_conn_id == retval)
                retval++;
            last_conn_id=retval;
            return retval;
        }
    }

    private void sendRequestForFirstSeqno(Address dest) {
        Message msg=new Message(dest);
        msg.setFlag(Message.OOB);
        UnicastHeader hdr=new UnicastHeader(UnicastHeader.SEND_FIRST_SEQNO, 0);
        msg.putHeader(name, hdr);
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> SEND_FIRST_SEQNO(" + dest + ")");
        down_prot.down(new Event(Event.MSG, msg));
    }





    public static class UnicastHeader extends Header implements Streamable {
        public static final byte DATA             = 0;
        public static final byte ACK              = 1;
        public static final byte SEND_FIRST_SEQNO = 2;

        byte    type=DATA;
        long    seqno=0;
        long    conn_id=0;
        boolean first=false;

        static final int serialized_size=Global.BYTE_SIZE * 2 + Global.LONG_SIZE * 2;
        private static final long serialVersionUID=-8983745221189309298L;


        public UnicastHeader() {} // used for externalization

        public UnicastHeader(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        public UnicastHeader(byte type, long seqno, long conn_id) {
            this(type, seqno);
            this.conn_id=conn_id;
        }

        public UnicastHeader(byte type, long seqno, long conn_id, boolean first) {
            this(type, seqno, conn_id);
            this.first=first;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append("[UNICAST: ").append(type2Str(type)).append(", seqno=").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            sb.append(']');
            return sb.toString();
        }

        public static String type2Str(byte t) {
            switch(t) {
                case DATA: return "DATA";
                case ACK: return "ACK";
                case SEND_FIRST_SEQNO: return "SEND_FIRST_SEQNO";
                default: return "<unknown>";
            }
        }

        public final int size() {
            return serialized_size;
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
            out.writeLong(conn_id);
            out.writeBoolean(first);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            seqno=in.readLong();
            conn_id=in.readLong();
            first=in.readBoolean();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            out.writeLong(seqno);
            out.writeLong(conn_id);
            out.writeBoolean(first);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            seqno=in.readLong();
            conn_id=in.readLong();
            first=in.readBoolean();
        }
    }

    private static final class Entry {
        AckReceiverWindow  received_msgs=null;  // stores all msgs rcvd by a certain peer in seqno-order
        long               recv_conn_id=0;

        AckSenderWindow    sent_msgs=null;      // stores (and retransmits) msgs sent by us to a certain peer
        long               sent_msgs_seqno=DEFAULT_FIRST_SEQNO;   // seqno for msgs sent by us
        long               send_conn_id=0;

        void reset() {
            if(sent_msgs != null)
                sent_msgs.reset();
            if(received_msgs != null)
                received_msgs.reset();
            sent_msgs_seqno=DEFAULT_FIRST_SEQNO;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append("sent_msgs=").append(sent_msgs).append('\n');
            if(received_msgs != null)
                sb.append("received_msgs=").append(received_msgs).append('\n');
            sb.append("send_conn_id=" + send_conn_id + ", recv_conn_id=" + recv_conn_id + "\n");
            return sb.toString();
        }
    }



}
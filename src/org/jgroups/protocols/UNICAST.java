package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.AckReceiverWindow;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
 * @version $Id: UNICAST.java,v 1.166 2010/03/12 09:07:44 belaban Exp $
 */
@MBean(description="Reliable unicast layer")
@DeprecatedProperty(names={"immediate_ack", "use_gms", "enabled_mbrs_timeout", "eager_lock_release"})
public class UNICAST extends Protocol implements AckSenderWindow.RetransmitCommand, AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */


    @Property(description="Whether to loop back messages sent to self. Default is false",
              deprecatedMessage="might get removed soon as it can destroy ordering. " +
                      "See https://jira.jboss.org/jira/browse/JGRP-1092 for details")
    @Deprecated
    private boolean loopback=false;


    private long[] timeout= { 400, 800, 1600, 3200 }; // for AckSenderWindow: max time to wait for missing acks

    @Property(description="Max number of messages to be removed from the AckReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    private int max_msg_batch_size=50000;

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    private long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0;
    private long num_acks_sent=0, num_acks_received=0, num_xmits=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */


    private final ConcurrentMap<Address,SenderEntry> send_table=new ConcurrentHashMap<Address,SenderEntry>();
    private final ConcurrentMap<Address,ReceiverEntry> recv_table=new ConcurrentHashMap<Address,ReceiverEntry>();

    private final Vector<Address> members=new Vector<Address>(11);

    private Address local_addr=null;

    private TimeScheduler timer=null; // used for retransmissions (passed to AckSenderWindow)

    private boolean started=false;

    // didn't make this 'connected' in case we need to send early acks which may race to the socket
    private volatile boolean disconnected=false;

    private short last_conn_id=0;

    protected long max_retransmit_time=60 * 1000L;

    private AgeOutCache<Address> cache=null;


    @Deprecated @ManagedAttribute
    public static int getUndeliveredMessages() {
        return 0;
    }

    public long[] getTimeout() {return timeout;}

    @Property(name="timeout",converter=PropertyConverters.LongArray.class)
    public void setTimeout(long[] val) {
        if(val != null)
            timeout=val;
    }

    public void setMaxMessageBatchSize(int size) {
        if(size >= 1)
            max_msg_batch_size=size;
    }

    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute
    public String getMembers() {return members != null? members.toString() : "[]";}


    @ManagedOperation
    public String printConnections() {
        StringBuilder sb=new StringBuilder();
        if(!send_table.isEmpty()) {
            sb.append("\nsend connections:\n");
            for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        if(!recv_table.isEmpty()) {
            sb.append("\nreceive connections:\n");
            for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
                sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
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
    public long getNumberOfRetransmissions() {
        return num_xmits;
    }

    public long getMaxRetransmitTime() {
        return max_retransmit_time;
    }

    @Property(description="Max number of milliseconds we try to retransmit a message to any given member. After that, " +
            "the connection is removed. Any new connection to that member will start with seqno #1 again. 0 disables this")
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

    public AgeOutCache<Address> getAgeOutCache() {
        return cache;
    }

    /** The number of messages in all Entry.sent_msgs tables (haven't received an ACK yet) */
    @ManagedAttribute
    public int getNumberOfUnackedMessages() {
        int num=0;
        for(SenderEntry entry: send_table.values()) {
                if(entry.sent_msgs != null)
                    num+=entry.sent_msgs.size();
            }
        return num;
    }

    @ManagedOperation
    public String printUnackedMessages() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            sb.append(entry.getKey()).append(": ");
            SenderEntry val=entry.getValue();
            if(val.sent_msgs != null)
                sb.append(val.sent_msgs).append("\n");
        }
        return sb.toString();
    }


    @ManagedAttribute
    public int getNumberOfMessagesInReceiveWindows() {
        int num=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                num+=entry.received_msgs.size();
        }
        return num;
    }

    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=num_acks_sent=num_acks_received=0;
        num_xmits=0;
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
        m.put("num_xmits", num_xmits);
        m.put("num_unacked_msgs", getNumberOfUnackedMessages());
        m.put("num_msgs_in_recv_windows", getNumberOfMessagesInReceiveWindows());
        return m;
    }



    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<Address>(timer, max_retransmit_time, this);
        started=true;
    }

    public void stop() {
        started=false;
        removeAllConnections();
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
                hdr=(UnicastHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                src=msg.getSrc();
                switch(hdr.type) {
                    case UnicastHeader.DATA:      // received regular message
                        handleDataReceived(src, hdr.seqno, hdr.conn_id, hdr.first, msg, evt);
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

                SenderEntry entry=send_table.get(dst);
                if(entry == null) {
                    entry=new SenderEntry(getNewConnectionId(), this, timeout, timer, local_addr);
                    SenderEntry existing=send_table.putIfAbsent(dst, entry);
                    if(existing != null)
                        entry=existing;
                    else {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": created connection to " + dst);
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }

                long seqno=-2;
                short send_conn_id=-1;
                UnicastHeader hdr;

                entry.lock(); // threads will only sync if they access the same entry
                try {
                    seqno=entry.sent_msgs_seqno;
                    send_conn_id=entry.send_conn_id;
                    hdr=UnicastHeader.createDataHeader(seqno, send_conn_id, seqno == DEFAULT_FIRST_SEQNO);
                    msg.putHeader(this.id, hdr);

                    // AckSenderWindow.add() is costly as it calls Retransmitter.add() which calls TimeScheduler.schedule(),
                    // which adds the scheduled task to a DelayQueue, which does costly tree rebalancing.
                    // In 2.9 (or 3.0), we'll replace use of ScheduledThreadPoolExecutor in TimeScheduler with
                    // a ConcurrentSkipListMap, which is faster (log(n) access cost for most ops). CSLM requires JDK 1.6
                    // Note that moving the next statement out of the lock scope made for some really ugly code, that's
                    // why this was reverted !

                    // entry.sent_msgs.add(seqno, msg);  // add *including* UnicastHeader, adds to retransmitter
                    entry.sent_msgs.addToMessages(seqno, msg);  // add *including* UnicastHeader, adds to retransmitter
                    entry.sent_msgs_seqno++;
                }
                finally {
                    entry.unlock();
                }

                long sleep_time=100;
                for(int i=1; i <= 10; i++) {
                    try {
                        entry.sent_msgs.addToRetransmitter(seqno, msg);  // adds to retransmitter
                        break;
                    }
                    catch(Throwable t) {
                        log.error("failed adding message " + seqno + " to " + dst +
                                " to the retransmitter, retrying... (attempt #" + i + ")");
                        Util.sleep(sleep_time);
                        sleep_time*=2;
                    }
                }

                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder();
                    sb.append(local_addr).append(" --> DATA(").append(dst).append(": #").append(seqno).
                            append(", conn_id=").append(send_conn_id);
                    if(hdr.first) sb.append(", first");
                    sb.append(')');
                    log.trace(sb);
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
                Set<Address> non_members=new HashSet<Address>(send_table.keySet());
                non_members.addAll(recv_table.keySet());

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

            case Event.CONNECT:
                disconnected=false;
                break;

            case Event.DISCONNECT:
                disconnected=true;
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
    public void removeConnection(Address mbr) {
        SenderEntry entry=send_table.remove(mbr);
        if(entry != null)
            entry.reset();

        ReceiverEntry entry2=recv_table.remove(mbr);
        if(entry2 != null)
            entry2.reset();
    }

    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        for(SenderEntry entry: send_table.values())
            entry.reset();
        send_table.clear();

        for(ReceiverEntry entry2: recv_table.values())
            entry2.reset();
        recv_table.clear();
    }



    /** Called by AckSenderWindow to resend messages for which no ACK has been received yet */
    public void retransmit(long seqno, Message msg) {
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> XMIT(" + msg.getDest() + ": #" + seqno + ')');
        down_prot.down(new Event(Event.MSG, msg));
        num_xmits++;
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
    private void handleDataReceived(Address sender, long seqno, long conn_id,  boolean first, Message msg, Event evt) {
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }

        ReceiverEntry entry=recv_table.get(sender);
        AckReceiverWindow win=entry != null? entry.received_msgs : null;

        if(first) {
            if(entry == null) {
                entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                win=entry.received_msgs;
            }
            else {  // entry != null && win != null
                if(conn_id != entry.recv_conn_id) {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": conn_id=" + conn_id + " != " + entry.recv_conn_id + "; resetting receiver window");

                    ReceiverEntry entry2=recv_table.remove(sender);
                    if(entry2 != null)
                        entry2.received_msgs.reset();
                    
                    entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                    win=entry.received_msgs;
                }
                else {
                    ;
                }
            }
        }
        else { // entry == null && win == null OR entry != null && win == null OR entry != null && win != null
            if(win == null || entry.recv_conn_id != conn_id) {
                sendRequestForFirstSeqno(sender); // drops the message and returns (see below)
                return;
            }
        }

        byte result=win.add2(seqno, msg); // win is guaranteed to be non-null if we get here
        boolean added=result > 0;
        num_msgs_received++;
        num_bytes_received+=msg.getLength();

        // Cannot be replaced with if(!added), see https://jira.jboss.org/jira/browse/JGRP-1043 comment 15/Sep/09 06:57 AM

        // We *have* to do send the ACK, to cover the following scenario:
        // - A sends #3 to B
        // - B removes #3 and sends ACK(3) to A. B's next_to_remove is now 4
        // - B's ACK(3) to A is dropped by the network
        // - A keeps retransmitting #3 to B, until it gets an ACK(3)
        // -B will never ACK #3 if the 2 lines below are commented ==> endless retransmission of A's #3 !
        if(result == -1) { // only ack if seqno was < next_to_remove !
            sendAck(sender, seqno);
        }

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(msg.isFlagSet(Message.OOB) && added) {
            try {
                up_prot.up(evt);
            }
            catch(Throwable t) {
                log.error("couldn't deliver OOB message " + msg, t);
            }
        }

        final AtomicBoolean processing=win.getProcessing();
        if(!processing.compareAndSet(false, true)) {
            return;
        }

        // try to remove (from the AckReceiverWindow) as many messages as possible as pass them up

        // Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
        // this is all the more important once we have a concurrent stack (http://jira.jboss.com/jira/browse/JGRP-181),
        // where lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time
        // We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
        // delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered only in the
        // order in which they were sent by their senders
        try {
            while(true) {
                Tuple<List<Message>,Long> tuple=win.removeMany(max_msg_batch_size);
                if(tuple == null)
                    return;
                List<Message> msgs=tuple.getVal1();
                if(msgs.isEmpty())
                    return;

                long highest_removed=tuple.getVal2();
                if(highest_removed > 0)
                    sendAck(sender, highest_removed); // guaranteed not to throw an exception !

                for(Message m: msgs) {
                    // discard OOB msg: it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-377)
                    if(m.isFlagSet(Message.OOB))
                        continue;
                    try {
                        up_prot.up(new Event(Event.MSG, m));
                    }
                    catch(Throwable t) {
                        log.error("couldn't deliver message " + m, t);
                    }
                }
            }
        }
        finally {
            processing.set(false);
        }
    }


    private ReceiverEntry getOrCreateReceiverEntry(Address sender, long seqno, long conn_id) {
        ReceiverEntry entry=new ReceiverEntry(new AckReceiverWindow(seqno), conn_id);
        ReceiverEntry entry2=recv_table.putIfAbsent(sender, entry);
        if(entry2 != null)
            return entry2;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": created receiver window for " + sender + " at seqno=#" + seqno + " for conn-id=" + conn_id);
        return entry;
    }


    /** Add the ACK to hashtable.sender.sent_msgs */
    private void handleAckReceived(Address sender, long seqno) {
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- ACK(").append(sender).
                    append(": #").append(seqno).append(')'));
        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            win.ack(seqno); // removes message from retransmission
            num_acks_received++;
        }
    }


    /**
     * We need to resend our first message with our conn_id
     * @param sender
     */
    private void handleResendingOfFirstMessage(Address sender) {
        if(log.isTraceEnabled())
            log.trace(local_addr + " <-- SEND_FIRST_SEQNO(" + sender + ")");
        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender window for " + sender + " not found");
            return;
        }
        Message rsp=win.getLowestMessage();
        if(rsp == null)
            return;

        // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
        // the headers and therefore we'd modify the original message in the sender retransmission window
        // (https://jira.jboss.org/jira/browse/JGRP-965)
        Message copy=rsp.copy();
        UnicastHeader hdr=(UnicastHeader)copy.getHeader(this.id);
        UnicastHeader newhdr=hdr.copy();
        newhdr.first=true;
        copy.putHeader(this.id, newhdr);

        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> DATA(").append(copy.getDest()).append(": #").append(newhdr.seqno).
                    append(", conn_id=").append(newhdr.conn_id);
            if(newhdr.first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }
        
        down_prot.down(new Event(Event.MSG, copy));
    }


    private void sendAck(Address dst, long seqno) {
        if(disconnected) // if we are disconnected, then don't send any acks which throw exceptions on shutdown
            return;
        Message ack=new Message(dst);
        // commented Jan 23 2008 (bela): TP.enable_unicast_bundling should decide whether we bundle or not, and *not*
        // the OOB flag ! Bundling UNICAST ACKs should be really fast

        // https://jira.jboss.org/jira/browse/JGRP-1125; will be reverted in 2.10
        // ack.setFlag(Message.OOB);

        ack.putHeader(this.id, UnicastHeader.createAckHeader(seqno));
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" --> ACK(").append(dst).
                    append(": #").append(seqno).append(')'));
        try {
            down_prot.down(new Event(Event.MSG, ack));
            num_acks_sent++;
        }
        catch(Throwable t) {
            log.error("failed sending ACK(" + seqno + ") to " + dst, t);
        }
    }


   
    private short getNewConnectionId() {
        synchronized(this) {
            short retval=last_conn_id;
            if(last_conn_id >= Short.MAX_VALUE || last_conn_id < 0)
                last_conn_id=0;
            else
                last_conn_id++;
            return retval;
        }
    }

    private void sendRequestForFirstSeqno(Address dest) {
        Message msg=new Message(dest);
        msg.setFlag(Message.OOB);
        UnicastHeader hdr=UnicastHeader.createSendFirstSeqnoHeader();
        msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> SEND_FIRST_SEQNO(" + dest + ")");
        down_prot.down(new Event(Event.MSG, msg));
    }


    /**
     * The following types and fields are serialized:
     * <pre>
     * | DATA | seqno | conn_id | first |
     * | ACK  | seqno |
     * | SEND_FIRST_SEQNO |
     * </pre>
     */
    public static class UnicastHeader extends Header {
        public static final byte DATA             = 0;
        public static final byte ACK              = 1;
        public static final byte SEND_FIRST_SEQNO = 2;

        byte    type;
        long    seqno;    // DATA and ACK
        short   conn_id;  // DATA
        boolean first;    // DATA


        public UnicastHeader() {} // used for externalization

        public static UnicastHeader createDataHeader(long seqno, short conn_id, boolean first) {
            return new UnicastHeader(DATA, seqno, conn_id, first);
        }

        public static UnicastHeader createAckHeader(long seqno) {
            return new UnicastHeader(ACK, seqno);
        }

        public static UnicastHeader createSendFirstSeqnoHeader() {
            return new UnicastHeader(SEND_FIRST_SEQNO, 0L);
        }

        private UnicastHeader(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        private UnicastHeader(byte type, long seqno, short conn_id, boolean first) {
            this.type=type;
            this.seqno=seqno;
            this.conn_id=conn_id;
            this.first=first;
        }

        public long getSeqno() {
            return seqno;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(type2Str(type)).append(", seqno=").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
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
            switch(type) {
                case DATA:
                    return Global.BYTE_SIZE *2 + Global.LONG_SIZE + Global.SHORT_SIZE;
                case ACK:
                    return Global.BYTE_SIZE + Global.LONG_SIZE;
                case SEND_FIRST_SEQNO:
                    return Global.BYTE_SIZE;
            }
            return 0;
        }

        public UnicastHeader copy() {
            return new UnicastHeader(type, seqno, conn_id, first);
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    out.writeLong(seqno);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case ACK:
                    out.writeLong(seqno);
                    break;
                case SEND_FIRST_SEQNO:
                    break;
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            switch(type) {
                case DATA:
                    seqno=in.readLong();
                    conn_id=in.readShort();
                    first=in.readBoolean();
                    break;
                case ACK:
                    seqno=in.readLong();
                    break;
                case SEND_FIRST_SEQNO:
                    break;
            }
        }
    }



    private static final class SenderEntry {
        // stores (and retransmits) msgs sent by us to a certain peer
        final AckSenderWindow   sent_msgs;
        long                    sent_msgs_seqno=DEFAULT_FIRST_SEQNO;   // seqno for msgs sent by us
        final short             send_conn_id;
        final Lock              lock=new ReentrantLock();

        public SenderEntry(short send_conn_id, AckSenderWindow.RetransmitCommand cmd, long[] timeout,
                           TimeScheduler timer, Address local_addr) {
            this.send_conn_id=send_conn_id;
            sent_msgs=new AckSenderWindow(cmd, new StaticInterval(timeout), timer, local_addr);
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }

        void reset() {
            if(sent_msgs != null)
                sent_msgs.reset();
            sent_msgs_seqno=DEFAULT_FIRST_SEQNO;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append(sent_msgs).append(", ");
            sb.append("send_conn_id=" + send_conn_id);
            return sb.toString();
        }
    }

    private static final class ReceiverEntry {
        private final AckReceiverWindow  received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        private final long               recv_conn_id;

        
        public ReceiverEntry(AckReceiverWindow received_msgs, long recv_conn_id) {
            this.received_msgs=received_msgs;
            this.recv_conn_id=recv_conn_id;
        }

        void reset() {
            if(received_msgs != null)
                received_msgs.reset();
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(received_msgs != null)
                sb.append(received_msgs).append(", ");
            sb.append("recv_conn_id=" + recv_conn_id);
            return sb.toString();
        }
    }



}
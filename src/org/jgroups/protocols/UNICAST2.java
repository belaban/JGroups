package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.*;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.TimeScheduler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Reliable unicast layer. Implemented with negative acks. Every sender keeps its messages in an AckSenderWindow. A
 * receiver stores incoming messages in a NakReceiverWindow, and asks the sender for retransmission if a gap is
 * detected. Every now and then (stable_interval), a timer task sends a STABLE message to all senders, including the
 * highest received and delivered seqnos. A sender purges messages lower than highest delivered and asks the STABLE
 * sender for messages it might have missed (smaller than highest received). A STABLE message can also be sent when
 * a receiver has received more than max_bytes from a given sender.<p/>
 * The advantage of this protocol over {@link org.jgroups.protocols.UNICAST} is that it doesn't send acks for every
 * message. Instead, it sends 'acks' after receiving max_bytes and/ or periodically (stable_interval).
 * @author Bela Ban
 * @version $Id: UNICAST2.java,v 1.9 2010/06/14 08:11:27 belaban Exp $
 */
@Experimental @Unsupported
@MBean(description="Reliable unicast layer")
public class UNICAST2 extends Protocol implements Retransmitter.RetransmitCommand, AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */
    
    private long[] timeout= { 400, 800, 1600, 3200 }; // for NakSenderWindow: max time to wait for missing acks

    @Property(description="Max number of messages to be removed from a NakReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    private int max_msg_batch_size=50000;

    @Property(description="Max number of bytes before a stability message is sent to the sender")
    protected long max_bytes=10000000;

    @Property(description="Max number of milliseconds before a stability message is sent to the sender(s)")
    protected long stable_interval=10000L;

    @Property(description="Max number of STABLE messages sent for the same highest_received seqno. A value < 1 is invalid")
    protected int max_stable_msgs=5;

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    private long num_msgs_sent=0, num_msgs_received=0, num_bytes_sent=0, num_bytes_received=0, num_xmits=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    private final ConcurrentMap<Address,SenderEntry> send_table=new ConcurrentHashMap<Address,SenderEntry>();
    private final ConcurrentMap<Address,ReceiverEntry> recv_table=new ConcurrentHashMap<Address,ReceiverEntry>();

    private final Vector<Address> members=new Vector<Address>(11);

    private Address local_addr=null;

    private TimeScheduler timer=null; // used for retransmissions (passed to AckSenderWindow)

    private boolean started=false;

    private short last_conn_id=0;

    protected long max_retransmit_time=60 * 1000L;

    private AgeOutCache<Address> cache=null;

    private Future<?> stable_task_future=null; // bcasts periodic STABLE message (added to timer below)


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
            sb.append("send connections:\n");
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
        num_msgs_sent=num_msgs_received=num_bytes_sent=num_bytes_received=num_xmits=0;
    }


    public Map<String, Object> dumpStats() {
        Map<String, Object> m=super.dumpStats();
        m.put("num_msgs_sent", num_msgs_sent);
        m.put("num_msgs_received", num_msgs_received);
        m.put("num_bytes_sent", num_bytes_sent);
        m.put("num_bytes_received", num_bytes_received);
        m.put("num_xmits", num_xmits);
        m.put("num_msgs_in_recv_windows", getNumberOfMessagesInReceiveWindows());
        return m;
    }


    public TimeScheduler getTimer() {
        return timer;
    }

    /**
     * Only used for unit tests, don't use !
     * @param timer
     */
    public void setTimer(TimeScheduler timer) {
        this.timer=timer;
    }

    public void init() throws Exception {
        super.init();
        if(max_stable_msgs < 1)
            throw new IllegalArgumentException("max_stable_msgs ( " + max_stable_msgs + ") must be > 0");
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<Address>(timer, max_retransmit_time, this);
        started=true;
        if(stable_interval > 0) {
            startStableTask();
        }
    }

    public void stop() {
        started=false;
        stopStableTask();
        removeAllConnections();
    }


    public Object up(Event evt) {
        Message        msg;
        Address        dst, src;
        Unicast2Header hdr;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                dst=msg.getDest();

                if(dst == null || dst.isMulticastAddress())  // only handle unicast messages
                    break;  // pass up

                // changed from removeHeader(): we cannot remove the header because if we do loopback=true at the
                // transport level, we will not have the header on retransmit ! (bela Aug 22 2006)
                hdr=(Unicast2Header)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                src=msg.getSrc();
                switch(hdr.type) {
                    case Unicast2Header.DATA:      // received regular message
                        handleDataReceived(src, hdr.seqno, hdr.conn_id, hdr.first, msg, evt);
                        return null; // we pass the deliverable message up in handleDataReceived()
                    case Unicast2Header.XMIT_REQ:  // received ACK for previously sent message
                        handleXmitRequest(src, hdr.seqno, hdr.high_seqno);
                        break;
                    case Unicast2Header.SEND_FIRST_SEQNO:
                        handleResendingOfFirstMessage(src);
                        break;
                    case Unicast2Header.STABLE:
                        stable(msg.getSrc(), hdr.seqno, hdr.high_seqno);
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
                    entry=new SenderEntry(getNewConnectionId());
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
                Unicast2Header hdr;

                entry.lock(); // threads will only sync if they access the same entry
                try {
                    seqno=entry.sent_msgs_seqno;
                    send_conn_id=entry.send_conn_id;
                    hdr=Unicast2Header.createDataHeader(seqno, send_conn_id, seqno == DEFAULT_FIRST_SEQNO);
                    msg.putHeader(this.id, hdr);
                    entry.sent_msgs.addToMessages(seqno, msg);  // add *including* UnicastHeader, adds to retransmitter
                    entry.sent_msgs_seqno++;
                }
                finally {
                    entry.unlock();
                }

                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder();
                    sb.append(local_addr).append(" --> DATA(").append(dst).append(": #").append(seqno).
                            append(", conn_id=").append(send_conn_id);
                    if(hdr.first) sb.append(", first");
                    sb.append(')');
                    log.trace(sb);
                }

                try {
                    down_prot.down(evt);
                    num_msgs_sent++;
                    num_bytes_sent+=msg.getLength();
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
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }

    /**
     * Purge all messages in window for local_addr, which are <= low. Check if the window's highest received message is
     * > high: if true, retransmit all messages from high - win.high to sender
     * @param sender
     * @param highest_delivered
     * @param highest_seen
     */
    protected void stable(Address sender, long highest_delivered, long highest_seen) {
        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win == null)
            return;

        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- STABLE(").append(sender).
                    append(": ").append(highest_delivered).append("-").append(highest_seen).append(')'));

        win.ack(highest_delivered);
        long win_high=win.getHighest();
        if(win_high > highest_seen) {
            for(long seqno=highest_seen; seqno <= win_high; seqno++) {
                Message msg=win.get(seqno); // destination is still the same (the member which sent the STABLE message)
                if(msg != null)
                    down_prot.down(new Event(Event.MSG, msg));
            }
        }
    }

    @ManagedOperation(description="Sends a STABLE message to all senders. This causes message purging and potential" +
            " retransmissions from senders")
    public void sendStableMessages() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address dest=entry.getKey();
            ReceiverEntry val=entry.getValue();
            NakReceiverWindow win=val != null? val.received_msgs : null;
            if(win != null) {
                long low=win.getHighestDelivered();
                long high=win.getHighestReceived();

                if(val.last_highest == high) {
                    if(val.num_stable_msgs >= val.max_stable_msgs) {
                        continue;
                    }
                    else
                        val.num_stable_msgs++;
                }
                else {
                    val.last_highest=high;
                    val.num_stable_msgs=1;
                }
                sendStableMessage(dest, low, high);
            }
        }
    }

    protected void sendStableMessage(Address dest, long low, long high) {
        Message stable_msg=new Message(dest, null, null);
        Unicast2Header hdr=Unicast2Header.createStableHeader(low, high);
        stable_msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> STABLE(").append(dest).append(": ").append(low).append("-").append(high).append(")");
            log.trace(sb.toString());
        }
        down_prot.down(new Event(Event.MSG, stable_msg));
    }


    private void startStableTask() {
        if(stable_task_future == null || stable_task_future.isDone()) {
            final Runnable stable_task=new Runnable() {
                public void run() {
                    try {
                        sendStableMessages();
                    }
                    catch(Throwable t) {
                        log.error("sending of STABLE messages failed", t);
                    }
                }
            };
            stable_task_future=timer.scheduleAtFixedRate(stable_task, stable_interval, stable_interval, TimeUnit.MILLISECONDS);
            if(log.isTraceEnabled())
                log.trace("stable task started");
        }
    }


    private void stopStableTask() {
        if(stable_task_future != null) {
            stable_task_future.cancel(false);
            stable_task_future=null;
        }
    }
    

    /**
     * Removes and resets from connection table (which is already locked). Returns true if member was found, otherwise
     * false. This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    public void removeConnection(Address mbr) {
        SenderEntry entry=send_table.remove(mbr);
        if(entry != null)
            entry.reset();

        ReceiverEntry entry2=recv_table.remove(mbr);
        if(entry2 != null) {
            NakReceiverWindow win=entry2.received_msgs;
            if(win != null)
                sendStableMessage(mbr, win.getHighestDelivered(), win.getHighestReceived());
            entry2.reset();
        }
    }

    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        for(SenderEntry entry: send_table.values())
            entry.reset();
        send_table.clear();

        sendStableMessages();
        for(ReceiverEntry entry2: recv_table.values())
            entry2.reset();
        recv_table.clear();
    }


    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        Unicast2Header hdr=Unicast2Header.createXmitReqHeader(first_seqno, last_seqno);
        Message xmit_req=new Message(sender, null, null);
        xmit_req.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG, xmit_req));
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
    private void handleDataReceived(Address sender, long seqno, long conn_id, boolean first, Message msg, Event evt) {
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }

        ReceiverEntry entry=recv_table.get(sender);
        NakReceiverWindow win=entry != null? entry.received_msgs : null;

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
                        entry2.received_msgs.destroy();

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

        boolean added=win.add(seqno, msg); // win is guaranteed to be non-null if we get here
        num_msgs_received++;
        num_bytes_received+=msg.getLength();

        if(added && max_bytes > 0) {
            int bytes=entry.received_bytes.addAndGet(msg.getLength());
            if(bytes >= max_bytes) {
                entry.received_bytes_lock.lock();
                try {
                    entry.received_bytes.set(0);
                }
                finally {
                    entry.received_bytes_lock.unlock();
                }

                sendStableMessage(sender, win.getHighestDelivered(), win.getHighestReceived());
            }
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

        // try to remove (from the AckReceiverWindow) as many messages as possible and pass them up

        // Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
        // this is all the more important once we have a concurrent stack (http://jira.jboss.com/jira/browse/JGRP-181),
        // where lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time
        // We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
        // delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered only in the
        // order in which they were sent by their senders
        try {
            while(true) {
                List<Message> msgs=win.removeMany(processing, true, max_msg_batch_size); // remove my own messages
                if(msgs == null || msgs.isEmpty())
                    return;

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
        NakReceiverWindow win=new NakReceiverWindow(local_addr, sender, this, seqno-1, seqno-1, timer);
        win.setDiscardDeliveredMessages(true);
        ReceiverEntry entry=new ReceiverEntry(win, conn_id, max_stable_msgs);
        ReceiverEntry entry2=recv_table.putIfAbsent(sender, entry);
        if(entry2 != null)
            return entry2;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": created receiver window for " + sender + " at seqno=#" + seqno + " for conn-id=" + conn_id);
        return entry;
    }


    private void handleXmitRequest(Address sender, long low, long high) {
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- XMIT(").append(sender).
                    append(": #").append(low).append( "-").append(high).append(')'));

        SenderEntry entry=send_table.get(sender);
        AckSenderWindow win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            for(long i=low; i <= high; i++) {
                Message msg=win.get(i);
                if(msg == null) {
                    if(log.isWarnEnabled() && !local_addr.equals(sender)) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("(requester=").append(sender).append(", local_addr=").append(this.local_addr);
                        sb.append(") message ").append(sender).append("::").append(i);
                        sb.append(" not found in retransmission table of ").append(sender).append(":\n").append(win);
                        log.warn(sb.toString());
                    }
                    continue;
                }
                
                down_prot.down(new Event(Event.MSG, msg));
                num_xmits++;
            }
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
        Unicast2Header hdr=(Unicast2Header)copy.getHeader(this.id);
        Unicast2Header newhdr=hdr.copy();
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
        Unicast2Header hdr=Unicast2Header.createSendFirstSeqnoHeader();
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
    public static class Unicast2Header extends Header {
        public static final byte DATA             = 0;
        public static final byte XMIT_REQ         = 1;
        public static final byte SEND_FIRST_SEQNO = 2;
        public static final byte STABLE           = 3;

        byte    type;
        long    seqno;       // DATA, XMIT_REQ and STABLE
        long    high_seqno;  // XMIT_REQ and STABLE
        short   conn_id;     // DATA
        boolean first;       // DATA


        public Unicast2Header() {} // used for externalization

        public static Unicast2Header createDataHeader(long seqno, short conn_id, boolean first) {
            return new Unicast2Header(DATA, seqno, 0L, conn_id, first);
        }

        public static Unicast2Header createXmitReqHeader(long low, long high) {
            Unicast2Header retval=new Unicast2Header(XMIT_REQ, low);
            retval.high_seqno=high;
            return retval;
        }

        public static Unicast2Header createStableHeader(long low, long high) {
            Unicast2Header retval=new Unicast2Header(STABLE, low);
            retval.high_seqno=high;
            return retval;
        }

        public static Unicast2Header createSendFirstSeqnoHeader() {
            return new Unicast2Header(SEND_FIRST_SEQNO, 0L);
        }

        private Unicast2Header(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        private Unicast2Header(byte type, long seqno, long high, short conn_id, boolean first) {
            this.type=type;
            this.seqno=seqno;
            this.high_seqno=high;
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
                case DATA:             return "DATA";
                case XMIT_REQ:         return "XMIT_REQ";
                case SEND_FIRST_SEQNO: return "SEND_FIRST_SEQNO";
                case STABLE:           return "STABLE";
                default:               return "<unknown>";
            }
        }

        public final int size() {
            switch(type) {
                case DATA:
                    return Global.BYTE_SIZE *2 + Global.LONG_SIZE + Global.SHORT_SIZE;
                case XMIT_REQ:
                case STABLE:
                    return Global.BYTE_SIZE + Global.LONG_SIZE *2;
                case SEND_FIRST_SEQNO:
                    return Global.BYTE_SIZE;
            }
            return 0;
        }

        public Unicast2Header copy() {
            return new Unicast2Header(type, seqno, high_seqno, conn_id, first);
        }



        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    out.writeLong(seqno);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case XMIT_REQ:
                case STABLE:
                    out.writeLong(seqno);
                    out.writeLong(high_seqno);
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
                case XMIT_REQ:
                case STABLE:
                    seqno=in.readLong();
                    high_seqno=in.readLong();
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

        public SenderEntry(short send_conn_id) {
            this.send_conn_id=send_conn_id;
            sent_msgs=new AckSenderWindow();
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
        private final NakReceiverWindow  received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        private final long               recv_conn_id;
        final AtomicInteger              received_bytes=new AtomicInteger(0);
        final Lock                       received_bytes_lock=new ReentrantLock();

        private long                     last_highest=-1;
        private int                      num_stable_msgs=0;
        public final int                 max_stable_msgs;



        public ReceiverEntry(NakReceiverWindow received_msgs, long recv_conn_id, final int max_stable_msgs) {
            this.received_msgs=received_msgs;
            this.recv_conn_id=recv_conn_id;
            this.max_stable_msgs=max_stable_msgs;
        }

        void reset() {
            if(received_msgs != null)
                received_msgs.destroy();
            received_bytes.set(0);
            last_highest=-1;
            num_stable_msgs=0;
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
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * @deprecated Will be removed in 4.0
 */
@Deprecated
@MBean(description="Reliable unicast layer")
public class UNICAST extends Protocol implements AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */

    @Deprecated
    protected int[]  timeout= { 400, 800, 1600, 3200 }; // for AckSenderWindow: max time to wait for missing acks

    @Property(description="Max number of messages to be removed from a retransmit window. This property might " +
            "get removed anytime, so don't use it !")
    protected int    max_msg_batch_size=500;

    @Property(description="Time (in milliseconds) after which an idle incoming or outgoing connection is closed. The " +
      "connection will get re-established when used again. 0 disables connection reaping")
    protected long   conn_expiry_timeout=0;

    @Deprecated
    @Property(description="Size (in bytes) of a Segment in the segments table. Only for experts, do not use !",
              deprecatedMessage="not used anymore")
    protected int    segment_capacity=1000;


    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int    xmit_table_num_rows=100;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    protected int    xmit_table_msgs_per_row=1000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    protected long   xmit_table_max_compaction_time=10 * 60 * 1000;

    // @Property(description="Max time (in ms) after which a connection to a non-member is closed")
    protected long                   max_retransmit_time=60 * 1000L;

    @Property(description="Interval (in milliseconds) at which messages in the send windows are resent")
    protected long   xmit_interval=2000;

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    protected long   num_msgs_sent=0, num_msgs_received=0;
    protected long   num_acks_sent=0, num_acks_received=0, num_xmits=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */


    protected final ConcurrentMap<Address, SenderEntry>   send_table=Util.createConcurrentMap();
    protected final ConcurrentMap<Address, ReceiverEntry> recv_table=Util.createConcurrentMap();

    protected final ReentrantLock    recv_table_lock=new ReentrantLock();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>              xmit_task;

    protected volatile List<Address> members=new ArrayList<>(11);

    protected Address                local_addr=null;

    protected TimeScheduler          timer=null; // used for retransmissions (passed to AckSenderWindow)

    protected volatile boolean       running=false;

    protected short                  last_conn_id=0;

    protected AgeOutCache<Address>   cache=null;

    protected Future<?>              connection_reaper; // closes idle connections


    public int[] getTimeout() {return timeout;}

    @Deprecated
    @Property(name="timeout",converter=PropertyConverters.IntegerArray.class, deprecatedMessage="not used anymore")
    public void setTimeout(int[] val) {
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
    public String getMembers() {return members.toString();}

    @ManagedAttribute(description="Whether the ConnectionReaper task is running")
    public boolean isConnectionReaperRunning() {return connection_reaper != null && !connection_reaper.isDone();}

    @ManagedAttribute(description="Returns the number of outgoing (send) connections")
    public int getNumSendConnections() {
        return send_table.size();
    }

    @ManagedAttribute(description="Returns the number of incoming (receive) connections")
    public int getNumReceiveConnections() {
        return recv_table.size();
    }

    @ManagedAttribute(description="Returns the total number of outgoing (send) and incoming (receive) connections")
    public int getNumConnections() {
        return getNumReceiveConnections() + getNumSendConnections();
    }

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
    public long getNumMessagesSent() {return num_msgs_sent;}

    @ManagedAttribute
    public long getNumMessagesReceived() {return num_msgs_received;}


    @ManagedAttribute
    public long getNumAcksSent() {return num_acks_sent;}

    @ManagedAttribute
    public long getNumAcksReceived() {return num_acks_received;}

    @ManagedAttribute
    public long getNumXmits() {return num_xmits;}

    public long getMaxRetransmitTime() {return max_retransmit_time;}

    @Property(description="Max number of milliseconds we try to retransmit a message to any given member. After that, " +
            "the connection is removed. Any new connection to that member will start with seqno #1 again. 0 disables this")
    public void setMaxRetransmitTime(long max_retransmit_time) {
        this.max_retransmit_time=max_retransmit_time;
        if(cache != null && max_retransmit_time > 0)
            cache.setTimeout(max_retransmit_time);
    }

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

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

    /** Used for testing only */
    public boolean hasSendConnectionTo(Address dest) {
        return send_table.containsKey(dest);
    }

    /** The number of messages in all Entry.sent_msgs tables (haven't received an ACK yet) */
    @ManagedAttribute
    public int getNumUnackedMessages() {
        int num=0;
        for(SenderEntry entry: send_table.values()) {
                if(entry.sent_msgs != null)
                    num+=entry.sent_msgs.size();
            }
        return num;
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

    @ManagedAttribute(description="Total number of undelivered messages in all receive windows")
    public long getXmitTableUndeliveredMessages() {
        long retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.size();
        }
        return retval;
    }

    @ManagedAttribute(description="Total number of missing messages in all receive windows")
    public long getXmitTableMissingMessages() {
        long retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.getNumMissing();
        }
        return retval;
    }

    @ManagedAttribute(description="Number of compactions in all (receive and send) windows")
    public int getXmitTableNumCompactions() {
        int retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.getNumCompactions();
        }
        for(SenderEntry entry: send_table.values()) {
            if(entry.sent_msgs != null)
                retval+=entry.sent_msgs.getNumCompactions();
        }
        return retval;
    }

    @ManagedAttribute(description="Number of moves in all (receive and send) windows")
    public int getXmitTableNumMoves() {
        int retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.getNumMoves();
        }
        for(SenderEntry entry: send_table.values()) {
            if(entry.sent_msgs != null)
                retval+=entry.sent_msgs.getNumMoves();
        }
        return retval;
    }

    @ManagedAttribute(description="Number of resizes in all (receive and send) windows")
    public int getXmitTableNumResizes() {
        int retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.getNumResizes();
        }
        for(SenderEntry entry: send_table.values()) {
            if(entry.sent_msgs != null)
                retval+=entry.sent_msgs.getNumResizes();
        }
        return retval;
    }

    @ManagedAttribute(description="Number of purges in all (receive and send) windows")
    public int getXmitTableNumPurges() {
        int retval=0;
        for(ReceiverEntry entry: recv_table.values()) {
            if(entry.received_msgs != null)
                retval+=entry.received_msgs.getNumPurges();
        }
        for(SenderEntry entry: send_table.values()) {
            if(entry.sent_msgs != null)
                retval+=entry.sent_msgs.getNumPurges();
        }
        return retval;
    }

    @ManagedOperation(description="Prints the contents of the receive windows for all members")
    public String printReceiveWindowMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address addr=entry.getKey();
            Table<Message> buf=entry.getValue().received_msgs;
            ret.append(addr).append(": ").append(buf.toString()).append('\n');
        }
        return ret.toString();
    }

    @ManagedOperation(description="Prints the contents of the send windows for all members")
    public String printSendWindowMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            Address addr=entry.getKey();
            Table<Message> buf=entry.getValue().sent_msgs;
            ret.append(addr).append(": ").append(buf.toString()).append('\n');
        }
        return ret.toString();
    }


    public void resetStats() {
        num_msgs_sent=num_msgs_received=num_acks_sent=num_acks_received=0;
        num_xmits=0;
    }


    public Map<String, Object> dumpStats() {
        Map<String, Object> m=super.dumpStats();
        m.put("num_unacked_msgs", getNumUnackedMessages());
        m.put("num_msgs_in_recv_windows", getNumberOfMessagesInReceiveWindows());
        return m;
    }



    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<>(timer, max_retransmit_time, this);
        running=true;
        if(conn_expiry_timeout > 0)
            startConnectionReaper();
        startRetransmitTask();
    }

    public void stop() {
        running=false;
        stopRetransmitTask();
        stopConnectionReaper();
        removeAllConnections();
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))  // only handle unicast messages
                    break;  // pass up

                UnicastHeader hdr=(UnicastHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                Address sender=msg.getSrc();
                switch(hdr.type) {
                    case UnicastHeader.DATA:      // received regular message
                        handleDataReceived(sender, hdr.seqno, hdr.conn_id, hdr.first, msg, evt);
                        break;
                    default:
                        handleUpEvent(sender, hdr);
                        break;
                }
                return null;
        }
        return up_prot.up(evt);   // Pass up to the layer above us
    }


    protected void handleUpEvent(Address sender, UnicastHeader hdr) {
        switch(hdr.type) {
            case UnicastHeader.DATA:      // received regular message
                throw new IllegalStateException("header of type DATA is not supposed to be handled by this method");
            case UnicastHeader.ACK:  // received ACK for previously sent message
                handleAckReceived(sender, hdr.seqno, hdr.conn_id);
                break;
            case UnicastHeader.SEND_FIRST_SEQNO:
                handleResendingOfFirstMessage(sender, hdr.seqno);
                break;
            default:
                log.error(Util.getMessage("UnicastHeaderType"), hdr.type);
                break;
        }
    }


    public void up(MessageBatch batch) {
        if(batch.dest() == null) { // not a unicast batch
            up_prot.up(batch);
            return;
        }

        int                       size=batch.size();
        Map<Short,List<Message>>  msgs=new TreeMap<>(); // map of messages, keyed by conn-id
        for(Message msg: batch) {
            if(msg == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
                continue;
            UnicastHeader hdr=(UnicastHeader)msg.getHeader(id);
            if(hdr == null)
                continue;
            batch.remove(msg); // remove the message from the batch, so it won't be passed up the stack

            if(hdr.type != UnicastHeader.DATA) {
                try {
                    handleUpEvent(msg.getSrc(), hdr);
                }
                catch(Throwable t) { // we cannot let an exception terminate the processing of this batch
                    log.error(local_addr + ": failed handling event", t);
                }
                continue;
            }

            List<Message> list=msgs.get(hdr.conn_id);
            if(list == null)
                msgs.put(hdr.conn_id, list=new ArrayList<>(size));
            list.add(msg);
        }

        if(!msgs.isEmpty())
            handleBatchReceived(batch.sender(), msgs); // process msgs:
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    public Object down(Event evt) {
        switch (evt.getType()) {

            case Event.MSG: // Add UnicastHeader, add to AckSenderWindow and pass down
                Message msg=(Message)evt.getArg();
                Address dst=msg.getDest();

                /* only handle unicast messages */
                if (dst == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
                    break;

                if(!running) {
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
                            log.trace(local_addr + ": created sender window for " + dst + " (conn-id=" + entry.send_conn_id + ")");
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }


                short send_conn_id=entry.send_conn_id;
                long seqno=entry.sent_msgs_seqno.getAndIncrement();
                long sleep=10;
                do {
                    try {
                        msg.putHeader(this.id, UnicastHeader.createDataHeader(seqno,send_conn_id,seqno == DEFAULT_FIRST_SEQNO));
                        entry.sent_msgs.add(seqno,msg);  // add *including* UnicastHeader, adds to retransmitter
                        if(conn_expiry_timeout > 0)
                            entry.update();
                        break;
                    }
                    catch(Throwable t) {
                        if(!running)
                            break;
                        Util.sleep(sleep);
                        sleep=Math.min(5000, sleep*2);
                    }
                }
                while(running);

                if(log.isTraceEnabled()) {
                    StringBuilder sb=new StringBuilder();
                    sb.append(local_addr).append(" --> DATA(").append(dst).append(": #").append(seqno).
                      append(", conn_id=").append(send_conn_id);
                    if(seqno == DEFAULT_FIRST_SEQNO) sb.append(", first");
                    sb.append(')');
                    log.trace(sb);
                }

                num_msgs_sent++;
                return down_prot.down(evt);

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                View view=(View)evt.getArg();
                List<Address> new_members=view.getMembers();
                Set<Address> non_members=new HashSet<>(send_table.keySet());
                non_members.addAll(recv_table.keySet());

                members=new_members;
                non_members.removeAll(new_members);
                if(cache != null)
                    cache.removeAll(new_members);

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
     * Removes and resets from connection table (which is already locked). Returns true if member was found,
     * otherwise false. This method is public only so it can be invoked by unit testing, but should not otherwise be
     * used ! 
     */
    public void removeConnection(Address mbr) {
        removeSendConnection(mbr);
        removeReceiveConnection(mbr);
    }

    public void removeSendConnection(Address mbr) {
        send_table.remove(mbr);
    }

    public void removeReceiveConnection(Address mbr) {
        recv_table.remove(mbr);
    }


    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        send_table.clear();
        recv_table.clear();
    }



    /** Called by AckSenderWindow to resend messages for which no ACK has been received yet */
    public void retransmit(Message msg) {
        if(log.isTraceEnabled()) {
            UnicastHeader hdr=(UnicastHeader)msg.getHeader(id);
            long seqno=hdr != null? hdr.seqno : -1;
            log.trace(local_addr + " --> XMIT(" + msg.getDest() + ": #" + seqno + ')');
        }
        down_prot.down(new Event(Event.MSG,msg));
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
    protected void handleDataReceived(Address sender, long seqno, short conn_id,  boolean first, Message msg, Event evt) {
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" <-- DATA(").append(sender).append(": #").append(seqno);
            if(conn_id != 0) sb.append(", conn_id=").append(conn_id);
            if(first) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }

        ReceiverEntry entry=getReceiverEntry(sender, seqno, first, conn_id);
        if(entry == null)
            return;
        if(conn_expiry_timeout > 0)
            entry.update();
        Table<Message> win=entry.received_msgs;
        boolean added=win.add(seqno, msg); // win is guaranteed to be non-null if we get here
        num_msgs_received++;

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(msg.isFlagSet(Message.Flag.OOB) && added) {
            try {
                up_prot.up(evt);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("CouldnTDeliverOOBMessage") + msg, t);
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
        removeAndDeliver(processing, win, sender);
        sendAck(sender, win.getHighestDelivered(), conn_id);
    }


    protected void handleBatchReceived(Address sender, Map<Short,List<Message>> map) {
        for(Map.Entry<Short,List<Message>> element: map.entrySet()) {
            final List<Message> msg_list=element.getValue();
            if(log.isTraceEnabled()) {
                StringBuilder sb=new StringBuilder();
                sb.append(local_addr).append(" <-- DATA(").append(sender).append(": " + printMessageList(msg_list)).append(')');
                log.trace(sb);
            }

            short          conn_id=element.getKey();
            ReceiverEntry  entry=null;
            for(Message msg: msg_list) {
                UnicastHeader hdr=(UnicastHeader)msg.getHeader(id);
                entry=getReceiverEntry(sender, hdr.seqno, hdr.first, conn_id);
                if(entry == null)
                    continue;
                Table<Message> win=entry.received_msgs;
                boolean msg_added=win.add(hdr.seqno, msg); // win is guaranteed to be non-null if we get here
                num_msgs_received++;

                if(hdr.first && msg_added)
                    sendAck(sender, hdr.seqno, conn_id); // send an ack immediately when we received the first message of a conn

                // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
                // http://jira.jboss.com/jira/browse/JGRP-377
                if(msg.isFlagSet(Message.Flag.OOB) && msg_added) {
                    try {
                        up_prot.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("CouldnTDeliverOOBMessage") + msg, t);
                    }
                }
            }
            if(entry != null && conn_expiry_timeout > 0)
                entry.update();
        }

        ReceiverEntry entry=recv_table.get(sender);
        Table<Message> win=entry != null? entry.received_msgs : null;
        if(win != null) {
            final AtomicBoolean processing=win.getProcessing();
            if(processing.compareAndSet(false, true)) {
                removeAndDeliver(processing, win, sender);
                sendAck(sender, win.getHighestDeliverable(), entry.recv_conn_id);
            }
        }
    }


    /**
     * Try to remove as many messages as possible from the table as pass them up.
     * Prevents concurrent passing up of messages by different threads (http://jira.jboss.com/jira/browse/JGRP-198);
     * lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time.
     * We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
     * delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered in the
     * order in which they were sent
     */
    protected int removeAndDeliver(final AtomicBoolean processing, Table<Message> win, Address sender) {
        int retval=0;
        boolean released_processing=false;
        try {
            while(true) {
                List<Message> list=win.removeMany(processing, true, max_msg_batch_size);
                if(list == null) {
                    released_processing=true;
                    return retval;
                }

                MessageBatch batch=new MessageBatch(local_addr, sender, null, false, list);
                for(Message msg_to_deliver: batch) {
                    // discard OOB msg: it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-377)
                    if(msg_to_deliver.isFlagSet(Message.Flag.OOB))
                        batch.remove(msg_to_deliver);
                }

                try {
                    if(log.isTraceEnabled()) {
                        Message first=batch.first(), last=batch.last();
                        StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                        if(first != null && last != null) {
                            UnicastHeader hdr1=(UnicastHeader)first.getHeader(id), hdr2=(UnicastHeader)last.getHeader(id);
                            sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                        }
                        sb.append(" (" + batch.size()).append(" messages)");
                        log.trace(sb);
                    }
                    up_prot.up(batch);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FailedToDeliverBatch") + batch, t);
                }
            }
        }
        finally {
            // processing is always set in win.remove(processing) above and never here ! This code is just a
            // 2nd line of defense should there be an exception before win.remove(processing) sets processing
            if(!released_processing)
                processing.set(false);
        }
    }

    protected ReceiverEntry getReceiverEntry(Address sender, long seqno, boolean first, short conn_id) {
        ReceiverEntry entry=recv_table.get(sender);
        if(entry != null && entry.recv_conn_id == conn_id)
            return entry;

        recv_table_lock.lock();
        try {
            entry=recv_table.get(sender);
            if(first) {
                if(entry == null) {
                    entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                }
                else {  // entry != null && win != null
                    if(conn_id != entry.recv_conn_id) {
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": conn_id=" + conn_id + " != " + entry.recv_conn_id + "; resetting receiver window");

                        recv_table.remove(sender);
                        entry=getOrCreateReceiverEntry(sender, seqno, conn_id);
                    }
                    else {
                        ;
                    }
                }
            }
            else { // entry == null && win == null OR entry != null && win == null OR entry != null && win != null
                if(entry == null || entry.recv_conn_id != conn_id) {
                    recv_table_lock.unlock();
                    sendRequestForFirstSeqno(sender, seqno); // drops the message and returns (see below)
                    return null;
                }
            }
            return entry;
        }
        finally {
            if(recv_table_lock.isHeldByCurrentThread())
                recv_table_lock.unlock();
        }
    }


    protected ReceiverEntry getOrCreateReceiverEntry(Address sender, long seqno, short conn_id) {
        Table<Message> table=new Table<>(xmit_table_num_rows, xmit_table_msgs_per_row, seqno-1,
                                                xmit_table_resize_factor, xmit_table_max_compaction_time);
        ReceiverEntry entry=new ReceiverEntry(table, conn_id);
        ReceiverEntry entry2=recv_table.putIfAbsent(sender, entry);
        if(entry2 != null)
            return entry2;
        if(log.isTraceEnabled())
            log.trace(local_addr + ": created receiver window for " + sender + " at seqno=#" + seqno + " for conn-id=" + conn_id);
        return entry;
    }


    protected void handleAckReceived(Address sender, long seqno, short conn_id) {
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- ACK(").append(sender).
              append(": #").append(seqno).append(", conn-id=").append(conn_id).append(')'));
        SenderEntry entry=send_table.get(sender);

        if(entry != null && entry.send_conn_id != conn_id) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": my conn_id (" + entry.send_conn_id +
                            ") != received conn_id (" + conn_id + "); discarding ACK");
            return;
        }

        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            win.purge(seqno, true); // removes all messages <= seqno (forced purge)
            num_acks_received++;
        }
    }


    
    /**
     * We need to resend our first message with our conn_id
     * @param sender
     * @param seqno Resend the non null messages in the range [lowest .. seqno]
     */
    protected void handleResendingOfFirstMessage(Address sender, long seqno) {
        if(log.isTraceEnabled())
            log.trace(local_addr + " <-- SEND_FIRST_SEQNO(" + sender + "," + seqno + ")");
        SenderEntry entry=send_table.get(sender);
        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win == null) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": sender window for " + sender + " not found");
            return;
        }

        boolean first_sent=false;
        for(long i=win.getLow() +1; i <= seqno; i++) {
            Message rsp=win.get(i);
            if(rsp == null)
                continue;
            if(first_sent) {
                down_prot.down(new Event(Event.MSG, rsp));
            }
            else {
                first_sent=true;
                // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
                // the headers and therefore we'd modify the original message in the sender retransmission window
                // (https://jira.jboss.org/jira/browse/JGRP-965)
                Message copy=rsp.copy();
                UnicastHeader hdr=(UnicastHeader)copy.getHeader(this.id);
                UnicastHeader newhdr=hdr.copy();
                newhdr.first=true;
                copy.putHeader(this.id, newhdr);
                down_prot.down(new Event(Event.MSG, copy));
            }
        }
    }

    protected void startRetransmitTask() {
         if(xmit_task == null || xmit_task.isDone())
             xmit_task=timer.scheduleWithFixedDelay(new RetransmitTask(), 0, xmit_interval, TimeUnit.MILLISECONDS);
    }

    protected void stopRetransmitTask() {
        if(xmit_task != null) {
            xmit_task.cancel(true);
            xmit_task=null;
        }
    }



    protected void sendAck(Address dst, long seqno, short conn_id) {
        if(!running) // if we are disconnected, then don't send any acks which throw exceptions on shutdown
            return;
        Message ack=new Message(dst).setFlag(Message.Flag.INTERNAL)
          .putHeader(this.id, UnicastHeader.createAckHeader(seqno, conn_id));
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" --> ACK(").append(dst).
              append(": #").append(seqno).append(')'));
        try {
            down_prot.down(new Event(Event.MSG, ack));
            num_acks_sent++;
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedSendingACK") + seqno + ") to " + dst, t);
        }
    }

    protected synchronized void startConnectionReaper() {
        if(connection_reaper == null || connection_reaper.isDone())
            connection_reaper=timer.scheduleWithFixedDelay(new ConnectionReaper(), conn_expiry_timeout, conn_expiry_timeout, TimeUnit.MILLISECONDS);
    }

    protected synchronized void stopConnectionReaper() {
        if(connection_reaper != null)
            connection_reaper.cancel(false);
    }
   
    protected synchronized short getNewConnectionId() {
        short retval=last_conn_id;
        if(last_conn_id >= Short.MAX_VALUE || last_conn_id < 0)
            last_conn_id=0;
        else
            last_conn_id++;
        return retval;
    }


    protected void sendRequestForFirstSeqno(Address dest, long seqno_received) {
        Message msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
        UnicastHeader hdr=UnicastHeader.createSendFirstSeqnoHeader(seqno_received);
        msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> SEND_FIRST_SEQNO(" + dest + "," + seqno_received + ")");
        down_prot.down(new Event(Event.MSG, msg));
    }

    @ManagedOperation(description="Closes connections that have been idle for more than conn_expiry_timeout ms")
    public void reapIdleConnections() {
        // remove expired connections from send_table
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            SenderEntry val=entry.getValue();
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                removeSendConnection(entry.getKey());
                if(log.isDebugEnabled())
                    log.debug(local_addr + ": removed expired connection for " + entry.getKey() +
                                " (" + age + " ms old) from send_table");
            }
        }

        // remove expired connections from recv_table
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            ReceiverEntry val=entry.getValue();
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                removeReceiveConnection(entry.getKey());
                if(log.isDebugEnabled())
                    log.debug(local_addr + ": removed expired connection for " + entry.getKey() +
                                " (" + age + " ms old) from recv_table");
            }
        }
    }


    protected String printMessageList(List<Message> list) {
        StringBuilder sb=new StringBuilder();
        int size=list.size();
        Message first=size > 0? list.get(0) : null, second=size > 1? list.get(size-1) : first;
        UnicastHeader hdr;
        if(first != null) {
            hdr=(UnicastHeader)first.getHeader(id);
            if(hdr != null)
                sb.append("#" + hdr.seqno);
        }
        if(second != null) {
            hdr=(UnicastHeader)second.getHeader(id);
            if(hdr != null)
                sb.append(" - #" + hdr.seqno);
        }
        return sb.toString();
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

        public static UnicastHeader createAckHeader(long seqno, short conn_id) {
            return new UnicastHeader(ACK, seqno, conn_id, false);
        }

        public static UnicastHeader createSendFirstSeqnoHeader(long seqno_received) {
            return new UnicastHeader(SEND_FIRST_SEQNO, seqno_received);
        }

        protected UnicastHeader(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        protected UnicastHeader(byte type, long seqno, short conn_id, boolean first) {
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
            int retval=Global.BYTE_SIZE;     // type
            switch(type) {
                case DATA:
                    retval+=Bits.size(seqno) // seqno
                      + Global.SHORT_SIZE    // conn_id
                      + Global.BYTE_SIZE;    // first
                    break;
                case ACK:
                    retval+=Bits.size(seqno) + Global.SHORT_SIZE; // conn_id
                    break;
                case SEND_FIRST_SEQNO:
                    retval+=Bits.size(seqno);
                    break;
            }
            return retval;
        }

        public UnicastHeader copy() {
            return new UnicastHeader(type, seqno, conn_id, first);
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    Bits.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case ACK:
                    Bits.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    break;
                case SEND_FIRST_SEQNO:
                    Bits.writeLong(seqno, out);
                    break;
            }
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            switch(type) {
                case DATA:
                    seqno=Bits.readLong(in);
                    conn_id=in.readShort();
                    first=in.readBoolean();
                    break;
                case ACK:
                    seqno=Bits.readLong(in);
                    conn_id=in.readShort();
                    break;
                case SEND_FIRST_SEQNO:
                    seqno=Bits.readLong(in);
                    break;
            }
        }
    }



    protected final class SenderEntry {
        // stores (and retransmits) msgs sent by us to a certain peer
        final Table<Message>        sent_msgs;
        final AtomicLong            sent_msgs_seqno=new AtomicLong(DEFAULT_FIRST_SEQNO);   // seqno for msgs sent by us
        final short                 send_conn_id;
        protected final AtomicLong  timestamp=new AtomicLong(0);
        final Lock                  lock=new ReentrantLock();

        public SenderEntry(short send_conn_id) {
            this.send_conn_id=send_conn_id;
            this.sent_msgs=new Table<>(xmit_table_num_rows, xmit_table_msgs_per_row, 0,
                                              xmit_table_resize_factor, xmit_table_max_compaction_time);
            update();
        }

        void update() {timestamp.set(System.currentTimeMillis());}
        long age() {return System.currentTimeMillis() - timestamp.longValue();}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append(sent_msgs).append(", ");
            sb.append("send_conn_id=" + send_conn_id).append(" (" + age() + " ms old)");
            return sb.toString();
        }
    }

    protected static final class ReceiverEntry {
        protected final Table<Message>  received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        protected final short           recv_conn_id;
        protected final AtomicLong      timestamp=new AtomicLong(0);

        
        public ReceiverEntry(Table<Message> received_msgs, short recv_conn_id) {
            this.received_msgs=received_msgs;
            this.recv_conn_id=recv_conn_id;
            update();
        }

        void update() {timestamp.set(System.currentTimeMillis());}
        long age() {return System.currentTimeMillis() - timestamp.longValue();}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(received_msgs != null)
                sb.append(received_msgs).append(", ");
            sb.append("recv_conn_id=" + recv_conn_id);
            sb.append(" (" + age() + " ms old)");
            return sb.toString();
        }
    }

    protected class ConnectionReaper implements Runnable {
        public void run() {
            reapIdleConnections();
        }

        public String toString() {
            return UNICAST.class.getSimpleName() + ": ConnectionReaper (interval=" + conn_expiry_timeout + " ms)";
        }
    }


    /**
     * Retransmitter task which periodically (every xmit_interval ms) looks at all the retransmit (send) tables and
     * re-sends messages for which we haven't received an ack yet
     */
    protected class RetransmitTask implements Runnable {

        public void run() {
            for(SenderEntry val: send_table.values()) {
                Table<Message> buf=val != null? val.sent_msgs : null;
                if(buf != null && !buf.isEmpty()) {
                    long from=buf.getHighestDelivered()+1, to=buf.getHighestReceived();
                    List<Message> list=buf.get(from, to);
                    if(list != null) {
                        for(Message msg: list)
                            retransmit(msg);
                    }
                }
            }
        }

        public String toString() {
            return UNICAST.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

}
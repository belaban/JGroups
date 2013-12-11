package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
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
import java.util.concurrent.locks.ReentrantLock;


/**
 * Reliable unicast protocol using a combination of positive and negative acks. See docs/design/UNICAST3.txt for details.
 * @author Bela Ban
 * @since  3.3
 */
@MBean(description="Reliable unicast layer")
public class UNICAST3 extends Protocol implements AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */

    @Property(description="Max number of messages to be removed from a retransmit window. This property might " +
            "get removed anytime, so don't use it !")
    protected int     max_msg_batch_size=500;

    @Property(description="Time (in milliseconds) after which an idle incoming or outgoing connection is closed. The " +
      "connection will get re-established when used again. 0 disables connection reaping")
    protected long    conn_expiry_timeout=60000 * 2;

    @Property(description="Time (in ms) until a connection marked to be closed will get removed. 0 disables this")
    protected long    conn_close_timeout=60000;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int     xmit_table_num_rows=100;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    protected int     xmit_table_msgs_per_row=1000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double  xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    protected long    xmit_table_max_compaction_time=10 * 60 * 1000;

    // @Property(description="Max time (in ms) after which a connection to a non-member is closed")
    protected long    max_retransmit_time=60 * 1000L;

    @Property(description="Interval (in milliseconds) at which messages in the send windows are resent")
    protected long    xmit_interval=500;

    @Property(description="If true, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Send an ack for a batch immediately instead of using a delayed ack",
              deprecatedMessage="replaced by ack_threshold")
    @Deprecated
    protected boolean ack_batches_immediately=true;

    @Property(description="Send an ack immediately when a batch of ack_threshold (or more) messages is received. " +
      "Otherwise send delayed acks. If 1, ack single messages (similar to UNICAST)")
    protected int     ack_threshold=5;

    /* --------------------------------------------- JMX  ---------------------------------------------- */


    protected long    num_msgs_sent=0, num_msgs_received=0;
    protected long    num_acks_sent=0, num_acks_received=0, num_xmits=0;

    @ManagedAttribute(description="Number of retransmit requests received")
    protected final AtomicLong xmit_reqs_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit requests sent")
    protected final AtomicLong xmit_reqs_sent=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses sent")
    protected final AtomicLong xmit_rsps_sent=new AtomicLong(0);

    /* --------------------------------------------- Fields ------------------------------------------------ */


    protected final ConcurrentMap<Address, SenderEntry>   send_table=Util.createConcurrentMap();
    protected final ConcurrentMap<Address, ReceiverEntry> recv_table=Util.createConcurrentMap();

    protected final ReentrantLock      recv_table_lock=new ReentrantLock();

    /** Used by the retransmit task to keep the last retransmitted seqno per sender (https://issues.jboss.org/browse/JGRP-1539) */
    protected final Map<Address,Long>  xmit_task_map=new HashMap<Address,Long>();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                xmit_task;

    protected volatile List<Address>   members=new ArrayList<Address>(11);

    protected Address                  local_addr;

    protected TimeScheduler            timer; // used for retransmissions (passed to AckSenderWindow)

    protected volatile boolean         running=false;

    protected short                    last_conn_id;

    protected AgeOutCache<Address>     cache;




    public void setMaxMessageBatchSize(int size) {
        if(size >= 1)
            max_msg_batch_size=size;
    }

    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    @ManagedAttribute
    public String getMembers() {return members.toString();}

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
        SenderEntry entry=send_table.get(dest);
        return entry != null && entry.state() == State.OPEN;
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
            cache=new AgeOutCache<Address>(timer, max_retransmit_time, this);
        running=true;
        startRetransmitTask();
    }

    public void stop() {
        sendPendingAcks();
        running=false;
        stopRetransmitTask();
        xmit_task_map.clear();
        removeAllConnections();
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))  // only handle unicast messages
                    break;  // pass up

                Header hdr=(Header)msg.getHeader(this.id);
                if(hdr == null)
                    break;
                Address sender=msg.getSrc();
                switch(hdr.type) {
                    case Header.DATA:      // received regular message
                        handleDataReceived(sender, hdr.seqno, hdr.conn_id, hdr.first, msg, evt);
                        break; // we pass the deliverable message up in handleDataReceived()
                    default:
                        handleUpEvent(sender, msg, hdr);
                        break;
                }
                return null;
        }

        return up_prot.up(evt);   // Pass up to the layer above us
    }

    protected void handleUpEvent(Address sender, Message msg, Header hdr) {
        switch(hdr.type) {
            case Header.DATA:  // received regular message
                throw new IllegalStateException("header of type DATA is not supposed to be handled by this method");
            case Header.ACK:   // received ACK for previously sent message
                handleAckReceived(sender, hdr.seqno, hdr.conn_id);
                break;
            case Header.SEND_FIRST_SEQNO:
                handleResendingOfFirstMessage(sender);
                break;
            case Header.XMIT_REQ:  // received ACK for previously sent message
                handleXmitRequest(sender, (SeqnoList)msg.getObject());
                break;
            case Header.CLOSE:
                log.trace(local_addr + "%s <-- CLOSE(%s: conn-id=%s)", local_addr, sender, hdr.conn_id);
                ReceiverEntry entry=recv_table.get(sender);
                if(entry != null && entry.connId() == hdr.conn_id) {
                    recv_table.remove(sender, entry);
                    log.trace("%s: removed receive connection for %s", local_addr, sender);
                }
                break;
            default:
                log.error(Util.getMessage("TypeNotKnown"), local_addr, hdr.type);
                break;
        }
    }


    public void up(MessageBatch batch) {
        if(batch.dest() == null) { // not a unicast batch
            up_prot.up(batch);
            return;
        }

        int size=batch.size();
        Map<Short,List<Tuple<Long,Message>>> msgs=new LinkedHashMap<Short,List<Tuple<Long,Message>>>();
        ReceiverEntry entry=recv_table.get(batch.sender());

        for(Message msg: batch) {
            if(msg == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
                continue;
            Header hdr=(Header)msg.getHeader(id);
            if(hdr == null)
                continue;
            batch.remove(msg); // remove the message from the batch, so it won't be passed up the stack

            if(hdr.type != Header.DATA) {
                try {
                    handleUpEvent(msg.getSrc(), msg, hdr);
                }
                catch(Throwable t) { // we cannot let an exception terminate the processing of this batch
                    log.error(Util.getMessage("FailedHandlingEvent"), local_addr, t);
                }
                continue;
            }

            List<Tuple<Long,Message>> list=msgs.get(hdr.conn_id);
            if(list == null)
                msgs.put(hdr.conn_id, list=new ArrayList<Tuple<Long,Message>>(size));
            list.add(new Tuple<Long,Message>(hdr.seqno(), msg));

            if(hdr.first)
                entry=getReceiverEntry(batch.sender(), hdr.seqno(), hdr.first, hdr.connId());
        }

        if(!msgs.isEmpty()) {
            if(entry == null)
                sendRequestForFirstSeqno(batch.sender());
            else {
                if(msgs.keySet().retainAll(Arrays.asList(entry.connId()))) // remove all conn-ids that don't match
                    sendRequestForFirstSeqno(batch.sender());
                List<Tuple<Long,Message>> list=msgs.get(entry.connId());
                if(list != null && !list.isEmpty())
                    handleBatchReceived(entry, batch.sender(), list, batch.mode() == MessageBatch.Mode.OOB);
            }
        }

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
                    log.trace("%s: discarded message as start() has not yet been called, message: %s", local_addr, msg);
                    return null;
                }

                SenderEntry entry=send_table.get(dst);
                if(entry == null || entry.state() == State.CLOSED) {
                    if(entry != null)
                        send_table.remove(dst, entry);
                    entry=new SenderEntry(getNewConnectionId());
                    SenderEntry existing=send_table.putIfAbsent(dst, entry);
                    if(existing != null)
                        entry=existing;
                    else {
                        log.trace("%s: created sender window for %s (conn-id=%s)", local_addr, dst, entry.connId());
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }

                if(entry.state() == State.CLOSING)
                    entry.state(State.OPEN);

                short send_conn_id=entry.connId();
                long seqno=entry.sent_msgs_seqno.getAndIncrement();
                long sleep=10;
                while(running) {
                    try {
                        msg.putHeader(this.id,Header.createDataHeader(seqno,send_conn_id,seqno == DEFAULT_FIRST_SEQNO));
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
                Set<Address> non_members=new HashSet<Address>(send_table.keySet());
                non_members.addAll(recv_table.keySet());

                members=new_members;
                non_members.removeAll(new_members);
                if(cache != null)
                    cache.removeAll(new_members);

                if(!non_members.isEmpty()) {
                    log.trace("%s: removing non members %s", local_addr, non_members);
                    for(Address non_mbr: non_members)
                        closeConnection(non_mbr);
                }
                if(!new_members.isEmpty()) {
                    for(Address mbr: new_members) {
                        Entry e=send_table.get(mbr);
                        if(e != null && e.state() == State.CLOSING)
                            e.state(State.OPEN);
                        e=recv_table.get(mbr);
                        if(e != null && e.state() == State.CLOSING)
                            e.state(State.OPEN);
                    }
                }
                xmit_task_map.keySet().retainAll(new_members);
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
    public void closeConnection(Address mbr) {
        closeSendConnection(mbr);
        closeReceiveConnection(mbr);
    }

    public void closeSendConnection(Address mbr) {
        SenderEntry entry=send_table.get(mbr);
        if(entry != null)
            entry.state(State.CLOSING).update();
    }

    protected void removeSendConnection(Address mbr) {
        SenderEntry entry=send_table.remove(mbr);
        if(entry != null) {
            entry.state(State.CLOSED);
            sendClose(mbr, entry.connId());
        }
    }

    public void closeReceiveConnection(Address mbr) {
        ReceiverEntry entry=recv_table.get(mbr);
        if(entry != null)
            entry.state(State.CLOSING).update();
    }

    protected void removeReceiveConnection(Address mbr) {
        ReceiverEntry entry=recv_table.remove(mbr);
        if(entry != null)
            entry.state(State.CLOSED);
    }


    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        send_table.clear();
        recv_table.clear();
    }


    /** Sends a retransmit request to the given sender */
    protected void retransmit(SeqnoList missing, Address sender) {
        Message xmit_msg=new Message(sender, missing).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(id, Header.createXmitReqHeader());
        log.trace("%s: sending XMIT_REQ (%s) to %s", local_addr, missing, sender);
        down_prot.down(new Event(Event.MSG, xmit_msg));
        xmit_reqs_sent.addAndGet(missing.size());
    }


    /** Called by the sender to resend messages for which no ACK has been received yet */
    protected void retransmit(Message msg) {
        if(log.isTraceEnabled()) {
            Header hdr=(Header)msg.getHeader(id);
            long seqno=hdr != null? hdr.seqno : -1;
            log.trace("%s --> XMIT(%s: #%d)", local_addr, msg.getDest(), seqno);
        }
        down_prot.down(new Event(Event.MSG, msg));
        num_xmits++;
    }

    /**
     * Called by AgeOutCache, to removed expired connections
     * @param key
     */
    public void expired(Address key) {
        if(key != null) {
            log.debug("%s: removing connection to %s because it expired", local_addr, key);
            closeConnection(key);
        }
    }


    /**
     * Check whether the hashtable contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    protected void handleDataReceived(Address sender, long seqno, short conn_id,  boolean first, Message msg, Event evt) {
        if(log.isTraceEnabled())
            log.trace("%s <-- DATA(%s: #%d, conn_id=%d%s)", local_addr, sender, seqno, conn_id, first? ", first" : "");

        ReceiverEntry entry=getReceiverEntry(sender, seqno, first, conn_id);
        if(entry == null)
            return;
        if(conn_expiry_timeout > 0)
            entry.update();
        if(entry.state() == State.CLOSING)
            entry.state(State.OPEN);
        boolean oob=msg.isFlagSet(Message.Flag.OOB);
        if(oob) // done so this thread delivers the message and no work stealing for OOB msgs is performed (JGRP-1733)
            msg.setTransientFlag(Message.TransientFlag.OOB_DELIVERED);
        Table<Message> win=entry.received_msgs;
        boolean added=win.add(seqno, msg); // win is guaranteed to be non-null if we get here
        num_msgs_received++;

        if(ack_threshold <= 1)
            sendAck(sender, win.getHighestDeliverable(), entry.connId());
        else
            entry.sendAck(true); // will be sent delayed (on the next xmit_interval)

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(oob && added) {
            if(log.isTraceEnabled())
                log.trace("%s: delivering %s#%s", local_addr, sender, seqno);
            try {
                up_prot.up(evt);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "OOB message", msg, t);
            }
        }

        final AtomicBoolean processing=win.getProcessing();
        if(processing.compareAndSet(false, true))
            removeAndDeliver(processing, win, sender);
    }



    protected void handleBatchReceived(final ReceiverEntry entry, Address sender, List<Tuple<Long,Message>> msgs, boolean oob) {
        if(log.isTraceEnabled())
            log.trace("%s <-- DATA(%s: %s)", local_addr, sender, printMessageList(msgs));

        int batch_size=msgs.size();
        Table<Message> win=entry.received_msgs;
        num_msgs_received+=batch_size;
        boolean added=oob ? win.add(msgs, true) : win.add(msgs);

        if(conn_expiry_timeout > 0)
            entry.update();
        if(entry.state() == State.CLOSING)
            entry.state(State.OPEN);

        if(batch_size >= ack_threshold)
            sendAck(sender, win.getHighestDeliverable(), entry.connId());
        else
            entry.sendAck(true);

        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && oob) {
            for(Tuple<Long,Message> tuple: msgs) {
                long    seq=tuple.getVal1();
                Message msg=tuple.getVal2();
                if(msg.isFlagSet(Message.Flag.OOB) && msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED)) {
                    if(log.isTraceEnabled())
                        log.trace("%s: delivering %s#%s", local_addr, sender, seq);
                    try {
                        up_prot.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "OOB message", msg, t);
                    }
                }
            }
        }

        final AtomicBoolean processing=win.getProcessing();
        if(processing.compareAndSet(false, true))
            removeAndDeliver(processing, win, sender);
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
                    if(msg_to_deliver.isFlagSet(Message.Flag.OOB) && !msg_to_deliver.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
                        batch.remove(msg_to_deliver);
                }
                if(batch.isEmpty())
                    continue;

                try {
                    if(log.isTraceEnabled()) {
                        Message first=batch.first(), last=batch.last();
                        StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                        if(first != null && last != null) {
                            Header hdr1=(Header)first.getHeader(id), hdr2=(Header)last.getHeader(id);
                            sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                        }
                        sb.append(" (" + batch.size()).append(" messages)");
                        log.trace(sb);
                    }
                    up_prot.up(batch);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "batch", batch, t);
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


    protected String printMessageList(List<Tuple<Long,Message>> list) {
        StringBuilder sb=new StringBuilder();
        int size=list.size();
        Message first=size > 0? list.get(0).getVal2() : null, second=size > 1? list.get(size-1).getVal2() : first;
        Header hdr;
        if(first != null) {
            hdr=(Header)first.getHeader(id);
            if(hdr != null)
                sb.append("#" + hdr.seqno);
        }
        if(second != null) {
            hdr=(Header)second.getHeader(id);
            if(hdr != null)
                sb.append(" - #" + hdr.seqno);
        }
        return sb.toString();
    }

    protected ReceiverEntry getReceiverEntry(Address sender, long seqno, boolean first, short conn_id) {
        ReceiverEntry entry=recv_table.get(sender);
        if(entry != null && entry.connId() == conn_id)
            return entry;

        recv_table_lock.lock();
        try {
            entry=recv_table.get(sender);
            if(first) {
                if(entry == null) {
                    entry=createReceiverEntry(sender,seqno,conn_id);
                }
                else {  // entry != null && win != null
                    if(conn_id != entry.connId()) {
                        log.trace("%s: conn_id=%d != %d; resetting receiver window", local_addr, conn_id, entry.connId());
                        recv_table.remove(sender);
                        entry=createReceiverEntry(sender,seqno,conn_id);
                    }
                }
            }
            else { // entry == null && win == null OR entry != null && win == null OR entry != null && win != null
                if(entry == null || entry.connId() != conn_id) {
                    recv_table_lock.unlock();
                    sendRequestForFirstSeqno(sender); // drops the message and returns (see below)
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


    protected ReceiverEntry createReceiverEntry(Address sender, long seqno, short conn_id) {
        Table<Message> table=new Table<Message>(xmit_table_num_rows, xmit_table_msgs_per_row, seqno-1,
                                                xmit_table_resize_factor, xmit_table_max_compaction_time);
        ReceiverEntry entry=new ReceiverEntry(table, conn_id);
        ReceiverEntry entry2=recv_table.putIfAbsent(sender, entry);
        if(entry2 != null)
            return entry2;
        log.trace("%s: created receiver window for %s at seqno=#%d for conn-id=%d", local_addr, sender, seqno, conn_id);
        return entry;
    }

    /** Add the ACK to hashtable.sender.sent_msgs */
    protected void handleAckReceived(Address sender, long seqno, short conn_id) {
        if(log.isTraceEnabled())
            log.trace("%s <-- ACK(%s: #%d, conn-id=%d)", local_addr, sender, seqno, conn_id);
        SenderEntry entry=send_table.get(sender);

        if(entry != null && entry.connId() != conn_id) {
            log.trace("%s: my conn_id (%d) != received conn_id (%d); discarding ACK", local_addr, entry.connId(), conn_id);
            return;
        }

        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            win.purge(seqno, true); // removes all messages <= seqno (forced purge)
            num_acks_received++;
        }
    }



    /**
     * We need to resend the first message with our conn_id
     * @param sender
     */
    protected void handleResendingOfFirstMessage(Address sender) {
        log.trace("%s <-- SEND_FIRST_SEQNO(%s)", local_addr, sender);
        SenderEntry entry=send_table.get(sender);
        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win == null) {
            log.warn(Util.getMessage("SenderNotFound"), local_addr, sender);
            return;
        }

        Message rsp=win.get(win.getLow() +1);
        if(rsp != null) {
            // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
            // the headers and therefore we'd modify the original message in the sender retransmission window
            // (https://jira.jboss.org/jira/browse/JGRP-965)
            Message copy=rsp.copy();
            Header hdr=(Header)copy.getHeader(this.id);
            Header newhdr=hdr.copy();
            newhdr.first=true;
            copy.putHeader(this.id, newhdr);
            down_prot.down(new Event(Event.MSG, copy));
        }
    }


    protected void handleXmitRequest(Address sender, SeqnoList missing) {
        log.trace("%s <-- XMIT(%s: #%s)", local_addr, sender, missing);

        SenderEntry entry=send_table.get(sender);
        xmit_reqs_received.addAndGet(missing.size());
        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            for(long seqno: missing) {
                Message msg=win.get(seqno);
                if(msg == null) {
                    if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(sender) && seqno > win.getLow())
                        log.warn(Util.getMessage("MessageNotFound"), local_addr, sender, seqno);
                    continue;
                }

                down_prot.down(new Event(Event.MSG, msg));
                xmit_rsps_sent.incrementAndGet();
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
        Message ack=new Message(dst).setFlag(Message.Flag.INTERNAL).putHeader(this.id, Header.createAckHeader(seqno, conn_id));
        if(log.isTraceEnabled())
            log.trace("%s --> ACK(%s: #%d)", local_addr, dst, seqno);
        try {
            down_prot.down(new Event(Event.MSG, ack));
            num_acks_sent++;
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedSendingAck"), local_addr, seqno, dst, t);
        }
    }


    protected synchronized short getNewConnectionId() {
        short retval=last_conn_id;
        if(last_conn_id >= Short.MAX_VALUE || last_conn_id < 0)
            last_conn_id=0;
        else
            last_conn_id++;
        return retval;
    }


    protected void sendRequestForFirstSeqno(Address dest) {
        Message msg=new Message(dest).setFlag(Message.Flag.OOB).putHeader(this.id, Header.createSendFirstSeqnoHeader());
        log.trace("%s --> SEND_FIRST_SEQNO(%s)", local_addr, dest);
        down_prot.down(new Event(Event.MSG, msg));
    }

    public void sendClose(Address dest, short conn_id) {
        Message msg=new Message(dest).setFlag(Message.Flag.INTERNAL).putHeader(id, Header.createCloseHeader(conn_id));
        log.trace("%s --> CLOSE(%s, conn-id=%d)", local_addr, dest, conn_id);
        down_prot.down(new Event(Event.MSG, msg));
    }

    @ManagedOperation(description="Closes connections that have been idle for more than conn_expiry_timeout ms")
    public void closeIdleConnections() {
        // close expired connections in send_table
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            SenderEntry val=entry.getValue();
            if(val.state() != State.OPEN) // only look at open connections
                continue;
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                log.debug("%s: closing expired connection for %s (%d ms old) in send_table",
                          local_addr, entry.getKey(), age);
                closeSendConnection(entry.getKey());
            }
        }

        // close expired connections in recv_table
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            ReceiverEntry val=entry.getValue();
            if(val.state() != State.OPEN) // only look at open connections
                continue;
            long age=val.age();
            if(age >= conn_expiry_timeout) {
                log.debug("%s: closing expired connection for %s (%d ms old) in recv_table",
                          local_addr, entry.getKey(), age);
                closeReceiveConnection(entry.getKey());
            }
        }
    }


    @ManagedOperation(description="Removes connections that have been closed for more than conn_close_timeout ms")
    public void removeExpiredConnections() {
        // remove expired connections from send_table
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            SenderEntry val=entry.getValue();
            if(val.state() == State.OPEN) // only look at closing or closed connections
                continue;
            long age=val.age();
            if(age >= conn_close_timeout) {
                log.debug("%s: removing expired connection for %s (%d ms old) from send_table",
                          local_addr, entry.getKey(), age);
                removeSendConnection(entry.getKey());
            }
        }

        // remove expired connections from recv_table
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            ReceiverEntry val=entry.getValue();
            if(val.state() == State.OPEN) // only look at closing or closed connections
                continue;
            long age=val.age();
            if(age >= conn_close_timeout) {
                log.debug("%s: removing expired connection for %s (%d ms old) from recv_table",
                          local_addr, entry.getKey(), age);
                removeReceiveConnection(entry.getKey());
            }
        }
    }


    protected static enum State {OPEN, CLOSING, CLOSED}


    /**
     * The following types and fields are serialized:
     * <pre>
     * | DATA | seqno | conn_id | first |
     * | ACK  | seqno |
     * | SEND_FIRST_SEQNO |
     * | CLOSE | conn_id |
     * </pre>
     */
    public static class Header extends org.jgroups.Header {
        public static final byte DATA             = 0;
        public static final byte ACK              = 1;
        public static final byte SEND_FIRST_SEQNO = 2;
        public static final byte XMIT_REQ         = 3; // SeqnoList of missing message is in the message's payload
        public static final byte CLOSE            = 4;

        byte    type;
        long    seqno;    // DATA and ACK
        short   conn_id;  // DATA and CLOSE
        boolean first;    // DATA


        public Header() {} // used for externalization

        protected Header(byte type) {
            this.type=type;
        }

        protected Header(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        protected Header(byte type, long seqno, short conn_id, boolean first) {
            this.type=type;
            this.seqno=seqno;
            this.conn_id=conn_id;
            this.first=first;
        }

        public static Header createDataHeader(long seqno, short conn_id, boolean first) {
            return new Header(DATA, seqno, conn_id, first);
        }

        public static Header createAckHeader(long seqno, short conn_id) {
            return new Header(ACK, seqno, conn_id, false);
        }

        public static Header createSendFirstSeqnoHeader() {
            return new Header(SEND_FIRST_SEQNO);
        }

        public static Header createXmitReqHeader() {
            return new Header(XMIT_REQ);
        }

        public static Header createCloseHeader(short conn_id) {
            return new Header(CLOSE, 0, conn_id, false);
        }

        public long seqno()   {return seqno;}
        public short connId() {return conn_id;}

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
                case ACK:              return "ACK";
                case SEND_FIRST_SEQNO: return "SEND_FIRST_SEQNO";
                case XMIT_REQ:         return "XMIT_REQ";
                case CLOSE:            return "CLOSE";
                default:               return "<unknown>";
            }
        }

        public final int size() {
            int retval=Global.BYTE_SIZE;     // type
            switch(type) {
                case DATA:
                    retval+=Util.size(seqno) // seqno
                      + Global.SHORT_SIZE    // conn_id
                      + Global.BYTE_SIZE;    // first
                    break;
                case ACK:
                    retval+=Util.size(seqno) + Global.SHORT_SIZE; // conn_id
                    break;
                case SEND_FIRST_SEQNO:
                case XMIT_REQ:
                    break;
                case CLOSE:
                    retval+=Global.SHORT_SIZE; // conn-id
                    break;
            }
            return retval;
        }

        public Header copy() {
            return new Header(type, seqno, conn_id, first);
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    Util.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case ACK:
                    Util.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    break;
                case SEND_FIRST_SEQNO:
                case XMIT_REQ:
                    break;
                case CLOSE:
                    out.writeShort(conn_id);
                    break;
            }
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            switch(type) {
                case DATA:
                    seqno=Util.readLong(in);
                    conn_id=in.readShort();
                    first=in.readBoolean();
                    break;
                case ACK:
                    seqno=Util.readLong(in);
                    conn_id=in.readShort();
                    break;
                case SEND_FIRST_SEQNO:
                case XMIT_REQ:
                    break;
                case CLOSE:
                    conn_id=in.readShort();
                    break;
            }
        }
    }

    protected abstract class Entry {
        final short                 conn_id;
        protected final AtomicLong  timestamp=new AtomicLong(0);
        protected volatile State    state=State.OPEN;

        protected Entry(short conn_id) {
            this.conn_id=conn_id;
            update();
        }

        short       connId()              {return conn_id;}
        void        update()              {timestamp.set(System.currentTimeMillis());}
        long        age()                 {return System.currentTimeMillis() - timestamp.longValue();}
        State       state()               {return state;}
        Entry       state(State state)    {this.state=state; return this;}
    }

    protected final class SenderEntry extends Entry {
        final Table<Message>        sent_msgs; // stores (and retransmits) msgs sent by us to a certain peer
        final AtomicLong            sent_msgs_seqno=new AtomicLong(DEFAULT_FIRST_SEQNO);   // seqno for msgs sent by us
        protected final long[]      watermark={0,0};   // the highest acked and highest sent seqno

        public SenderEntry(short send_conn_id) {
            super(send_conn_id);
            this.sent_msgs=new Table<Message>(xmit_table_num_rows, xmit_table_msgs_per_row, 0,
                                              xmit_table_resize_factor, xmit_table_max_compaction_time);
        }

        long[]      watermark()                 {return watermark;}
        SenderEntry watermark(long ha, long hs) {watermark[0]=ha; watermark[1]=hs; return this;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append(sent_msgs).append(", ");
            sb.append("send_conn_id=" + conn_id).append(" (" + age() + " ms old) - " + state);
            return sb.toString();
        }
    }

    protected final class ReceiverEntry extends Entry {
        protected final Table<Message>  received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        protected volatile boolean      send_ack;

        
        public ReceiverEntry(Table<Message> received_msgs, short recv_conn_id) {
            super(recv_conn_id);
            this.received_msgs=received_msgs;
        }

        ReceiverEntry  sendAck(boolean flag) {send_ack=flag; return this;}
        boolean        sendAck()             {boolean retval=send_ack; send_ack=false; return retval;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(received_msgs != null)
                sb.append(received_msgs).append(", ");
            sb.append("recv_conn_id=" + conn_id);
            sb.append(" (" + age() + " ms old) - " + state);
            if(send_ack)
                sb.append(" [ack pending]");
            return sb.toString();
        }
    }



    /**
     * Retransmitter task which periodically (every xmit_interval ms):
     * <ul>
     *     <li>If any of the receiver windows have the ack flag set, clears the flag and sends an ack for the
     *         highest delivered seqno to the sender</li>
     *     <li>Checks all receiver windows for missing messages and asks senders for retransmission</li>
     *     <li>For all sender windows, checks if highest acked (HA) < highest sent (HS). If not, and HA/HS is the same
     *         as on the last retransmission run, send the highest sent message again</li>
     * </ul>
     */
    protected class RetransmitTask implements Runnable {

        public void run() {
            triggerXmit();
        }

        public String toString() {
            return UNICAST3.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

    @ManagedOperation(description="Triggers the retransmission task")
    public void triggerXmit() {
        SeqnoList missing;

        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address        target=entry.getKey(); // target to send retransmit requests to
            ReceiverEntry  val=entry.getValue();
            Table<Message> win=val != null? val.received_msgs : null;

            // receiver: send ack for received messages if needed
            if(win != null && val.sendAck()) // sendAck() resets send_ack to false
                sendAck(target, win.getHighestDeliverable(), val.connId());

            // receiver: retransmit missing messages
            if(win != null && win.getNumMissing() > 0 && (missing=win.getMissing()) != null) { // getNumMissing() is fast
                long highest=missing.getLast();
                Long prev_seqno=xmit_task_map.get(target);
                if(prev_seqno == null)
                    xmit_task_map.put(target, highest); // no retransmission
                else {
                    missing.removeHigherThan(prev_seqno); // we only retransmit the 'previous batch'
                    if(highest > prev_seqno)
                        xmit_task_map.put(target, highest);
                    if(missing.size() > 0)
                        retransmit(missing, target);
                }
            }
            else if(!xmit_task_map.isEmpty())
                xmit_task_map.remove(target); // no current gaps for target
        }

        // sender: only send the *highest sent* message if HA < HS and HA/HS didn't change from the prev run
        for(SenderEntry val: send_table.values()) {
            Table<Message> win=val != null? val.sent_msgs : null;
            if(win != null /** && !win.isEmpty() */) {
                long highest_acked=win.getHighestDelivered(); // highest delivered == highest ack (sender win)
                long highest_sent=win.getHighestReceived();   // we use table as a *sender* win, so it's highest *sent*...

                if(highest_acked < highest_sent && val.watermark[0] == highest_acked && val.watermark[1] == highest_sent) {
                    // highest acked and sent hasn't moved up - let's resend the HS
                    Message highest_sent_msg=win.get(highest_sent);
                    if(highest_sent_msg != null)
                        retransmit(highest_sent_msg);
                }
                else
                    val.watermark(highest_acked, highest_sent);
            }
        }


        // close idle connections
        if(conn_expiry_timeout > 0)
            closeIdleConnections();

        if(conn_close_timeout > 0)
            removeExpiredConnections();
    }

    /** Sends ACKs immediately for entries which are marked as pending (ACK hasn't been sent yet) */
    protected void sendPendingAcks() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address        target=entry.getKey(); // target to send retransmit requests to
            ReceiverEntry  val=entry.getValue();
            Table<Message> win=val != null? val.received_msgs : null;

            // receiver: send ack for received messages if needed
            if(win != null && val.sendAck()) // sendAck() resets send_ack to false
                sendAck(target, win.getHighestDeliverable(), val.connId());
        }
    }


}
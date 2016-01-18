package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AgeOutCache;
import org.jgroups.util.Bits;
import org.jgroups.util.Filter;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Table;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * @deprecated Will be removed in 4.0
 */
@Deprecated
@MBean(description="Reliable unicast layer")
public class UNICAST2 extends Protocol implements AgeOutCache.Handler<Address> {
    public static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;


    /* ------------------------------------------ Properties  ------------------------------------------ */
    @Deprecated
    protected int[]   timeout= {400, 800, 1600, 3200}; // for NakSenderWindow: max time to wait for missing acks

    /**
     * The first value (in milliseconds) to use in the exponential backoff
     * retransmission mechanism. Only enabled if the value is > 0
     */
    @Deprecated
    @Property(description="The first value (in milliseconds) to use in the exponential backoff. Enabled if greater than 0",
              deprecatedMessage="Not used anymore")
    protected int     exponential_backoff=300;


    @Property(description="Max number of messages to be removed from a NakReceiverWindow. This property might " +
            "get removed anytime, so don't use it !")
    protected int     max_msg_batch_size=500;

    @Property(description="Max number of bytes before a stability message is sent to the sender")
    protected long    max_bytes=10000000;

    @Property(description="Max number of milliseconds before a stability message is sent to the sender(s)")
    protected long    stable_interval=60000L;

    @Property(description="Max number of STABLE messages sent for the same highest_received seqno. A value < 1 is invalid")
    protected int     max_stable_msgs=5;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int     xmit_table_num_rows=100;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    protected int     xmit_table_msgs_per_row=2000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double  xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    protected long    xmit_table_max_compaction_time=10 * 60 * 1000;

    @Deprecated
    @Property(description="If enabled, the removal of a message from the retransmission table causes an " +
      "automatic purge (only for experts)",writable=false, deprecatedMessage="not used anymore")
    protected boolean xmit_table_automatic_purging=true;

    @Property(description="Whether to use the old retransmitter which retransmits individual messages or the new one " +
      "which uses ranges of retransmitted messages. Default is true. Note that this property will be removed in 3.0; " +
      "it is only used to switch back to the old (and proven) retransmitter mechanism if issues occur")
    protected boolean use_range_based_retransmitter=true;

    @Property(description="If true, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Time (in milliseconds) after which an idle incoming or outgoing connection is closed. The " +
      "connection will get re-established when used again. 0 disables connection reaping")
    protected long    conn_expiry_timeout=0;

    // @Property(description="Max time (in ms) after which a connection to a non-member is closed")
    protected long                   max_retransmit_time=60 * 1000L;

    @Property(description="Interval (in milliseconds) at which missing messages (from all retransmit buffers) " +
      "are retransmitted")
    protected long    xmit_interval=1000;
    /* --------------------------------------------- JMX  ---------------------------------------------- */


    @ManagedAttribute(description="Number of messages sent")
    protected int num_messages_sent=0;

    @ManagedAttribute(description="Number of messages received")
    protected int num_messages_received=0;


    /* --------------------------------------------- Fields ------------------------------------------------ */

    protected final ConcurrentMap<Address,SenderEntry>   send_table=Util.createConcurrentMap();
    protected final ConcurrentMap<Address,ReceiverEntry> recv_table=Util.createConcurrentMap();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                 xmit_task;
    /** Used by the retransmit task to keep the last retransmitted seqno per sender (https://issues.jboss.org/browse/JGRP-1539) */
    protected final Map<Address,Long>   xmit_task_map=new HashMap<>();

    protected final ReentrantLock       recv_table_lock=new ReentrantLock();

    protected volatile List<Address>    members=new ArrayList<>(11);

    protected Address                   local_addr=null;

    protected TimeScheduler             timer=null; // used for retransmissions (passed to AckSenderWindow)

    protected volatile boolean          running=false;

    protected short                     last_conn_id=0;

    protected AgeOutCache<Address>      cache=null;

    protected Future<?>                 stable_task_future=null; // bcasts periodic STABLE message (added to timer below)

    protected Future<?>                 connection_reaper; // closes idle connections

    protected static final Filter<Message> dont_loopback_filter=new Filter<Message>() {
        public boolean accept(Message msg) {
            return msg != null && msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK);
        }
    };

    @Deprecated
    public int[] getTimeout() {return timeout;}

    @Deprecated
    @Property(name="timeout",converter=PropertyConverters.IntegerArray.class,
              description="list of timeouts", deprecatedMessage="not used anymore")
    public void setTimeout(int[] val) {}

    public void setMaxMessageBatchSize(int size) {
        if(size >= 1)
            max_msg_batch_size=size;
    }

    @ManagedAttribute
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}

    @ManagedAttribute
    public String getMembers() {return Util.printListWithDelimiter(members, ",");}


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

    /** Is the send connection to target established  */
    public boolean connectionEstablished(Address target) {
        SenderEntry entry=target != null? send_table.get(target) : null;
        return entry != null && entry.connEstablished();
    }

    @ManagedAttribute(description="Whether the ConnectionReaper task is running")
    public boolean isConnectionReaperRunning() {return connection_reaper != null && !connection_reaper.isDone();}


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



    @ManagedAttribute(description="Number of retransmit requests received")
    protected final AtomicLong xmit_reqs_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit requests sent")
    protected final AtomicLong xmit_reqs_sent=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses sent")
    protected final AtomicLong xmit_rsps_sent=new AtomicLong(0);

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

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

    /** Used for testing only */
    public boolean hasSendConnectionTo(Address dest) {
        return send_table.containsKey(dest);
    }

    public void resetStats() {
        num_messages_sent=num_messages_received=0;
        xmit_reqs_received.set(0);
        xmit_reqs_sent.set(0);
        xmit_rsps_sent.set(0);
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
        if(max_bytes <= 0)
            throw new IllegalArgumentException("max_bytes has to be > 0");
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<>(timer, max_retransmit_time, this);
        running=true;
        if(stable_interval > 0)
            startStableTask();
        if(conn_expiry_timeout > 0)
            startConnectionReaper();
        startRetransmitTask();
    }

    public void stop() {
        running=false;
        stopStableTask();
        stopConnectionReaper();
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
                Unicast2Header hdr=(Unicast2Header)msg.getHeader(this.id);
                if(hdr == null)
                    break;

                Address sender=msg.getSrc();
                switch(hdr.type) {
                    case Unicast2Header.DATA:      // received regular message
                        if(handleDataReceived(sender, hdr.seqno, hdr.conn_id, hdr.first, msg, evt) && hdr.first)
                            sendAck(sender, hdr.seqno, hdr.conn_id);
                        return null; // we pass the deliverable message up in handleDataReceived()
                    case Unicast2Header.XMIT_REQ:  // received ACK for previously sent message
                        handleXmitRequest(sender, (SeqnoList)msg.getObject());
                        break;
                    case Unicast2Header.SEND_FIRST_SEQNO:
                        handleResendingOfFirstMessage(sender, hdr.seqno);
                        break;
                    case Unicast2Header.ACK:
                        if(log.isTraceEnabled())
                            log.trace(local_addr + " <-- ACK(" + sender + "," + hdr.seqno + " [conn_id=" + hdr.conn_id + "])");
                        SenderEntry entry=send_table.get(msg.getSrc());
                        if(entry != null) {
                            if(entry.send_conn_id != hdr.conn_id) {
                                if(log.isTraceEnabled())
                                    log.trace(local_addr + ": ACK from " + sender + " is discarded as the connection IDs don't match: " +
                                                "my conn-id=" + entry.send_conn_id + ", hdr.conn_id=" + hdr.conn_id);
                            }
                            else
                                entry.connEstablished(true);
                        }
                        break;
                    case Unicast2Header.STABLE:
                        stable(msg.getSrc(), hdr.conn_id, hdr.seqno, hdr.high_seqno);
                        break;
                    default:
                        log.error(Util.getMessage("UnicastHeaderType"), hdr.type);
                        break;
                }
                return null;
        }

        return up_prot.up(evt);   // Pass up to the layer above us
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
            Unicast2Header hdr=(Unicast2Header)msg.getHeader(id);
            if(hdr == null)
                continue;
            batch.remove(msg); // remove the message from the batch, so it won't be passed up the stack

            if(hdr.type != Unicast2Header.DATA) {
                up(new Event(Event.MSG, msg));
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
                        log.trace(local_addr + ": discarded message as start() has not yet been called, message: " + msg);
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
                            log.trace(local_addr + ": created connection to " + dst + " (conn_id=" + entry.send_conn_id + ")");
                        if(cache != null && !members.contains(dst))
                            cache.add(dst);
                    }
                }

                boolean dont_loopback_set=msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK)
                  && dst.equals(local_addr);
                short send_conn_id=entry.send_conn_id;
                long seqno=entry.sent_msgs_seqno.getAndIncrement();
                long sleep=10;
                do {
                    try {
                        msg.putHeader(this.id, Unicast2Header.createDataHeader(seqno, send_conn_id, seqno == DEFAULT_FIRST_SEQNO));
                        entry.sent_msgs.add(seqno,msg, dont_loopback_set? dont_loopback_filter : null);  // add *including* UnicastHeader, adds to retransmitter
                        if(conn_expiry_timeout > 0)
                            entry.update();
                        if(dont_loopback_set)
                            entry.sent_msgs.purge(entry.sent_msgs.getHighestDeliverable());
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

                num_messages_sent++;
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
                        log.trace(local_addr + ": removing non members " + non_members);
                    for(Address non_mbr: non_members)
                        removeConnection(non_mbr);
                }
                xmit_task_map.keySet().retainAll(view.getMembers());
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
     * @param hd Highest delivered seqno
     * @param hr Highest received seqno
     */
    protected void stable(Address sender, short conn_id, long hd, long hr) {
        SenderEntry entry=send_table.get(sender);
        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win == null)
            return;

        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- STABLE(").append(sender)
                        .append(": ").append(hd).append("-")
                        .append(hr).append(", conn_id=" + conn_id) +")");

        if(entry.send_conn_id != conn_id) {
            log.trace(local_addr + ": my conn_id (" + entry.send_conn_id +
                       ") != received conn_id (" + conn_id + "); discarding STABLE message !");
            return;
        }

        win.purge(hd,true);
        long win_hr=win.getHighestReceived();
        if(win_hr > hr) {
            for(long seqno=hr; seqno <= win_hr; seqno++) {
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
            Table<Message> win=val != null? val.received_msgs : null;
            if(win != null) {
                long[] tmp=win.getDigest();
                long low=tmp[0], high=tmp[1];

                if(val.last_highest == high) {
                    if(val.num_stable_msgs >= max_stable_msgs) {
                        continue;
                    }
                    else
                        val.num_stable_msgs++;
                }
                else {
                    val.last_highest=high;
                    val.num_stable_msgs=1;
                }
                sendStableMessage(dest, val.recv_conn_id, low, high);
            }
        }
    }

    protected void sendStableMessage(Address dest, short conn_id, long hd, long hr) {
        Message stable_msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(this.id, Unicast2Header.createStableHeader(conn_id, hd, hr));

        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> STABLE(").append(dest).append(": ")
              .append(hd).append("-").append(hr).append(", conn_id=").append(conn_id).append(")");
            log.trace(sb.toString());
        }
        down_prot.down(new Event(Event.MSG, stable_msg));
    }


    protected void startStableTask() {
        if(stable_task_future == null || stable_task_future.isDone()) {
            final Runnable stable_task=new Runnable() {
                public void run() {
                    try {
                        sendStableMessages();
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("SendingOfSTABLEMessagesFailed"), t);
                    }
                }

                public String toString() {
                    return UNICAST2.class.getSimpleName() + ": StableTask (interval=" + stable_interval + " ms)";
                }
            };
            stable_task_future=timer.scheduleWithFixedDelay(stable_task, stable_interval, stable_interval, TimeUnit.MILLISECONDS);
            if(log.isTraceEnabled())
                log.trace(local_addr + ": stable task started (interval=" + stable_interval + ")");
        }
    }


    protected void stopStableTask() {
        if(stable_task_future != null) {
            stable_task_future.cancel(false);
            stable_task_future=null;
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

    /**
     * Removes and resets from connection table (which is already locked). Returns true if member was found, otherwise
     * false. This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    public void removeConnection(Address mbr) {
        removeSendConnection(mbr);
        removeReceiveConnection(mbr);
    }

    public void removeSendConnection(Address mbr) {
        send_table.remove(mbr);
    }

    public void removeReceiveConnection(Address mbr) {
        ReceiverEntry entry2=recv_table.remove(mbr);
        if(entry2 != null) {
            Table<Message> win=entry2.received_msgs;
            if(win != null) {
                long[] digest=win.getDigest();
                sendStableMessage(mbr, entry2.recv_conn_id, digest[0], digest[1]);
            }
            entry2.reset();
        }
    }


    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        send_table.clear();
        sendStableMessages();
        for(ReceiverEntry entry2: recv_table.values())
            entry2.reset();
        recv_table.clear();
    }


    public void retransmit(SeqnoList missing, Address sender) {
        Unicast2Header hdr=Unicast2Header.createXmitReqHeader();
        Message retransmit_msg=new Message(sender, missing).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending XMIT_REQ (" + missing + ") to " + sender);
        retransmit_msg.putHeader(this.id, hdr);
        down_prot.down(new Event(Event.MSG,retransmit_msg));
        xmit_reqs_sent.addAndGet(missing.size());
    }

    
    /**
     * Called by AgeOutCache, to removed expired connections
     * @param key
     */
    public void expired(Address key) {
        if(key != null) {
            if(log.isDebugEnabled())
                log.debug(local_addr + ": removing connection to " + key + " because it expired");
            removeConnection(key);
        }
    }



    /**
     * Check whether the hashmap contains an entry e for <code>sender</code> (create if not). If
     * e.received_msgs is null and <code>first</code> is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    protected boolean handleDataReceived(Address sender, long seqno, short conn_id, boolean first, Message msg, Event evt) {
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
            return false;
        if(conn_expiry_timeout > 0)
            entry.update();
        Table<Message> win=entry.received_msgs;
        boolean added=win.add(seqno, msg); // win is guaranteed to be non-null if we get here
        num_messages_received++;

        if(added) {
            int len=msg.getLength();
            if(len > 0 &&  entry.incrementStable(len)) {
                long[] digest=win.getDigest();
                sendStableMessage(sender, entry.recv_conn_id, digest[0], digest[1]);
            }
        }

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // http://jira.jboss.com/jira/browse/JGRP-377
        if(msg.isFlagSet(Message.Flag.OOB) && added) {
            try {
                up_prot.up(evt);
            }
            catch(Throwable t) {
                log.error("couldn't deliver OOB message %s: %s", msg, t);
            }
        }

        removeAndPassUp(win, sender);
        return true;
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
            Table<Message> win=null;
            boolean        added=false; // set to true when at least 1 message was added
            int            total_len=0;
            for(Message msg: msg_list) {
                Unicast2Header hdr=(Unicast2Header)msg.getHeader(id);
                entry=getReceiverEntry(sender, hdr.seqno, hdr.first, conn_id);
                if(entry == null)
                    continue;
                win=entry.received_msgs;
                boolean msg_added=win.add(hdr.seqno, msg); // win is guaranteed to be non-null if we get here
                added|=msg_added;
                num_messages_received++;
                total_len+=msg.getLength();

                if(hdr.first && msg_added)
                    sendAck(sender, hdr.seqno, hdr.conn_id);

                // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
                // http://jira.jboss.com/jira/browse/JGRP-377
                if(msg.isFlagSet(Message.Flag.OOB) && msg_added) {
                    try {
                        up_prot.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error("couldn't deliver OOB message %s: %s", msg, t);
                    }
                }
            }
            if(entry != null && conn_expiry_timeout > 0)
                entry.update();
            if(added && total_len > 0 && entry != null && entry.incrementStable(total_len) && win != null) {
                long[] digest=win.getDigest();
                sendStableMessage(sender, entry.recv_conn_id, digest[0], digest[1]);
            }
        }

        ReceiverEntry tmp=recv_table.get(sender);
        Table<Message> win=tmp != null? tmp.received_msgs : null;
        if(win != null)
            removeAndPassUp(win, sender);
    }


    protected String printMessageList(List<Message> list) {
        StringBuilder sb=new StringBuilder();
        int size=list.size();
        Message first=size > 0? list.get(0) : null, second=size > 1? list.get(size-1) : null;
        Unicast2Header hdr;
        if(first != null) {
            hdr=(Unicast2Header)first.getHeader(id);
            if(hdr != null)
                sb.append("#" + hdr.seqno);
        }
        if(second != null) {
            hdr=(Unicast2Header)second.getHeader(id);
            if(hdr != null)
                sb.append(" - #" + hdr.seqno);
        }
        return sb.toString();
    }


    /**
     * Try to remove as many messages as possible and pass them up. Prevents concurrent passing up of messages by
     * different threads (http://jira.jboss.com/jira/browse/JGRP-198); this is all the more important once we have a
     * concurrent stack (http://jira.jboss.com/jira/browse/JGRP-181), where lots of threads can come up to this point
     * concurrently, but only 1 is allowed to pass at a time.<p/>
     * We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
     * delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered only in the
     * order in which they were sent by their senders
     */
    protected void removeAndPassUp(Table<Message> win, Address sender) {
        final AtomicBoolean processing=win.getProcessing();
        if(!processing.compareAndSet(false, true))
            return;

        boolean released_processing=false;
        try {
            while(true) {
                List<Message> msgs=win.removeMany(processing, true, max_msg_batch_size); // remove my own messages
                if(msgs == null || msgs.isEmpty()) {
                    released_processing=true;
                    return;
                }

                MessageBatch batch=new MessageBatch(local_addr, sender, null, false, msgs);
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
                            Unicast2Header hdr1=(Unicast2Header)first.getHeader(id), hdr2=(Unicast2Header)last.getHeader(id);
                            sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                        }
                        sb.append(" (" + batch.size()).append(" messages)");
                        log.trace(sb);
                    }
                    up_prot.up(batch);
                }
                catch(Throwable t) {
                    log.error(Util.getMessage("FailedToDeliverBatch"), batch, t);
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


    protected void handleXmitRequest(Address sender, SeqnoList missing) {
        if(log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(" <-- XMIT(").append(sender).
              append(": #").append(missing).append(')'));

        SenderEntry entry=send_table.get(sender);
        xmit_reqs_received.addAndGet(missing.size());
        Table<Message> win=entry != null? entry.sent_msgs : null;
        if(win != null) {
            for(long seqno: missing) {
                Message msg=win.get(seqno);
                if(msg == null) {
                    if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(sender) && seqno > win.getLow()) {
                        StringBuilder sb=new StringBuilder();
                        sb.append(local_addr +  ": (requester=").append(sender).append(") message ").append(sender)
                          .append("::").append(seqno).append(" not found in retransmission table of ").append(sender)
                          .append(":\n").append(win);
                        log.warn(sb.toString());
                    }
                    continue;
                }
                
                down_prot.down(new Event(Event.MSG, msg));
                xmit_rsps_sent.incrementAndGet();
            }
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
            if(log.isTraceEnabled())
                log.trace(local_addr + ": sender window for " + sender + " not found");
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
                Unicast2Header hdr=(Unicast2Header)copy.getHeader(this.id);
                Unicast2Header newhdr=hdr.copy();
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
        Unicast2Header hdr=Unicast2Header.createSendFirstSeqnoHeader(seqno_received);
        msg.putHeader(this.id, hdr);
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> SEND_FIRST_SEQNO(" + dest + "," + seqno_received + ")");
        down_prot.down(new Event(Event.MSG, msg));
    }

    protected void sendAck(Address dest, long seqno, short conn_id) {
        Message msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(this.id, Unicast2Header.createAckHeader(seqno, conn_id));
        if(log.isTraceEnabled())
            log.trace(local_addr + " --> ACK(" + dest + "," + seqno + " [conn_id=" + conn_id + "])");
        down_prot.down(new Event(Event.MSG, msg));
    }

    @ManagedOperation(description="Closes connections that have been idle for more than conn_expiry_timeout ms")
    public void reapIdleConnections() {
        if(conn_expiry_timeout <= 0)
            return;

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


    /**
     * The following types and fields are serialized:
     * <pre>
     * | DATA | seqno | conn_id | first |
     * | ACK  | seqno |
     * | SEND_FIRST_SEQNO | seqno |
     * </pre>
     */
    public static class Unicast2Header extends Header {
        public static final byte DATA             = 0;
        public static final byte XMIT_REQ         = 1;
        public static final byte SEND_FIRST_SEQNO = 2;
        public static final byte STABLE           = 3;
        public static final byte ACK              = 4;

        byte    type;
        long    seqno;       // DATA, STABLE, SEND_FIRST_SEQNO and ACK
        long    high_seqno;  // STABLE
        short   conn_id;     // DATA, STABLE, ACK
        boolean first;       // DATA


        public Unicast2Header() {} // used for externalization

        public static Unicast2Header createDataHeader(long seqno, short conn_id, boolean first) {
            return new Unicast2Header(DATA, seqno, 0L, conn_id, first);
        }

        public static Unicast2Header createXmitReqHeader() {
            return new Unicast2Header(XMIT_REQ);
        }

        public static Unicast2Header createStableHeader(short conn_id, long low, long high) {
            if(low > high)
                throw new IllegalArgumentException("low (" + low + ") needs to be <= high (" + high + ")");
            Unicast2Header retval=new Unicast2Header(STABLE, low);
            retval.high_seqno=high;
            retval.conn_id=conn_id;
            return retval;
        }

        public static Unicast2Header createSendFirstSeqnoHeader(long seqno_received) {
            return new Unicast2Header(SEND_FIRST_SEQNO, seqno_received);
        }

        public static Unicast2Header createAckHeader(long acked_seqno, short conn_id) {
            Unicast2Header retval=new Unicast2Header(ACK,acked_seqno);
            retval.conn_id=conn_id;
            return retval;
        }

        protected Unicast2Header(byte type) {
            this.type=type;
        }

        protected Unicast2Header(byte type, long seqno) {
            this.type=type;
            this.seqno=seqno;
        }

        protected Unicast2Header(byte type, long seqno, long high, short conn_id, boolean first) {
            this.type=type;
            this.seqno=seqno;
            this.high_seqno=high;
            this.conn_id=conn_id;
            this.first=first;
        }

        public byte getType() {
            return type;
        }

        public long getSeqno() {
            return seqno;
        }

        public long getHighSeqno() {
            return high_seqno;
        }

        public short getConnId() {
            return conn_id;
        }

        public boolean isFirst() {
            return first;
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
                case ACK:              return "ACK";
                default:               return "<unknown>";
            }
        }

        public final int size() {
            int retval=Global.BYTE_SIZE; // type
            switch(type) {
                case DATA:
                    retval+=Bits.size(seqno) // seqno
                      + Global.SHORT_SIZE    // conn_id
                      + Global.BYTE_SIZE;    // first
                    break;
                case XMIT_REQ:
                    break;
                case STABLE:
                    retval+=Bits.size(seqno, high_seqno) + Global.SHORT_SIZE; // conn_id
                    break;
                case SEND_FIRST_SEQNO:
                    retval+=Bits.size(seqno);
                    break;
                case ACK:
                    retval+=Bits.size(seqno) + Global.SHORT_SIZE; // conn_id
                    break;
            }
            return retval;
        }

        public Unicast2Header copy() {
            return new Unicast2Header(type, seqno, high_seqno, conn_id, first);
        }



        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            switch(type) {
                case DATA:
                    Bits.writeLong(seqno, out);
                    out.writeShort(conn_id);
                    out.writeBoolean(first);
                    break;
                case XMIT_REQ:
                    break;
                case STABLE:
                    Bits.writeLongSequence(seqno, high_seqno, out);
                    out.writeShort(conn_id);
                    break;
                case SEND_FIRST_SEQNO:
                    Bits.writeLong(seqno, out);
                    break;
                case ACK:
                    Bits.writeLong(seqno, out);
                    out.writeShort(conn_id);
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
                case XMIT_REQ:
                    break;
                case STABLE:
                    long[] seqnos=Bits.readLongSequence(in);
                    seqno=seqnos[0];
                    high_seqno=seqnos[1];
                    conn_id=in.readShort();
                    break;
                case SEND_FIRST_SEQNO:
                    seqno=Bits.readLong(in);
                    break;
                case ACK:
                    seqno=Bits.readLong(in);
                    conn_id=in.readShort();
                    break;
            }
        }
    }



    protected final class SenderEntry {
        // stores (and retransmits) msgs sent by us to a given peer
        final Table<Message>       sent_msgs;
        final AtomicLong           sent_msgs_seqno=new AtomicLong(DEFAULT_FIRST_SEQNO);   // seqno for msgs sent by us
        final short                send_conn_id;
        protected final AtomicLong timestamp=new AtomicLong(0);
        protected volatile boolean ack_received; // true if ack for the first message was received

        public SenderEntry(short send_conn_id) {
            this.send_conn_id=send_conn_id;
            this.sent_msgs=new Table<>(xmit_table_num_rows, xmit_table_msgs_per_row, 0,
                                              xmit_table_resize_factor, xmit_table_max_compaction_time);
            update();
        }

        protected void        update()                      {timestamp.set(System.currentTimeMillis());}
        protected long        age()                         {return System.currentTimeMillis() - timestamp.longValue();}
        protected boolean     connEstablished()             {return ack_received;}
        protected SenderEntry connEstablished(boolean flag) {ack_received=flag; return this;}
        protected Message     getFirstMessage()             {return sent_msgs.get(sent_msgs.getLow() +1);}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(sent_msgs != null)
                sb.append(sent_msgs).append(", ");
            sb.append("send_conn_id=" + send_conn_id).append(" (" + age() + " ms old) [")
              .append((ack_received? "acked" : "not acked") + "])");
            return sb.toString();
        }
    }

    protected final class ReceiverEntry {
        protected final Table<Message>     received_msgs;  // stores all msgs rcvd by a certain peer in seqno-order
        protected final short              recv_conn_id;
        protected int                      received_bytes=0;
        protected final AtomicLong         timestamp=new AtomicLong(0);
        protected final Lock               lock=new ReentrantLock();

        protected long                     last_highest=-1;
        protected int                      num_stable_msgs=0;



        public ReceiverEntry(Table<Message> received_msgs, short recv_conn_id) {
            this.received_msgs=received_msgs;
            this.recv_conn_id=recv_conn_id;
            update();
        }

        /** Adds len bytes, if max_bytes is exceeded, the value is reset and true returned, else false */
        boolean incrementStable(int len) {
            lock.lock();
            try {
                if(received_bytes+len >= max_bytes) {
                    received_bytes=0;
                    return true;
                }
                received_bytes+=len;
                return false;
            }
            finally {
                lock.unlock();
            }
        }

        void reset() {
            received_bytes=0;
            last_highest=-1;
            num_stable_msgs=0;
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
            return UNICAST2.class.getSimpleName() + ": ConnectionReaper (interval=" + conn_expiry_timeout + " ms)";
        }
    }

    /**
     * Retransmitter task which periodically (every xmit_interval ms) looks at all the retransmit tables and
     * sends retransmit request to all members from which we have missing messages
     */
    protected class RetransmitTask implements Runnable {

        public void run() {
            triggerXmit();
        }

        public String toString() {
            return UNICAST2.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

    @ManagedOperation(description="Triggers the retransmission task, asking all senders for missing messages")
    public void triggerXmit() {
        SeqnoList missing;

        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address target=entry.getKey(); // target to send retransmit requests to
            ReceiverEntry val=entry.getValue();
            Table<Message> buf=val != null? val.received_msgs : null;

            if(buf != null && buf.getNumMissing() > 0 && (missing=buf.getMissing()) != null) { // getNumMissing() is fast
                long highest=missing.getLast();
                Long prev_seqno=xmit_task_map.get(target);
                if(prev_seqno == null) {
                    xmit_task_map.put(target, highest); // no retransmission
                }
                else {
                    missing.removeHigherThan(prev_seqno); // we only retransmit the 'previous batch'
                    if(highest > prev_seqno)
                        xmit_task_map.put(target, highest);
                    if(!missing.isEmpty())
                        retransmit(missing, target);
                }
            }
            else if(!xmit_task_map.isEmpty())
                xmit_task_map.remove(target); // no current gaps for target
        }


        // Now check all SenderEntries for !ack_received and resend the first message if true
        // (https://issues.jboss.org/browse/JGRP-1563)
        for(SenderEntry entry: send_table.values()) {
            if(!entry.connEstablished()) {
                Message msg=entry.getFirstMessage();
                if(msg == null)
                    continue;
                Unicast2Header hdr=(Unicast2Header)msg.getHeader(id);
                if(hdr.first) {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": resending first message " + hdr.seqno + " to " + msg.getDest());
                    down_prot.down(new Event(Event.MSG,msg));
                }
                else {
                    Message copy=msg.copy();
                    hdr=(Unicast2Header)copy.getHeader(id);
                    Unicast2Header newhdr=hdr.copy();
                    newhdr.first=true;
                    copy.putHeader(id, newhdr);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": resending first message " + hdr.seqno + " to " + msg.getDest());
                    down_prot.down(new Event(Event.MSG, copy));

                }
            }
        }
    }

}
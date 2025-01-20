package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.relay.RELAY;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.Message.Flag.*;
import static org.jgroups.Message.TransientFlag.*;
import static org.jgroups.conf.AttributeType.SCALAR;
import static org.jgroups.protocols.UnicastHeader.DATA;


/**
 * Base class for reliable unicast protocols
 * @author Bela Ban
 * @since  5.4
 */
@MBean(description="Reliable unicast layer")
public abstract class ReliableUnicast extends Protocol implements AgeOutCache.Handler<Address> {
    protected static final long DEFAULT_FIRST_SEQNO=Global.DEFAULT_FIRST_UNICAST_SEQNO;
    protected static final long DEFAULT_XMIT_INTERVAL=500;

    /* ------------------------------------------ Properties  ------------------------------------------ */

    @Property(description="Time (in milliseconds) after which an idle incoming or outgoing connection is closed. The " +
      "connection will get re-established when used again. 0 disables connection reaping. Note that this creates " +
      "lingering connection entries, which increases memory over time.",type=AttributeType.TIME)
    protected long    conn_expiry_timeout=(long) 60000 * 2;

    @Property(description="Time (in ms) until a connection marked to be closed will get removed. 0 disables this",
      type=AttributeType.TIME)
    protected long    conn_close_timeout=60_000 * 4; // 4 mins == TIME_WAIT timeout (= 2 * MSL)

    // @Property(description="Max time (in ms) after which a connection to a non-member is closed")
    protected long    max_retransmit_time=60 * 1000L;

    @Property(description="Interval (in milliseconds) at which messages in the send windows are resent",type=AttributeType.TIME)
    protected long    xmit_interval=DEFAULT_XMIT_INTERVAL;

    @Property(description="When true, the sender retransmits messages until ack'ed and the receiver asks for missing " +
      "messages. When false, this is not done, but ack'ing and stale connection testing is still done. " +
      "https://issues.redhat.com/browse/JGRP-2676")
    protected boolean xmits_enabled=true;

    @Property(description="If true, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Min time (in ms) to elapse for successive SEND_FIRST_SEQNO messages to be sent to the same sender",
      type=AttributeType.TIME)
    protected long    sync_min_interval=2000;

    @Property(description="Max number of messages to ask for in a retransmit request. 0 disables this and uses " +
      "the max bundle size in the transport")
    protected int     max_xmit_req_size;

    @Property(description="The max size of a message batch when delivering messages. 0 is unbounded")
    protected int     max_batch_size;

    @Property(description="Increment seqno and send a message atomically. Reduces retransmissions. " +
      "Description in doc/design/NAKACK4.txt ('misc')")
    protected boolean send_atomically;

    @Property(description="Reuses the same message batch for delivery of regular messages (only done by a single " +
      "thread anyway). Not advisable for buffers that can grow infinitely (NAKACK3)")
    protected boolean reuse_message_batches=true;

    @Property(description="If true, a unicast message to self is looped back up on the same thread. Note that this may " +
      "cause problems (e.g. deadlocks) in some applications, so make sure that your code can handle this. " +
      "Issue: https://issues.redhat.com/browse/JGRP-2547")
    protected boolean loopback;

    protected static final int DEFAULT_INITIAL_CAPACITY=128;
    protected static final int DEFAULT_INCREMENT=512;

    /* --------------------------------------------- JMX  ---------------------------------------------- */

    @ManagedAttribute(description="Number of message sent",type=SCALAR)
    protected final LongAdder num_msgs_sent=new LongAdder();
    @ManagedAttribute(description="Number of message received",type=SCALAR)
    protected final LongAdder num_msgs_received=new LongAdder();
    @ManagedAttribute(description="Number of acks sent",type=SCALAR)
    protected final LongAdder num_acks_sent=new LongAdder();
    @ManagedAttribute(description="Number of acks received",type=SCALAR)
    protected final LongAdder num_acks_received=new LongAdder();
    @ManagedAttribute(description="Number of retransmitted messages",type=SCALAR)
    protected final LongAdder num_xmits=new LongAdder();

    @ManagedAttribute(description="Number of retransmit requests received",type=SCALAR)
    protected final LongAdder  xmit_reqs_received=new LongAdder();

    @ManagedAttribute(description="Number of retransmit requests sent",type=SCALAR)
    protected final LongAdder  xmit_reqs_sent=new LongAdder();

    @ManagedAttribute(description="Number of retransmit responses sent",type=SCALAR)
    protected final LongAdder  xmit_rsps_sent=new LongAdder();

    @ManagedAttribute(description="Average batch size of messages delivered to the application")
    protected final AverageMinMax avg_delivery_batch_size=new AverageMinMax(1024);

    @ManagedAttribute(description="True if sending a message can block at the transport level")
    protected boolean sends_can_block=true;

    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean is_trace=log.isTraceEnabled();

    @ManagedAttribute(description="Whether or not a RELAY protocol was found below in the stack")
    protected boolean relay_present;

    /* --------------------------------------------- Fields ------------------------------------------------ */


    protected final Map<Address,SenderEntry>   send_table=Util.createConcurrentMap();
    protected final Map<Address,ReceiverEntry> recv_table=Util.createConcurrentMap();
    /** To cache batches for sending messages up the stack (https://issues.redhat.com/browse/JGRP-2841).
     * Note: values are all _regular_ batches; OOB batches/messages have been passed up, or were removed when
     * draining the table in removeAndDeliver() */
    protected final Map<Address,MessageBatch>  cached_batches=Util.createConcurrentMap();

    protected final ReentrantLock          recv_table_lock=new ReentrantLock();

    /** Used by the retransmit task to keep the last retransmitted seqno per member (applicable only
     * for received messages (ReceiverEntry)): https://issues.redhat.com/browse/JGRP-1539 */
    protected final Map<Address,Long>      xmit_task_map=new ConcurrentHashMap<>();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                    xmit_task;

    protected volatile List<Address>       members=new ArrayList<>(11);

    protected TimeScheduler                timer; // used for retransmissions

    protected volatile boolean             running;

    protected short                        last_conn_id;

    protected AgeOutCache<Address>         cache;

    protected TimeService                  time_service; // for aging out of receiver and send entries

    protected final AtomicInteger          timestamper=new AtomicInteger(0); // timestamping of ACKs / SEND_FIRST-SEQNOs

    /** Keep track of when a SEND_FIRST_SEQNO message was sent to a given sender */
    protected ExpiryCache<Address>         last_sync_sent;

    @ManagedAttribute(description="Number of unicast messages to self looped back up",type=SCALAR)
    protected final LongAdder              num_loopbacks=new LongAdder();

    // Queues messages until a {@link ReceiverEntry} has been created. Queued messages are then removed from
    // the cache and added to the ReceiverEntry
    protected final MessageCache           msg_cache=new MessageCache();

    protected static final Message         DUMMY_OOB_MSG=new EmptyMessage().setFlag(OOB);

    protected final Predicate<Message>     drop_oob_and_dont_loopback_msgs_filter=msg ->
      msg != null && msg != DUMMY_OOB_MSG
        && (!msg.isFlagSet(OOB) || msg.setFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
        && !(msg.isFlagSet(DONT_LOOPBACK) && Objects.equals(local_addr, msg.getSrc()));

    protected static final Predicate<Message> remove_filter=m -> m != null
      && (m.isFlagSet(DONT_LOOPBACK) || m == DUMMY_OOB_MSG || m.isFlagSet(OOB_DELIVERED));

    protected static final BiConsumer<MessageBatch,Message> BATCH_ACCUMULATOR=MessageBatch::add;

    protected abstract Buffer<Message> createBuffer(long initial_seqno);
    protected Buffer.Options           sendOptions() {return Buffer.Options.DEFAULT();}
    protected abstract boolean         needToSendAck(Entry e, int num_acks);

    public long getNumLoopbacks() {return num_loopbacks.sum();}

    @ManagedAttribute(description="Returns the number of outgoing (send) connections",type=SCALAR)
    public int getNumSendConnections() {
        return send_table.size();
    }

    @ManagedAttribute(description="Returns the number of incoming (receive) connections",type=SCALAR)
    public int getNumReceiveConnections() {
        return recv_table.size();
    }

    @ManagedAttribute(description="Returns the total number of outgoing (send) and incoming (receive) connections",type=SCALAR)
    public int getNumConnections() {
        return getNumReceiveConnections() + getNumSendConnections();
    }

    @ManagedAttribute(description="Next seqno issued by the timestamper",type=SCALAR)
    public int getTimestamper() {return timestamper.get();}

    @Property(name="level", description="Sets the level")
    public <T extends Protocol> T setLevel(String level) {
        T retval= super.setLevel(level);
        is_trace=log.isTraceEnabled();
        return retval;
    }
    public long            getXmitInterval()                     {return xmit_interval;}
    public ReliableUnicast setXmitInterval(long i)               {xmit_interval=i; return this;}
    public boolean         isXmitsEnabled()                      {return xmits_enabled;}
    public ReliableUnicast setXmitsEnabled(boolean b)            {xmits_enabled=b; return this;}
    public long            getConnExpiryTimeout()                {return conn_expiry_timeout;}
    public ReliableUnicast setConnExpiryTimeout(long c)          {this.conn_expiry_timeout=c; return this;}
    public long            getConnCloseTimeout()                 {return conn_close_timeout;}
    public ReliableUnicast setConnCloseTimeout(long c)           {this.conn_close_timeout=c; return this;}
    public boolean         logNotFoundMsgs()                     {return log_not_found_msgs;}
    public ReliableUnicast logNotFoundMsgs(boolean l)            {this.log_not_found_msgs=l; return this;}
    public long            getSyncMinInterval()                  {return sync_min_interval;}
    public ReliableUnicast setSyncMinInterval(long s)            {this.sync_min_interval=s; return this;}
    public int             getMaxXmitReqSize()                   {return max_xmit_req_size;}
    public ReliableUnicast setMaxXmitReqSize(int m)              {this.max_xmit_req_size=m; return this;}
    public boolean         reuseMessageBatches()                 {return reuse_message_batches;}
    public ReliableUnicast reuseMessageBatches(boolean b)        {this.reuse_message_batches=b; return this;}
    public boolean         sendsCanBlock()                       {return sends_can_block;}
    public ReliableUnicast sendsCanBlock(boolean s)              {this.sends_can_block=s; return this;}
    public boolean         sendAtomically()                      {return send_atomically;}
    public ReliableUnicast sendAtomically(boolean f)             {send_atomically=f; return this;}
    public boolean         loopback()                            {return loopback;}
    public ReliableUnicast loopback(boolean b)                   {this.loopback=b; return this;}
    public ReliableUnicast timeService(TimeService ts)           {this.time_service=ts; return this;}  // testing only!
    public ReliableUnicast lastSync(ExpiryCache<Address> c)      {this.last_sync_sent=c; return this;} // testing only!


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

    @ManagedOperation(description="Prints the cached batches (if reuse_message_batches is true)")
    public String printCachedBatches() {
        return "\n" + cached_batches.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    @ManagedOperation(description="Prints the cached batches (if reuse_message_batches is true)")
    public ReliableUnicast clearCachedBatches() {
        cached_batches.clear();
        return this;
    }

    @ManagedOperation(description="Adjust the capacity of cached batches")
    public ReliableUnicast trimCachedBatches() {
        cached_batches.values().forEach(mb -> mb.array().trimTo(DEFAULT_INITIAL_CAPACITY));
        return this;
    }

    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long getNumMessagesSent()     {return num_msgs_sent.sum();}

    /** Don't remove! https://issues.redhat.com/browse/JGRP-2814 */
    @ManagedAttribute(type=SCALAR) @Deprecated
    public long getNumMessagesReceived() {return num_msgs_received.sum();}


    public long getNumAcksSent()         {return num_acks_sent.sum();}
    public long getNumAcksReceived()     {return num_acks_received.sum();}
    public long getNumXmits()            {return num_xmits.sum();}
    public long getMaxRetransmitTime()   {return max_retransmit_time;}

    @Property(description="Max number of milliseconds we try to retransmit a message to any given member. After that, " +
      "the connection is removed. Any new connection to that member will start with seqno #1 again. 0 disables this",
      type=AttributeType.TIME)
    public ReliableUnicast setMaxRetransmitTime(long max_retransmit_time) {
        this.max_retransmit_time=max_retransmit_time;
        if(cache != null && max_retransmit_time > 0)
            cache.setTimeout(max_retransmit_time);
        return this;
    }

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

    @ManagedAttribute(type=SCALAR)
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
        Entry entry=send_table.get(dest);
        return entry != null && entry.state() == State.OPEN;
    }

    /** The number of messages in all Entry.sent_msgs tables (haven't received an ACK yet) */
    @ManagedAttribute(type=SCALAR)
    public int getNumUnackedMessages() {
        return accumulate(Buffer::size, send_table.values());
    }

    public int getNumUnackedMessages(Address dest) {
        Entry entry=send_table.get(dest);
        return entry != null ? entry.buf.size() : 0;
    }

    @ManagedAttribute(description="Total number of undelivered messages in all receive windows",type=SCALAR)
    public int getXmitTableUndeliveredMessages() {
        return accumulate(Buffer::size, recv_table.values());
    }

    @ManagedAttribute(description="Total number of missing messages in all receive windows",type=SCALAR)
    public int getXmitTableMissingMessages() {
        return accumulate(Buffer::numMissing, recv_table.values());
    }

    @ManagedAttribute(description="Total number of deliverable messages in all receive windows",type=SCALAR)
    public int getXmitTableDeliverableMessages() {
        return accumulate(Buffer::getNumDeliverable, recv_table.values());
    }

    @ManagedOperation(description="Prints the contents of the receive windows for all members")
    public String printReceiveWindowMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address addr=entry.getKey();
            Buffer<Message> buf=entry.getValue().buf;
            ret.append(addr).append(": ").append(buf).append('\n');
        }
        return ret.toString();
    }

    @ManagedOperation(description="Prints the contents of the send windows for all members")
    public String printSendWindowMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
            Address addr=entry.getKey();
            Buffer<Message> buf=entry.getValue().buf;
            ret.append(addr).append(": ").append(buf).append('\n');
        }
        return ret.toString();
    }

    public void resetStats() {
        avg_delivery_batch_size.clear();
        Stream.of(num_msgs_sent, num_msgs_received, num_acks_sent, num_acks_received, num_xmits, xmit_reqs_received,
                  xmit_reqs_sent, xmit_rsps_sent, num_loopbacks).forEach(LongAdder::reset);
        send_table.values().stream().map(e -> e.buf).forEach(Buffer::resetStats);
        recv_table.values().stream().map(e -> e.buf).forEach(Buffer::resetStats);
    }

    public void init() throws Exception {
        super.init();
        TP transport=getTransport();
        sends_can_block=transport instanceof TCP; // UDP and TCP_NIO2 won't block
        time_service=transport.getTimeService();
        if(time_service == null)
            throw new IllegalStateException("time service from transport is null");
        last_sync_sent=new ExpiryCache<>(sync_min_interval);

        // max bundle size (minus overhead) divided by <long size> times bits per long
        // Example: for 8000 missing messages, SeqnoList has a serialized size of 1012 bytes, for 64000 messages, the
        // serialized size is 8012 bytes. Therefore, for a serialized size of 64000 bytes, we can retransmit a max of
        // 8 * 64000 = 512'000 seqnos
        // see SeqnoListTest.testSerialization3()
        int estimated_max_msgs_in_xmit_req=(transport.getBundler().getMaxSize() -50) * Global.LONG_SIZE;
        int old_max_xmit_size=max_xmit_req_size;
        if(max_xmit_req_size <= 0)
            max_xmit_req_size=estimated_max_msgs_in_xmit_req;
        else
            max_xmit_req_size=Math.min(max_xmit_req_size, estimated_max_msgs_in_xmit_req);
        if(old_max_xmit_size != max_xmit_req_size)
            log.trace("%s: set max_xmit_req_size from %d to %d", local_addr, old_max_xmit_size, max_xmit_req_size);

        if(xmit_interval <= 0) {
            log.warn("%s: xmit_interval (%d) has to be > 0; setting it to the default of %d",
                     local_addr, xmit_interval, DEFAULT_XMIT_INTERVAL);
            xmit_interval=DEFAULT_XMIT_INTERVAL;
        }

        if(xmits_enabled == false) {
            // https://issues.redhat.com/browse/JGRP-2676
            RejectedExecutionHandler handler=transport.getThreadPool().getRejectedExecutionHandler();
            if(handler != null && !isCallerRunsHandler(handler)) {
                log.warn("%s: xmits_enabled == false requires a CallerRunsPolicy in the thread pool; replacing %s",
                         local_addr, handler.getClass().getSimpleName());
                transport.getThreadPool().setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            }
            Class<RED> cl=RED.class;
            if(stack.findProtocol(cl) != null) {
                String e=String.format("found %s: when retransmission is disabled (xmits_enabled=false), this can lead " +
                                         "to message loss. Please remove %s or enable retransmission",
                                       cl.getSimpleName(), cl.getSimpleName());
                throw new IllegalStateException(e);
            }
        }
        relay_present=ProtocolStack.findProtocol(this.down_prot, true, RELAY.class) != null;
    }

    public void start() throws Exception {
        msg_cache.clear();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        if(max_retransmit_time > 0)
            cache=new AgeOutCache<>(timer, max_retransmit_time, this);
        running=true;
        startRetransmitTask();
    }

    public void stop() {
        sendPendingAcks();
        running=false;
        stopRetransmitTask();
        xmit_task_map.clear();
        removeAllConnections();
        msg_cache.clear();
    }

    protected void handleUpEvent(Address sender, Message msg, UnicastHeader hdr) {
        try {
            switch(hdr.type) {
                case DATA:  // received regular message
                    throw new IllegalStateException("header of type DATA is not supposed to be handled by this method");
                case UnicastHeader.ACK:   // received ACK for previously sent message
                    handleAckReceived(sender, hdr.seqno, hdr.conn_id, hdr.timestamp());
                    break;
                case UnicastHeader.SEND_FIRST_SEQNO:
                    handleResendingOfFirstMessage(sender, hdr.timestamp());
                    break;
                case UnicastHeader.XMIT_REQ:  // received ACK for previously sent message
                    handleXmitRequest(sender, msg.getObject());
                    break;
                case UnicastHeader.CLOSE:
                    log.trace("%s <-- %s: CLOSE(conn-id=%s)", local_addr, sender, hdr.conn_id);
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
        catch(Throwable t) { // we cannot let an exception terminate the processing of this batch
            log.error(Util.getMessage("FailedHandlingEvent"), local_addr, t);
        }
    }

    public Object up(Message msg) {
        Address dest=msg.dest(), sender=msg.src();
        if(dest == null || dest.isMulticast() || msg.isFlagSet(NO_RELIABILITY)) // only handle unicast messages
            return up_prot.up(msg);  // pass up

        UnicastHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);
        switch(hdr.type) {
            case DATA:      // received regular message
                if(is_trace)
                    log.trace("%s <-- %s: DATA(#%d, conn_id=%d%s)", local_addr, sender, hdr.seqno, hdr.conn_id, hdr.first? ", first" : "");
                if(Objects.equals(local_addr, sender))
                    handleDataReceivedFromSelf(sender, hdr.seqno, msg);
                else
                    handleDataReceived(sender, hdr.seqno, hdr.conn_id, hdr.first, msg);
                break; // we pass the deliverable message up in handleDataReceived()
            default:
                handleUpEvent(sender, msg, hdr);
                break;
        }
        return null;
    }

    public void up(MessageBatch batch) {
        if(batch.dest() == null || batch.dest().isMulticast()) { // not a unicast batch
            up_prot.up(batch);
            return;
        }
        final Address sender=batch.sender();
        if(local_addr == null || local_addr.equals(sender)) {
            Entry entry=local_addr != null? send_table.get(local_addr) : null;
            if(entry != null)
                handleBatchFromSelf(batch, entry);
            return;
        }

        int   size=batch.size();
        short highest_conn_id=0;
        long  lowest_seqno=-1; // -1 means uninitialized (first: 1)
        boolean first_seqno=false;
        Map<Short,List<LongTuple<Message>>> msgs=new ConcurrentHashMap<>();
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            UnicastHeader hdr;
            if(msg == null || msg.isFlagSet(NO_RELIABILITY) || (hdr=msg.getHeader(id)) == null)
                continue;
            it.remove(); // remove the message from the batch, so it won't be passed up the stack
            if(hdr.type != DATA) {
                handleUpEvent(msg.getSrc(), msg, hdr);
                continue;
            }
            highest_conn_id=max(hdr.conn_id, highest_conn_id);
            if(lowest_seqno < 0) {
                lowest_seqno=hdr.seqno;
                first_seqno=hdr.first;
            }
            else {
                if(lowest_seqno > hdr.seqno) {
                    lowest_seqno=hdr.seqno;
                    first_seqno=hdr.first;
                }
            }
            List<LongTuple<Message>> list=msgs.computeIfAbsent(hdr.conn_id, k -> new FastArray<>(size));
            list.add(new LongTuple<>(hdr.seqno(), msg));
        }
        if(msgs.isEmpty()) {
            up_prot.up(batch);
            return;
        }

        List<LongTuple<Message>> list=msgs.get(highest_conn_id);
        if(msgs.size() > 1) { // more than 1 conn_id (this should not normally be the case)
            msgs.keySet().retainAll(List.of(highest_conn_id));
            Tuple<Long,Boolean> tuple=getLowestSeqno(id, list);
            lowest_seqno=tuple.getVal1();
            first_seqno=tuple.getVal2();
        }

        ReceiverEntry entry=getReceiverEntry(sender, lowest_seqno, first_seqno, highest_conn_id, batch.dest());
        if(entry == null) {
            if(!list.isEmpty()) {
                for(LongTuple<Message> tuple: list) {
                    msg_cache.add(sender, tuple.getVal2());
                    log.trace("%s: cached %s#%d", local_addr, sender, tuple.getVal1());
                }
            }
            return;
        }
        boolean added_queued_msgs=false;
        if(!msg_cache.isEmpty()) { // quick and dirty check
            Collection<Message> queued_msgs=msg_cache.drain(sender);
            if(queued_msgs != null) {
                addQueuedMessages(sender, entry, queued_msgs);
                added_queued_msgs=true;
            }
        }

        if(added_queued_msgs || (list != null && !list.isEmpty()))
            handleBatchReceived(entry, sender, list, batch.mode() == MessageBatch.Mode.OOB, batch.dest());

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleBatchFromSelf(MessageBatch batch, Entry entry) {
        List<LongTuple<Message>> list=new ArrayList<>(batch.size());
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            UnicastHeader hdr;
            if(msg == null || msg.isFlagSet(NO_RELIABILITY) || (hdr=msg.getHeader(id)) == null)
                continue;
            it.remove(); // remove the message from the batch, so it won't be passed up the stack

            if(hdr.type != DATA) {
                handleUpEvent(msg.getSrc(), msg, hdr);
                continue;
            }

            if(entry.conn_id != hdr.conn_id) {
                it.remove();
                continue;
            }
            list.add(new LongTuple<>(hdr.seqno(), msg));
        }

        if(!list.isEmpty()) {
            if(is_trace)
                log.trace("%s <-- %s: DATA(%s)", local_addr, batch.sender(), printMessageList(list));

            int len=list.size();
            Buffer<Message> win=entry.buf;
            update(entry, len);

            // OOB msg is passed up. When removed, we discard it. Affects ordering: https://issues.redhat.com/browse/JGRP-379
            if(batch.mode() == MessageBatch.Mode.OOB) {
                MessageBatch oob_batch=new MessageBatch(local_addr, batch.sender(), batch.clusterName(), batch.multicast(), MessageBatch.Mode.OOB, len);
                for(LongTuple<Message> tuple: list) {
                    long    seq=tuple.getVal1();
                    Message msg=win.get(seq); // we *have* to get the message, because loopback means we didn't add it to win !
                    if(msg != null && msg.isFlagSet(OOB) && msg.setFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
                        oob_batch.add(msg);
                }
                deliverBatch(oob_batch, entry, batch.dest());
            }
            removeAndDeliver(entry, batch.sender(), batch.clusterName(), batch.capacity());
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public Object down(Event evt) {
        switch (evt.getType()) {

            case Event.VIEW_CHANGE:  // remove connections to peers that are not members anymore !
                View view=evt.getArg();
                List<Address> new_members=view.getMembers();
                Set<Address> non_members=new HashSet<>(send_table.keySet());
                non_members.addAll(recv_table.keySet());
                members=new_members;
                new_members.forEach(non_members::remove);
                if(cache != null)
                    cache.removeAll(new_members);

                if(!non_members.isEmpty()) {
                    log.trace("%s: closing connections to non members %s", local_addr, non_members);
                    // remove all non-members, except those from remote sites: https://issues.redhat.com/browse/JGRP-2729
                    non_members.stream().filter(this::isLocal).forEach(this::closeConnection);
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
                last_sync_sent.removeExpiredElements();
                cached_batches.keySet().retainAll(new_members);
                break;
        }

        return down_prot.down(evt);          // Pass on to the layer below us
    }

    public Object down(Message msg) {
        Address dst=msg.getDest();
        if(dst == null || dst.isMulticast() || msg.isFlagSet(NO_RELIABILITY)) // only handle unicast messages
            return down_prot.down(msg);

        if(!running) {
            log.trace("%s: discarded message as start() has not yet been called, message: %s", local_addr, msg);
            return null;
        }

        if(msg.getSrc() == null)
            msg.setSrc(local_addr); // this needs to be done, so we can check whether the message sender is the local_addr

        // if the destination is the local site master, we change the it to be the local address. The reason is that
        // the message will be looped back and the send-table entry (msg.dest) should match msg.src of the
        // received message (https://issues.redhat.com/browse/JGRP-2729)
        if(isLocalSiteMaster(dst))
            msg.dest(dst=local_addr);

        if(loopback && Objects.equals(local_addr, dst)) {// https://issues.redhat.com/browse/JGRP-2547
            if(msg.isFlagSet(DONT_LOOPBACK))
                return null;
            num_loopbacks.increment();
            return up_prot.up(msg);
        }

        SenderEntry entry=getSenderEntry(dst);
        boolean dont_loopback_set=msg.isFlagSet(DONT_LOOPBACK) && dst.equals(local_addr),
          dont_block=msg.isFlagSet(DONT_BLOCK);
        if(send(msg, entry, dont_loopback_set, dont_block))
            num_msgs_sent.increment();
        return null; // the message was already sent down the stack in send()
    }

    protected boolean isLocalSiteMaster(Address dest) {
        // quick check to avoid the use of 'instanceof'; will be removed once https://bugs.openjdk.org/browse/JDK-8180450
        // has been fixed (in Java 22, should be backported to older versions)
        if(relay_present && dest.isSiteMaster()) {
            Object ret=down_prot.down(new Event(Event.IS_LOCAL_SITEMASTER, dest));
            return ret != null && (Boolean)ret;
        }
        return false;
    }

    protected boolean isLocal(Address addr) {
        // quick check to avoid the use of 'instanceof'; will be removed once https://bugs.openjdk.org/browse/JDK-8180450
        // has been fixed (in Java 22, should be backported to older versions)
        if(relay_present && addr.isSiteAddress()) {
            Object ret=down_prot.down(new Event(Event.IS_LOCAL, addr));
            return ret != null && (Boolean)ret;
        }
        return true;
    }

    /**
     * Removes and resets from connection table (which is already locked). Returns true if member was found,
     * otherwise false. This method is public only so it can be invoked by unit testing, but should not be used !
     */
    public void closeConnection(Address mbr) {
        closeSendConnection(mbr);
        closeReceiveConnection(mbr);
    }

    public void closeSendConnection(Address mbr) {
        SenderEntry entry=send_table.get(mbr);
        if(entry != null)
            entry.state(State.CLOSING);
    }

    public void closeReceiveConnection(Address mbr) {
        ReceiverEntry entry=recv_table.get(mbr);
        if(entry != null)
            entry.state(State.CLOSING);
    }

    public void removeSendConnection(Address mbr) {
        SenderEntry entry=send_table.remove(mbr);
        if(entry != null) {
            entry.state(State.CLOSED);
            if(members.contains(mbr))
                sendClose(mbr, entry.connId());
        }
    }

    public void removeReceiveConnection(Address mbr) {
        sendPendingAcks();
        ReceiverEntry entry=recv_table.remove(mbr);
        if(entry != null)
            entry.state(State.CLOSED);
    }

    /**
     * This method is public only so it can be invoked by unit testing, but should not otherwise be used !
     */
    @ManagedOperation(description="Trashes all connections to other nodes. This is only used for testing")
    public void removeAllConnections() {
        for(SenderEntry se: send_table.values())
            se.state(State.CLOSED);
        send_table.clear();
        recv_table.clear();
    }

    /** Sends a retransmit request to the given sender */
    protected void retransmit(SeqnoList missing, Address sender, Address real_dest) {
        Message xmit_msg=new ObjectMessage(sender, missing).setFlag(OOB, NO_FC)
          .putHeader(id, UnicastHeader.createXmitReqHeader());
        if(!Objects.equals(local_addr, real_dest))
            xmit_msg.setSrc(real_dest);
        if(is_trace)
            log.trace("%s --> %s: XMIT_REQ(%s)", local_addr, sender, missing);
        down_prot.down(xmit_msg);
        xmit_reqs_sent.add(missing.size());
    }

    /** Called by the sender to resend messages for which no ACK has been received yet */
    protected void retransmit(Message msg) {
        if(is_trace) {
            UnicastHeader hdr=msg.getHeader(id);
            long seqno=hdr != null? hdr.seqno : -1;
            log.trace("%s --> %s: resending(#%d)", local_addr, msg.getDest(), seqno);
        }
        resend(msg);
        num_xmits.increment();
    }

    /** Called by AgeOutCache, to removed expired connections */
    public void expired(Address key) {
        if(key != null) {
            log.debug("%s: removing expired connection to %s", local_addr, key);
            closeConnection(key);
        }
    }

    /**
     * Check whether the hashtable contains an entry e for {@code sender} (create if not). If
     * e.received_msgs is null and {@code first} is true: create a new AckReceiverWindow(seqno) and
     * add message. Set e.received_msgs to the new window. Else just add the message.
     */
    protected void handleDataReceived(final Address sender, long seqno, short conn_id,  boolean first, final Message msg) {
        ReceiverEntry entry=getReceiverEntry(sender, seqno, first, conn_id, msg.dest());
        if(entry == null) {
            msg_cache.add(sender, msg);
            log.trace("%s: cached %s#%d", local_addr, sender, seqno);
            return;
        }
        if(!msg_cache.isEmpty()) { // quick and dirty check
            Collection<Message> queued_msgs=msg_cache.drain(sender);
            if(queued_msgs != null)
                addQueuedMessages(sender, entry, queued_msgs);
        }
        addMessage(entry, sender, seqno, msg);
        removeAndDeliver(entry, sender, null, 1);
    }

    protected void addMessage(ReceiverEntry entry, Address sender, long seqno, Message msg) {
        final Buffer<Message> win=entry.buf();
        update(entry, 1);
        boolean oob=msg.isFlagSet(OOB),
          added=win.add(seqno, oob? DUMMY_OOB_MSG : msg); // adding the same dummy OOB msg saves space (we won't remove it)

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it. This affects ordering !
        // https://issues.redhat.com/browse/JGRP-377
        if(oob && added)
            deliverMessage(msg, sender, seqno);
        if(needToSendAck(entry, 1))
            sendAck(sender, entry, msg.dest());
    }

    protected void addQueuedMessages(final Address sender, final ReceiverEntry entry, Collection<Message> queued_msgs) {
        for(Message msg: queued_msgs) {
            UnicastHeader hdr=msg.getHeader(this.id);
            if(hdr.conn_id != entry.conn_id) {
                log.warn("%s: dropped queued message %s#%d as its conn_id (%d) did not match (entry.conn_id=%d)",
                         local_addr, sender, hdr.seqno, hdr.conn_id, entry.conn_id);
                continue;
            }
            addMessage(entry, sender, hdr.seqno(), msg);
        }
    }

    /** Called when the sender of a message is the local member. In this case, we don't need to add the message
     * to the table as the sender already did that */
    protected void handleDataReceivedFromSelf(final Address sender, long seqno, Message msg) {
        Entry entry=send_table.get(sender);
        if(entry == null || entry.state() == State.CLOSED) {
            log.warn("%s: entry not found for %s; dropping message", local_addr, sender);
            return;
        }

        update(entry, 1);
        final Buffer<Message> win=entry.buf;

        // An OOB message is passed up immediately. Later, when remove() is called, we discard it.
        // This affects ordering ! JIRA: https://issues.redhat.com/browse/JGRP-377
        if(msg.isFlagSet(OOB)) {
            msg=win.get(seqno); // we *have* to get a message, because loopback means we didn't add it to win !
            if(msg != null && msg.isFlagSet(OOB) && msg.setFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
                deliverMessage(msg, sender, seqno);
        }
        removeAndDeliver(entry, sender, null, 1); // there might be more messages to deliver
    }

    protected void handleBatchReceived(final ReceiverEntry entry, Address sender, List<LongTuple<Message>> msgs,
                                       boolean oob, Address original_dest) {
        if(is_trace)
            log.trace("%s <-- %s: DATA(%s)", local_addr, sender, printMessageList(msgs));

        int batch_size=msgs.size();
        Buffer<Message> buf=entry.buf;

        // adds all messages to the table, removing messages from 'msgs' which could not be added (already present)
        boolean added=buf.add(msgs, oob, oob? DUMMY_OOB_MSG : null);
        update(entry, batch_size);
        entry.sendAck();

        // OOB msg is passed up. When removed, we discard it. Affects ordering: https://issues.redhat.com/browse/JGRP-379
        if(added && oob) {
            MessageBatch oob_batch=new MessageBatch(local_addr, sender, null, false, MessageBatch.Mode.OOB, msgs.size());
            for(LongTuple<Message> tuple: msgs)
                oob_batch.add(tuple.getVal2());
            deliverBatch(oob_batch, entry, original_dest);
        }
        removeAndDeliver(entry, sender, null, msgs.size());
    }


    /**
     * Try to remove as many messages as possible from the table as pass them up.
     * Prevents concurrent passing up of messages by different threads (https://issues.redhat.com/browse/JGRP-198);
     * lots of threads can come up to this point concurrently, but only 1 is allowed to pass at a time.
     * We *can* deliver messages from *different* senders concurrently, e.g. reception of P1, Q1, P2, Q2 can result in
     * delivery of P1, Q1, Q2, P2: FIFO (implemented by UNICAST) says messages need to be delivered in the
     * order in which they were sent
     */
    protected void removeAndDeliver(Entry entry, Address sender, AsciiString cluster, int min_size) {
        Buffer<Message> buf=entry.buf();
        AtomicInteger adders=buf.getAdders();
        if(adders.getAndIncrement() != 0)
            return;

        AsciiString cl=cluster != null? cluster : getTransport().getClusterNameAscii();
        int cap=Math.max(Math.max(Math.max(buf.size(), max_batch_size), min_size), DEFAULT_INITIAL_CAPACITY);
        MessageBatch batch=reuse_message_batches && cl != null?
          cached_batches.computeIfAbsent(sender, __ -> new MessageBatch(cap).dest(local_addr).sender(sender).cluster(cl)
            .mcast(true).increment(DEFAULT_INCREMENT))
          : new MessageBatch(cap).dest(local_addr).sender(sender).cluster(cl).multicast(true).increment(DEFAULT_INCREMENT);
        Supplier<MessageBatch> batch_creator=() -> batch;
        MessageBatch mb=null;
        do {
            try {
                batch.reset(); // sets index to 0: important as batch delivery may not remove messages from batch!
                mb=buf.removeMany(true, max_batch_size, drop_oob_and_dont_loopback_msgs_filter,
                                  batch_creator, BATCH_ACCUMULATOR);
            }
            catch(Throwable t) {
                log.error("%s: failed removing messages from table for %s: %s", local_addr, sender, t);
            }
            if(!batch.isEmpty()) {
                // batch is guaranteed to NOT contain any OOB messages as the drop_oob_msgs_filter above removed them
                deliverBatch(batch, entry, null); // catches Throwable
            }
        }
        while(mb != null || adders.decrementAndGet() != 0);
    }

    protected String printMessageList(List<LongTuple<Message>> list) {
        StringBuilder sb=new StringBuilder();
        int size=list.size();
        Message first=size > 0? list.get(0).getVal2() : null, second=size > 1? list.get(size-1).getVal2() : first;
        UnicastHeader hdr;
        if(first != null) {
            hdr=first.getHeader(id);
            if(hdr != null)
                sb.append("#" + hdr.seqno);
        }
        if(second != null) {
            hdr=second.getHeader(id);
            if(hdr != null)
                sb.append(" - #" + hdr.seqno);
        }
        return sb.toString();
    }

    protected ReceiverEntry getReceiverEntry(Address sender, long seqno, boolean first, short conn_id, Address real_dest) {
        ReceiverEntry entry=recv_table.get(sender);
        if(entry != null && entry.connId() == conn_id)
            return entry;
        return _getReceiverEntry(sender, seqno, first, conn_id, real_dest);
    }

    // public for unit testing - don't use in app code!
    public ReceiverEntry _getReceiverEntry(Address sender, long seqno, boolean first, short conn_id, Address real_dest) {
        ReceiverEntry entry;
        recv_table_lock.lock();
        try {
            entry=recv_table.get(sender);
            if(entry == null) {
                if(first)
                    return createReceiverEntry(sender, seqno, conn_id, real_dest);
                else {
                    recv_table_lock.unlock();
                    sendRequestForFirstSeqno(sender, real_dest); // drops the message and returns (see below)
                    return null;
                }
            }
            // entry != null
            return compareConnIds(conn_id, entry.connId(), first, entry, sender, seqno, real_dest);
        }
        finally {
            if(recv_table_lock.isHeldByCurrentThread())
                recv_table_lock.unlock();
        }
    }

    protected ReceiverEntry compareConnIds(short other, short mine, boolean first, ReceiverEntry e,
                                           Address sender, long seqno, Address real_dest) {
        if(other == mine)
            return e;
        if(other < mine)
            return null;
        // other_conn_id > my_conn_id
        if(first) {
            log.trace("%s: other conn_id (%d) > mine (%d); creating new receiver window", local_addr, other, mine);
            recv_table.remove(sender);
            return createReceiverEntry(sender, seqno, other, real_dest);
        }
        else {
            log.trace("%s: other conn_id (%d) > mine (%d) (!first); asking for first message", local_addr, other, mine);
            recv_table_lock.unlock();
            sendRequestForFirstSeqno(sender, real_dest); // drops the message and returns (see below)
            return null;
        }
    }

    protected SenderEntry getSenderEntry(Address dst) {
        SenderEntry entry=send_table.get(dst);
        if(entry == null || entry.state() == State.CLOSED) {
            if(entry != null)
                send_table.remove(dst, entry);
            entry=send_table.computeIfAbsent(dst, k -> new SenderEntry(getNewConnectionId()));
            log.trace("%s: created sender window for %s (conn-id=%s)", local_addr, dst, entry.connId());
            if(cache != null && !members.contains(dst))
                cache.add(dst);
        }
        return entry;
    }

    protected ReceiverEntry createReceiverEntry(Address sender, long seqno, short conn_id, Address dest) {
        ReceiverEntry entry=recv_table.computeIfAbsent(sender, k -> new ReceiverEntry(createBuffer(seqno-1), conn_id, dest));
        log.trace("%s: created receiver window for %s at seqno=#%d for conn-id=%d", local_addr, sender, seqno, conn_id);
        return entry;
    }

    /** Add the ACK to hashtable.sender.sent_msgs */
    protected void handleAckReceived(Address sender, long seqno, short conn_id, int timestamp) {
        if(is_trace)
            log.trace("%s <-- %s: ACK(#%d, conn-id=%d, ts=%d)", local_addr, sender, seqno, conn_id, timestamp);
        SenderEntry entry=send_table.get(sender);
        if(entry != null && entry.connId() != conn_id) {
            log.trace("%s: my conn_id (%d) != received conn_id (%d); discarding ACK", local_addr, entry.connId(), conn_id);
            return;
        }

        Buffer<Message> win=entry != null? entry.buf : null;
        if(win != null && entry.updateLastTimestamp(timestamp)) {
            win.purge(seqno, true); // removes all messages <= seqno (forced purge)
            num_acks_received.increment();
        }
    }

    /** We need to resend the first message with our conn_id */
    protected void handleResendingOfFirstMessage(Address sender, int timestamp) {
        log.trace("%s <-- %s: SEND_FIRST_SEQNO", local_addr, sender);
        SenderEntry entry=send_table.get(sender);
        Buffer<Message> win=entry != null? entry.buf : null;
        if(win == null) {
            log.warn(Util.getMessage("SenderNotFound"), local_addr, sender);
            return;
        }

        if(!entry.updateLastTimestamp(timestamp))
            return;

        Message rsp=win.get(win.low() +1);
        if(rsp != null) {
            // We need to copy the UnicastHeader and put it back into the message because Message.copy() doesn't copy
            // the headers, and therefore we'd modify the original message in the sender retransmission window
            // (https://issues.redhat.com/browse/JGRP-965)
            Message copy=rsp.copy(true, true);
            UnicastHeader hdr=copy.getHeader(this.id);
            UnicastHeader newhdr=hdr.copy();
            newhdr.first=true;
            copy.putHeader(this.id, newhdr).setFlag(DONT_BLOCK);
            resend(copy);
        }
    }

    protected void handleXmitRequest(Address sender, SeqnoList missing) {
        if(is_trace)
            log.trace("%s <-- %s: XMIT(#%s)", local_addr, sender, missing);

        SenderEntry entry=send_table.get(sender);
        xmit_reqs_received.add(missing.size());
        Buffer<Message> win=entry != null? entry.buf : null;
        if(win == null)
            return;
        for(Long seqno: missing) {
            Message msg=win.get(seqno);
            if(msg == null) {
                if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(sender) && seqno > win.low())
                    log.warn(Util.getMessage("MessageNotFound"), local_addr, sender, seqno);
                continue;
            }
            // This will change the original message, but that's fine as retransmissions will have DONT_BLOCK anyway
            msg.setFlag(DONT_BLOCK);
            resend(msg);
            xmit_rsps_sent.increment();
        }
    }

    protected boolean send(Message msg, SenderEntry entry, boolean dont_loopback_set, boolean dont_block) {
        Buffer<Message> buf=entry.buf;
        long seqno=entry.seqno.getAndIncrement();
        short send_conn_id=entry.connId();
        msg.putHeader(this.id,UnicastHeader.createDataHeader(seqno, send_conn_id,seqno == DEFAULT_FIRST_SEQNO));
        final Lock lock=send_atomically? buf.lock() : null;
        if(lock != null) {
            // As described in doc/design/NAKACK4 ("misc"): if we hold the lock while (1) getting the seqno for a message,
            // (2) adding it to the send window and (3) sending it (so it is sent by the transport in that order).
            // Messages should be received in order and therefore not require retransmissions.
            // Passing the message down should not block with TransferQueueBundler (default), as drop_when_full==true
            //noinspection LockAcquiredButNotSafelyReleased
            lock.lock();
        }
        try {
            boolean added=addToSendBuffer(buf, seqno, msg, dont_loopback_set? remove_filter : null, dont_block);
            if(!added) // e.g. message already present in send buffer, or no space and dont_block set
                return false;
            down_prot.down(msg); // if this fails, since msg is in sent_msgs, it can be retransmitted
            if(entry.state() == State.CLOSING)
                entry.state(State.OPEN);
            if(conn_expiry_timeout > 0)
                entry.update();
            if(dont_loopback_set)
                buf.purge(buf.getHighestDeliverable());
        }
        finally {
            if(lock != null)
                lock.unlock();
        }
        if(is_trace) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(" --> ").append(msg.dest()).append(": DATA(").append("#").append(seqno).
              append(", conn_id=").append(send_conn_id);
            if(seqno == DEFAULT_FIRST_SEQNO) sb.append(", first");
            sb.append(')');
            log.trace(sb);
        }
        return true;
    }

    /**
     * Adds the message to the send buffer. The loop tries to handle temporary OOMEs by retrying if add() failed.
     * @return True if added successfully. False if not, e.g. no space in buffer and DONT_BLOCK set, or message
     * already present, or seqno lower than buffer.low
     */
    protected boolean addToSendBuffer(Buffer<Message> win, long seq, Message msg,
                                      Predicate<Message> filter, boolean dont_block) {
        long sleep=10;
        boolean rc=false;
        do {
            try {
                rc=win.add(seq, msg, filter, sendOptions(), dont_block);
                break;
            }
            catch(Throwable t) {
                if(running) {
                    Util.sleep(sleep);
                    sleep=Math.min(5000, sleep*2);
                }
            }
        }
        while(running);
        return rc;
    }


    protected void resend(Message msg) { // needed for byteman ProtPerf script - don't remove!
        down_prot.down(msg);
    }

    protected void deliverMessage(final Message msg, final Address sender, final long seqno) {
        if(is_trace)
            log.trace("%s: delivering %s#%s", local_addr, sender, seqno);
        try {
            up_prot.up(msg);
        }
        catch(Throwable t) {
            log.warn(Util.getMessage("FailedToDeliverMsg"), local_addr, msg.isFlagSet(OOB) ?
              "OOB message" : "message", msg, t);
        }
    }

    protected void deliverBatch(MessageBatch batch, Entry entry, Address original_dest) {
        try {
            if(batch.isEmpty())
                return;
            if(is_trace) {
                Message first=batch.first(), last=batch.last();
                StringBuilder sb=new StringBuilder(local_addr + ": delivering");
                if(first != null && last != null) {
                    UnicastHeader hdr1=first.getHeader(id), hdr2=last.getHeader(id);
                    if(hdr1 != null && hdr2 != null)
                        sb.append(" #").append(hdr1.seqno).append(" - #").append(hdr2.seqno);
                }
                sb.append(" (" + batch.size()).append(" messages)");
                log.trace(sb);
            }
            if(needToSendAck(entry, batch.size()))
                sendAck(batch.sender(), entry, original_dest);
            up_prot.up(batch);
            if(stats)
                avg_delivery_batch_size.add(batch.size());
        }
        catch(Throwable t) {
            log.warn(Util.getMessage("FailedToDeliverMsg"), local_addr, "batch", batch, t);
        }
    }

    protected long getTimestamp() {
        return time_service.timestamp();
    }

    public void startRetransmitTask() {
        if(xmit_task == null || xmit_task.isDone())
            xmit_task=timer.scheduleWithFixedDelay(new RetransmitTask(), 0, xmit_interval, MILLISECONDS, sends_can_block);
    }

    public void stopRetransmitTask() {
        if(xmit_task != null) {
            xmit_task.cancel(true);
            xmit_task=null;
        }
    }

    protected static boolean isCallerRunsHandler(RejectedExecutionHandler h) {
        return h instanceof ThreadPoolExecutor.CallerRunsPolicy ||
          (h instanceof ShutdownRejectedExecutionHandler
            && ((ShutdownRejectedExecutionHandler)h).handler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
    }

    protected void sendAck(Address dst, Entry entry, Address real_dest) { // real_dest required by RELAY3
        if(!running) // if we are disconnected, then don't send any acks which throw exceptions on shutdown
            return;
        long seqno=entry.buf.highestDelivered();
        short conn_id=entry.connId();
        Message ack=new EmptyMessage(dst).setFlag(DONT_BLOCK).setFlag(NO_FC)
          .putHeader(this.id, UnicastHeader.createAckHeader(seqno, conn_id, timestamper.incrementAndGet()));
        if(real_dest != null && !Objects.equals(local_addr, real_dest))
            ack.setSrc(real_dest);
        if(is_trace)
            log.trace("%s --> %s: ACK(#%d)", local_addr, dst, seqno);
        try {
            down_prot.down(ack);
            num_acks_sent.increment();
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedSendingAck"), local_addr, seqno, dst, t);
        }
    }


    protected synchronized short getNewConnectionId() {
        short retval=last_conn_id;
        if(last_conn_id == Short.MAX_VALUE || last_conn_id < 0)
            last_conn_id=0;
        else
            last_conn_id++;
        return retval;
    }


    protected void sendRequestForFirstSeqno(Address dest, Address original_dest) {
        if(last_sync_sent.addIfAbsentOrExpired(dest)) {
            Message msg=new EmptyMessage(dest).setFlag(OOB, NO_FC).setFlag(DONT_BLOCK)
              .putHeader(this.id, UnicastHeader.createSendFirstSeqnoHeader(timestamper.incrementAndGet()));
            if(!Objects.equals(local_addr, original_dest))
                msg.setSrc(original_dest);
            log.trace("%s --> %s: SEND_FIRST_SEQNO", local_addr, dest);
            down_prot.down(msg);
        }
    }

    public void sendClose(Address dest, short conn_id) {
        Message msg=new EmptyMessage(dest).putHeader(id, UnicastHeader.createCloseHeader(conn_id)).setFlag(NO_FC);
        log.trace("%s --> %s: CLOSE(conn-id=%d)", local_addr, dest, conn_id);
        down_prot.down(msg);
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
    public int removeExpiredConnections() {
        int num_removed=0;
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
                num_removed++;
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
                num_removed++;
            }
        }
        return num_removed;
    }

    /**
     * Removes send- and/or receive-connections whose state is not OPEN (CLOSING or CLOSED).
     * @param remove_send_connections If true, send connections whose state is !OPEN are destroyed and removed
     * @param remove_receive_connections If true, receive connections with state !OPEN are destroyed and removed
     * @return The number of connections which were removed
     */
    @ManagedOperation(description="Removes send- and/or receive-connections whose state is not OPEN (CLOSING or CLOSED)")
    public int removeConnections(boolean remove_send_connections, boolean remove_receive_connections) {
        int num_removed=0;
        if(remove_send_connections) {
            for(Map.Entry<Address,SenderEntry> entry: send_table.entrySet()) {
                SenderEntry val=entry.getValue();
                if(val.state() != State.OPEN) { // only look at closing or closed connections
                    log.debug("%s: removing connection for %s (%d ms old, state=%s) from send_table",
                              local_addr, entry.getKey(), val.age(), val.state());
                    removeSendConnection(entry.getKey());
                    num_removed++;
                }
            }
        }
        if(remove_receive_connections) {
            for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
                ReceiverEntry val=entry.getValue();
                if(val.state() != State.OPEN) { // only look at closing or closed connections
                    log.debug("%s: removing expired connection for %s (%d ms old, state=%s) from recv_table",
                              local_addr, entry.getKey(), val.age(), val.state());
                    removeReceiveConnection(entry.getKey());
                    num_removed++;
                }
            }
        }
        return num_removed;
    }

    @ManagedOperation(description="Triggers the retransmission task")
    public void triggerXmit() {
        // check for gaps in the received messages and ask senders to send them again
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address        target=entry.getKey(); // target to send retransmit requests to
            ReceiverEntry  val=entry.getValue();
            Buffer<Message> win=val != null? val.buf : null;

            // receiver: send ack for received messages if needed
            if(win != null && val.needToSendAck()) // sendAck() resets send_ack to false
                sendAck(target, val, val.realDest());

            if(xmits_enabled) {
                // receiver: retransmit missing messages (getNumMissing() is fast)
                SeqnoList missing;
                if(win != null && win.numMissing() > 0 && (missing=win.getMissing(max_xmit_req_size)) != null) {
                    long highest=missing.getLast();
                    Long prev_seqno=xmit_task_map.get(target);
                    if(prev_seqno == null)
                        xmit_task_map.put(target, highest); // no retransmission
                    else {
                        missing.removeHigherThan(prev_seqno); // we only retransmit the 'previous batch'
                        if(highest > prev_seqno)
                            xmit_task_map.put(target, highest);
                        if(!missing.isEmpty()) {
                            // remove msgs that are <= highest-delivered (https://issues.redhat.com/browse/JGRP-2574)
                            long highest_deliverable=win.getHighestDeliverable(), first=missing.getFirst();
                            if(first < highest_deliverable)
                                missing.removeLowerThan(highest_deliverable + 1);
                            retransmit(missing, target, val.real_dest);
                        }
                    }
                }
                else if(!xmit_task_map.isEmpty())
                    xmit_task_map.remove(target); // no current gaps for target
            }
        }

        if(xmits_enabled) {
            // resend sent messages until ack'ed
            // sender: only send the *highest sent* message if HA < HS and HA/HS didn't change from the prev run
            for(SenderEntry val : send_table.values()) {
                Buffer<Message> win=val != null? val.buf : null;
                if(win != null) {
                    long highest_acked=win.highestDelivered(); // highest delivered == highest ack (sender win)
                    long highest_sent=win.high();   // we use table as a *sender* win, so it's highest *sent*...

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
        }

        // close idle connections
        if(conn_expiry_timeout > 0)
            closeIdleConnections();

        if(conn_close_timeout > 0)
            removeExpiredConnections();
    }


    @ManagedOperation(description="Sends ACKs immediately for entries which are marked as pending (ACK hasn't been sent yet)")
    public void sendPendingAcks() {
        for(Map.Entry<Address,ReceiverEntry> entry: recv_table.entrySet()) {
            Address        target=entry.getKey(); // target to send retransmit requests to
            ReceiverEntry  val=entry.getValue();
            Buffer<Message> win=val != null? val.buf : null;

            // receiver: send ack for received messages if needed
            if(win != null && val.needToSendAck())// sendAck() resets send_ack to false
                sendAck(target, val, val.realDest());
        }
    }

    protected void update(Entry entry, int num_received) {
        if(conn_expiry_timeout > 0)
            entry.update();
        if(entry.state() == State.CLOSING)
            entry.state(State.OPEN);
        num_msgs_received.add(num_received);
    }

    /** Compares 2 timestamps, handles numeric overflow */
    protected static int compare(int ts1, int ts2) {
        int diff=ts1 - ts2;
        return Integer.compare(diff, 0);
    }

    @SafeVarargs
    protected static int accumulate(ToIntFunction<Buffer<Message>> func, Collection<? extends Entry> ... entries) {
        return Stream.of(entries).flatMap(Collection::stream)
          .map(entry -> entry.buf).filter(Objects::nonNull)
          .mapToInt(func).sum();
    }

    protected static short max(short a, short b) {return (a >= b) ? a : b;}

    protected static Tuple<Long,Boolean> getLowestSeqno(short prot_id, List<LongTuple<Message>> list) {
        long lowest_seqno=-1;
        boolean first=false;
        for(LongTuple<Message> tuple: list) {
            Message msg=tuple.getVal2();
            UnicastHeader hdr=msg.getHeader(prot_id);
            if(lowest_seqno < 0) {
                lowest_seqno=hdr.seqno;
                first=hdr.first;
            }
            else {
                if(lowest_seqno > hdr.seqno) {
                    lowest_seqno=hdr.seqno;
                    first=hdr.first;
                }
            }
        }
        return new Tuple<>(lowest_seqno, first);
    }

    protected enum State {OPEN, CLOSING, CLOSED}




    protected abstract class Entry {
        protected final Buffer<Message> buf; // stores sent or received messages
        protected final short           conn_id;
        protected final AtomicLong      timestamp=new AtomicLong(0); // ns
        protected volatile State        state=State.OPEN;
        protected final AtomicBoolean   send_ack=new AtomicBoolean();
        protected final AtomicInteger   acks_sent=new AtomicInteger();

        protected Entry(short conn_id, Buffer<Message> buf) {
            this.conn_id=conn_id;
            this.buf=Objects.requireNonNull(buf);
            update();
        }

        public    Buffer<Message> buf()           {return buf;}
        public    short           connId()        {return conn_id;}
        protected void            update()        {timestamp.set(getTimestamp());}
        protected long            age()           {return MILLISECONDS.convert(getTimestamp() - timestamp.longValue(), NANOSECONDS);}
        protected boolean         needToSendAck() {return send_ack.compareAndSet(true, false);}
        protected Entry           sendAck()       {send_ack.compareAndSet(false, true); return this;}
        protected State           state()         {return state;}

        protected Entry state(State s) {
            if(state != s) {
                switch(state) {
                    case OPEN:
                        if(s == State.CLOSED)
                            buf.open(false); // unblocks blocked senders
                        break;
                    case CLOSING:
                        buf.open(s != State.CLOSED);
                        break;
                    case CLOSED:
                        break;
                }
                state=s;
                update();
            }
            return this;
        }

        /** Returns true if a real ACK should be sent. This is based on num_acks_sent being > ack_threshold */
        public boolean update(int num_acks, final IntBinaryOperator op) {
            boolean should_send_ack=acks_sent.accumulateAndGet(num_acks, op) == 0;
            if(should_send_ack)
                return true;
            sendAck();
            return false;
        }
    }

    protected final class SenderEntry extends Entry {
        final AtomicLong seqno=new AtomicLong(DEFAULT_FIRST_SEQNO);   // seqno for msgs sent by us
        final long[]     watermark={0,0}; // the highest acked and highest sent seqno
        int              last_timestamp;  // to prevent out-of-order ACKs from a receiver

        public SenderEntry(short send_conn_id) {
            super(send_conn_id, createBuffer(0));
        }

        long[]      watermark()                 {return watermark;}
        SenderEntry watermark(long ha, long hs) {watermark[0]=ha; watermark[1]=hs; return this;}

        /** Updates last_timestamp. Returns true of the update was in order (ts > last_timestamp) */
        private synchronized boolean updateLastTimestamp(int ts) {
            if(last_timestamp == 0) {
                last_timestamp=ts;
                return true;
            }
            boolean success=compare(ts, last_timestamp) > 0; // ts has to be > last_timestamp
            if(success)
                last_timestamp=ts;
            return success;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(buf != null)
                sb.append(buf).append(", ");
            sb.append("send_conn_id=" + conn_id).append(" (" + age()/1000 + " secs old) - " + state);
            if(last_timestamp != 0)
                sb.append(", last-ts: ").append(last_timestamp);
            return sb.toString();
        }
    }

    // public for unit testing
    public final class ReceiverEntry extends Entry {
        private final Address real_dest ; // if real_dest != local_addr (https://issues.redhat.com/browse/JGRP-2729)

        public ReceiverEntry(Buffer<Message> received_msgs, short recv_conn_id, Address real_dest) {
            super(recv_conn_id, received_msgs);
            this.real_dest=real_dest;
        }

        Address realDest() {return real_dest;}

        public String toString() {
            StringBuilder sb=new StringBuilder();
            if(buf != null)
                sb.append(buf).append(", ");
            sb.append("recv_conn_id=" + conn_id).append(" (" + age() / 1000 + " secs old) - " + state);
            if(send_ack.get())
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
            return ReliableUnicast.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

}

package org.jgroups.protocols.pbcast;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.TP;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * Negative AcKnowledgement layer (NAKs). Messages are assigned a monotonically
 * increasing sequence number (seqno). Receivers deliver messages ordered
 * according to seqno and request retransmission of missing messages.<br/>
 * Retransmit requests are usually sent to the original sender of a message, but
 * this can be changed by xmit_from_random_member (send to random member) or
 * use_mcast_xmit_req (send to everyone). Responses can also be sent to everyone
 * instead of the requester by setting use_mcast_xmit to true.
 *
 * @author Bela Ban
 */
@MBean(description="Reliable transmission multipoint FIFO protocol")
public class NAKACK2 extends Protocol implements DiagnosticsHandler.ProbeHandler {
    protected static final int NUM_REBROADCAST_MSGS=3;

    /* -----------------------------------------------------    Properties     --------------------- ------------------------------------ */



    /**
     * Retransmit messages using multicast rather than unicast. This has the advantage that, if many receivers
     * lost a message, the sender only retransmits once
     */
    @Property(description="Retransmit retransmit responses (messages) using multicast rather than unicast")
    protected boolean use_mcast_xmit=true;

    /**
     * Use a multicast to request retransmission of missing messages. This may
     * be costly as every member in the cluster will send a response
     */
    @Property(description="Use a multicast to request retransmission of missing messages")
    protected boolean use_mcast_xmit_req=false;


    /**
     * Ask a random member for retransmission of a missing message. If set to
     * true, discard_delivered_msgs will be set to false
     */
    @Property(description="Ask a random member for retransmission of a missing message. Default is false")
    protected boolean xmit_from_random_member=false;


    /**
     * Messages that have been received in order are sent up the stack (= delivered to the application).
     * Delivered messages are removed from the retransmission buffer, so they can get GC'ed by the JVM. When this
     * property is true, everyone (except the sender of a message) removes the message from their retransission
     * buffers as soon as it has been delivered to the application
     */
    @Property(description="Should messages delivered to application be discarded")
    protected boolean discard_delivered_msgs=true;

    @Property(description="Timeout to rebroadcast messages. Default is 2000 msec")
    protected long    max_rebroadcast_timeout=2000;

    /** If true, logs messages discarded because received from other members */
    @Property(description="discards warnings about promiscuous traffic")
    protected boolean log_discard_msgs=true;

    @Property(description="If false, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Interval (in milliseconds) at which missing messages (from all retransmit buffers) " +
      "are retransmitted")
    protected long    xmit_interval=1000;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int     xmit_table_num_rows=100;

    @Property(description="Number of elements of a row of the matrix in the retransmission table; gets rounded to the " +
      "next power of 2 (only for experts). The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",
              writable=false)
    protected int     xmit_table_msgs_per_row=1024;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double  xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    protected long    xmit_table_max_compaction_time=10000;

    @Property(description="Size of the queue to hold messages received after creating the channel, but before being " +
      "connected (is_server=false). After becoming the server, the messages in the queue are fed into up() and the " +
      "queue is cleared. The motivation is to avoid retransmissions (see https://issues.jboss.org/browse/JGRP-1509 " +
      "for details). 0 disables the queue.")
    protected int     become_server_queue_size=50;

    @Property(description="Time during which identical warnings about messages from a non member will be suppressed. " +
      "0 disables this (every warning will be logged). Setting the log level to ERROR also disables this.")
    protected long    suppress_time_non_member_warnings=60000;

    @Property(description="Max number of messages to ask for in a retransmit request. 0 disables this and uses " +
      "the max bundle size in the transport")
    protected int     max_xmit_req_size;

    @Property(description="If enabled, multicasts the highest sent seqno every xmit_interval ms. This is skipped if " +
      "a regular message has been multicast, and the task aquiesces if the highest sent seqno hasn't changed for " +
      "resend_last_seqno_max_times times. Used to speed up retransmission of dropped last messages (JGRP-1904)")
    protected boolean resend_last_seqno=true;

    @Property(description="Max number of times the last seqno is resent before acquiescing if last seqno isn't incremented")
    protected int     resend_last_seqno_max_times=1;

    @ManagedAttribute(description="True if sending a message can block at the transport level")
    protected boolean sends_can_block=true;

    /* -------------------------------------------------- JMX ---------------------------------------------------------- */


    @ManagedAttribute(description="Number of messages sent")
    protected int                          num_messages_sent;

    @ManagedAttribute(description="Number of messages received")
    protected int                          num_messages_received;

    protected static final Message         DUMMY_OOB_MSG=new Message().setFlag(Message.Flag.OOB);

    // Accepts messages which are (1) non-null, (2) no DUMMY_OOB_MSGs and (3) not OOB_DELIVERED
    protected final Predicate<Message> no_dummy_and_no_oob_delivered_msgs_and_no_dont_loopback_msgs= msg ->
      msg != null && msg != DUMMY_OOB_MSG
        && (!msg.isFlagSet(Message.Flag.OOB) || msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
        && !(msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK) && this.local_addr != null && this.local_addr.equals(msg.getSrc()));

    protected static final Predicate<Message> dont_loopback_filter=msg -> msg != null && msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK);

    protected static final BiConsumer<MessageBatch,Message> BATCH_ACCUMULATOR=MessageBatch::add;


    @ManagedAttribute(description="Number of retransmit requests received")
    protected final LongAdder xmit_reqs_received=new LongAdder();

    @ManagedAttribute(description="Number of retransmit requests sent")
    protected final LongAdder xmit_reqs_sent=new LongAdder();

    @ManagedAttribute(description="Number of retransmit responses received")
    protected final LongAdder xmit_rsps_received=new LongAdder();

    @ManagedAttribute(description="Number of retransmit responses sent")
    protected final LongAdder xmit_rsps_sent=new LongAdder();

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

    @ManagedAttribute(description="Number of messages from non-members")
    public int getNonMemberMessages() {
        return suppress_log_non_member != null? suppress_log_non_member.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for messages from non-members")
    public void clearNonMemberCache() {
        if(suppress_log_non_member != null)
            suppress_log_non_member.getCache().clear();
    }

    @ManagedAttribute
    public void setResendLastSeqno(boolean flag) {
        if(resend_last_seqno != flag)
            resend_last_seqno=flag;
        if(resend_last_seqno) {
            if(last_seqno_resender == null)
                last_seqno_resender=new LastSeqnoResender();
        }
        else {
            if(last_seqno_resender != null)
                last_seqno_resender=null;
        }
    }

    public NAKACK2 resendLastSeqno(boolean flag) {setResendLastSeqno(flag); return this;}

    @ManagedAttribute(description="Whether or not the task to resend the last seqno is running (depends on resend_last_seqno)")
    public boolean resendTaskRunning() {return last_seqno_resender != null;}

    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean is_trace=log.isTraceEnabled();

    /* -------------------------------------------------    Fields    ------------------------------------------------------------------------- */
    protected volatile boolean          is_server;
    protected Address                   local_addr;
    protected volatile List<Address>    members=new ArrayList<>();
    protected volatile View             view;
    private final AtomicLong            seqno=new AtomicLong(0); // current message sequence number (starts with 1)

    /** Map to store sent and received messages (keyed by sender) */
    protected final ConcurrentMap<Address,Table<Message>> xmit_table=Util.createConcurrentMap();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                 xmit_task;
    /** Used by the retransmit task to keep the last retransmitted seqno per sender (https://issues.jboss.org/browse/JGRP-1539) */
    protected final Map<Address,Long>   xmit_task_map=new ConcurrentHashMap<>();

    protected volatile boolean          leaving=false;
    protected volatile boolean          running=false;
    protected TimeScheduler             timer=null;
    protected LastSeqnoResender         last_seqno_resender;
    protected final Lock                rebroadcast_lock=new ReentrantLock();
    protected final Condition           rebroadcast_done=rebroadcast_lock.newCondition();

    // set during processing of a rebroadcast event
    protected volatile boolean          rebroadcasting=false;
    protected final Lock                rebroadcast_digest_lock=new ReentrantLock();
    @GuardedBy("rebroadcast_digest_lock")
    protected Digest                    rebroadcast_digest=null;

    /** Keeps the last N stability messages */
    protected final BoundedList<String> stability_msgs=new BoundedList<>(10);

    /** Keeps a bounded list of the last N digest sets */
    protected final BoundedList<String> digest_history=new BoundedList<>(10);

    protected BoundedList<Message>      become_server_queue;

     /** Log to suppress identical warnings for messages from non-members */
    protected SuppressLog<Address>      suppress_log_non_member;


    public long    getXmitRequestsReceived()               {return xmit_reqs_received.sum();}
    public long    getXmitRequestsSent()                   {return xmit_reqs_sent.sum();}
    public long    getXmitResponsesReceived()              {return xmit_rsps_received.sum();}
    public long    getXmitResponsesSent()                  {return xmit_rsps_sent.sum();}
    public boolean isUseMcastXmit()                        {return use_mcast_xmit;}
    public boolean isXmitFromRandomMember()                {return xmit_from_random_member;}
    public boolean isDiscardDeliveredMsgs()                {return discard_delivered_msgs;}
    public boolean getLogDiscardMessages()                 {return log_discard_msgs;}
    public NAKACK2 setUseMcastXmit(boolean use_mcast_xmit) {this.use_mcast_xmit=use_mcast_xmit; return this;}
    public NAKACK2 setUseMcastXmitReq(boolean flag)        {this.use_mcast_xmit_req=flag; return this;}
    public NAKACK2 setLogDiscardMessages(boolean flag)     {log_discard_msgs=flag; return this;}
    public NAKACK2 setLogNotFoundMessages(boolean flag)    {log_not_found_msgs=flag; return this;}
    public NAKACK2 setResendLastSeqnoMaxTimes(int n)       {this.resend_last_seqno_max_times=n; return this;}

    public NAKACK2 setXmitFromRandomMember(boolean xmit_from_random_member) {
        this.xmit_from_random_member=xmit_from_random_member; return this;
    }
    public void    setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        this.discard_delivered_msgs=discard_delivered_msgs;
    }

    public <T extends Protocol> T setLevel(String level) {
        T retval=super.setLevel(level);
        is_trace=log.isTraceEnabled();
        return retval;
    }

    @ManagedAttribute(description="Actual size of the become_server_queue")
    public int getBecomeServerQueueSizeActual() {
        return become_server_queue != null? become_server_queue.size() : -1;
    }

    /** Returns the receive window for sender; only used for testing. Do not use ! */
    public Table<Message> getWindow(Address sender) {
        return xmit_table.get(sender);
    }


    /** Only used for unit tests, don't use ! */
    public void setTimer(TimeScheduler timer) {this.timer=timer;}
    

    @ManagedAttribute(description="Total number of undelivered messages in all retransmit buffers")
    public int getXmitTableUndeliveredMsgs() {
        int num=0;
        for(Table<Message> buf: xmit_table.values())
            num+=buf.size();
        return num;
    }

    @ManagedAttribute(description="Total number of missing (= not received) messages in all retransmit buffers")
    public int getXmitTableMissingMessages() {
        int num=0;
        for(Table<Message> buf: xmit_table.values())
            num+=buf.getNumMissing();
        return num;
    }

    @ManagedAttribute(description="Capacity of the retransmit buffer. Computed as xmit_table_num_rows * xmit_table_msgs_per_row")
    public long getXmitTableCapacity() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.capacity() : 0;
    }

    @ManagedAttribute(description="Prints the number of rows currently allocated in the matrix. This value will not " +
      "be lower than xmit_table_now_rows")
    public int getXmitTableNumCurrentRows() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.getNumRows() : 0;
    }

    @ManagedAttribute(description="Returns the number of bytes of all messages in all retransmit buffers. " +
      "To compute the size, Message.getLength() is used")
    public long getSizeOfAllMessages() {
        long retval=0;
        for(Table<Message> buf: xmit_table.values())
            retval+=sizeOfAllMessages(buf,false);
        return retval;
    }

    @ManagedAttribute(description="Returns the number of bytes of all messages in all retransmit buffers. " +
      "To compute the size, Message.size() is used")
    public long getSizeOfAllMessagesInclHeaders() {
        long retval=0;
        for(Table<Message> buf: xmit_table.values())
            retval+=sizeOfAllMessages(buf, true);
        return retval;
    }

    @ManagedAttribute(description="Number of retransmit table compactions")
    public int getXmitTableNumCompactions() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.getNumCompactions() : 0;
    }

    @ManagedAttribute(description="Number of retransmit table moves")
    public int getXmitTableNumMoves() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.getNumMoves() : 0;
    }

    @ManagedAttribute(description="Number of retransmit table resizes")
    public int getXmitTableNumResizes() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.getNumResizes(): 0;
    }

    @ManagedAttribute(description="Number of retransmit table purges")
    public int getXmitTableNumPurges() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        return table != null? table.getNumPurges(): 0;
    }

    @ManagedOperation(description="Prints the contents of the receiver windows for all members")
    public String printMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,Table<Message>> entry: xmit_table.entrySet()) {
            Address addr=entry.getKey();
            Table<Message> buf=entry.getValue();
            ret.append(addr).append(": ").append(buf.toString()).append('\n');
        }
        return ret.toString();
    }

    @ManagedAttribute public long getCurrentSeqno() {return seqno.get();}

    @ManagedOperation(description="Prints the stability messages received")
    public String printStabilityMessages() {
        return Util.printListWithDelimiter(stability_msgs, "\n");
    }

    @ManagedOperation(description="Keeps information about the last N times a digest was set or merged")
    public String printDigestHistory() {
        StringBuilder sb=new StringBuilder(local_addr + ":\n");
        for(String tmp: digest_history)
            sb.append(tmp).append("\n");
        return sb.toString();
    }

    @ManagedOperation(description="Compacts the retransmit buffer")
    public void compact() {
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        if(table != null)
            table.compact();
    }

    @ManagedOperation(description="Prints the number of rows currently allocated in the matrix for all members. " +
      "This value will not be lower than xmit_table_now_rows")
    public String dumpXmitTablesNumCurrentRows() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Table<Message>> entry: xmit_table.entrySet())
            sb.append(String.format("%s: %d\n", entry.getKey(), entry.getValue().getNumRows()));
        return sb.toString();
    }

    @ManagedOperation(description="Resets all statistics")
    public void resetStats() {
        num_messages_sent=num_messages_received=0;
        xmit_reqs_received.reset();
        xmit_reqs_sent.reset();
        xmit_rsps_received.reset();
        xmit_rsps_sent.reset();
        stability_msgs.clear();
        digest_history.clear();
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        if(table != null)
            table.resetStats();
    }

    public void init() throws Exception {
        if(xmit_from_random_member && discard_delivered_msgs) {
            discard_delivered_msgs=false;
            log.debug("%s: xmit_from_random_member set to true: changed discard_delivered_msgs to false", local_addr);
        }

        TP transport=getTransport();
        sends_can_block=transport instanceof TCP; // UDP and TCP_NIO2 won't block
        transport.registerProbeHandler(this);
        if(!transport.supportsMulticasting()) {
            if(use_mcast_xmit) {
                log.debug(Util.getMessage("NoMulticastTransport"), "use_mcast_xmit", transport.getName(), "use_mcast_xmit");
                use_mcast_xmit=false;
            }
            if(use_mcast_xmit_req) {
                log.debug(Util.getMessage("NoMulticastTransport"), "use_mcast_xmit_req", transport.getName(), "use_mcast_xmit_req");
                use_mcast_xmit_req=false;
            }
        }

        if(become_server_queue_size > 0)
            become_server_queue=new BoundedList<>(become_server_queue_size);

        if(suppress_time_non_member_warnings > 0)
            suppress_log_non_member=new SuppressLog<>(log, "MsgDroppedNak", "SuppressMsg");

        // max bundle size (minus overhead) divided by <long size> times bits per long
        int estimated_max_msgs_in_xmit_req=(transport.getMaxBundleSize() -50) * Global.LONG_SIZE;
        int old_max_xmit_size=max_xmit_req_size;
        if(max_xmit_req_size <= 0)
            max_xmit_req_size=estimated_max_msgs_in_xmit_req;
        else
            max_xmit_req_size=Math.min(max_xmit_req_size, estimated_max_msgs_in_xmit_req);
        if(old_max_xmit_size != max_xmit_req_size)
            log.trace("%s: set max_xmit_req_size from %d to %d", local_addr, old_max_xmit_size, max_xmit_req_size);

        if(resend_last_seqno)
            setResendLastSeqno(resend_last_seqno);
    }



    public List<Integer> providedUpServices() {
        return Arrays.asList(Event.GET_DIGEST,Event.SET_DIGEST,Event.OVERWRITE_DIGEST,Event.MERGE_DIGEST);
    }


    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        running=true;
        leaving=false;
        startRetransmitTask();
    }


    public void stop() {
        running=false;
        is_server=false;
        if(become_server_queue != null)
            become_server_queue.clear();
        stopRetransmitTask();
        xmit_task_map.clear();
        reset();
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use {@code down_prot.down()} in this
     * method as the event is passed down by default by the superclass after this method returns !</b>
     */
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable(evt.getArg());
                return null;  // do not pass down further (Bela Aug 7 2001)

            case Event.GET_DIGEST:
                return getDigest(evt.getArg());

            case Event.SET_DIGEST:
                setDigest(evt.getArg());
                return null;

            case Event.OVERWRITE_DIGEST:
                overwriteDigest(evt.getArg());
                return null;

            case Event.MERGE_DIGEST:
                mergeDigest(evt.getArg());
                return null;

            case Event.TMP_VIEW:
                View tmp_view=evt.getArg();
                members=tmp_view.getMembers();
                break;

            case Event.VIEW_CHANGE:
                tmp_view=evt.getArg();
                List<Address> mbrs=tmp_view.getMembers();
                members=mbrs;
                view=tmp_view;
                adjustReceivers(mbrs);
                is_server=true;  // check vids from now on
                if(suppress_log_non_member != null)
                    suppress_log_non_member.removeExpired(suppress_time_non_member_warnings);
                xmit_task_map.keySet().retainAll(mbrs);
                break;

            case Event.BECOME_SERVER:
                is_server=true;
                flushBecomeServerQueue();
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;

            case Event.DISCONNECT:
                leaving=true;
                reset();
                break;

            case Event.REBROADCAST:
                rebroadcasting=true;
                rebroadcast_digest=evt.getArg();
                try {
                    rebroadcastMessages();
                }
                finally {
                    rebroadcasting=false;
                    rebroadcast_digest_lock.lock();
                    try {
                        rebroadcast_digest=null;
                    }
                    finally {
                        rebroadcast_digest_lock.unlock();
                    }
                }
                return null;
        }

        return down_prot.down(evt);
    }

    public Object down(Message msg) {
        Address dest=msg.getDest();
        if(dest != null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
            return down_prot.down(msg); // unicast address: not null and not mcast, pass down unchanged
        send(msg);
        return null;    // don't pass down the stack
    }

    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use {@code passUp} in this
     * method as the event is passed up by default by the superclass after this method returns !</b>
     */
    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable(evt.getArg());
                return null;  // do not pass up further (Bela Aug 7 2001)

            case Event.SUSPECT:
                // release the promise if rebroadcasting is in progress... otherwise we wait forever. there will be a new
                // flush round anyway
                if(rebroadcasting)
                    cancelRebroadcasting();
                break;
        }
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        if(msg.isFlagSet(Message.Flag.NO_RELIABILITY))
            return up_prot.up(msg);
        NakAckHeader2 hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);  // pass up (e.g. unicast msg)

        if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
            queueMessage(msg, hdr.seqno);
            return null;
        }

        switch(hdr.type) {

            case NakAckHeader2.MSG:
                handleMessage(msg, hdr);
                return null;        // transmitter passes message up for us !

            case NakAckHeader2.XMIT_REQ:
                try {
                    SeqnoList missing=Util.streamableFromBuffer(SeqnoList::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                    if(missing != null)
                        handleXmitReq(msg.getSrc(), missing, hdr.sender);
                }
                catch(Exception e) {
                    log.error("failed deserializing retransmission list", e);
                }
                return null;

            case NakAckHeader2.XMIT_RSP:
                handleXmitRsp(msg, hdr);
                return null;

            case NakAckHeader2.HIGHEST_SEQNO:
                handleHighestSeqno(msg.src(), hdr.seqno);
                return null;

            default:
                log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
                return null;
        }
    }

    public void up(MessageBatch batch) {
        int                       size=batch.size();
        boolean                   got_retransmitted_msg=false; // if at least 1 XMIT-RSP was received
        List<LongTuple<Message>>  msgs=null;      // regular or retransmitted messages

        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            final Message msg=it.next();
            NakAckHeader2 hdr;
            if(msg == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY) || (hdr=msg.getHeader(id)) == null)
                continue;
            it.remove(); // remove the message from the batch, so it won't be passed up the stack

            if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
                queueMessage(msg, hdr.seqno);
                continue;
            }

            switch(hdr.type) {
                case NakAckHeader2.MSG:
                    if(msgs == null)
                        msgs=new ArrayList<>(size);
                    msgs.add(new LongTuple<>(hdr.seqno, msg));
                    break;
                case NakAckHeader2.XMIT_REQ:
                    try {
                        SeqnoList missing=Util.streamableFromBuffer(SeqnoList::new, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                        if(missing != null)
                            handleXmitReq(msg.getSrc(), missing, hdr.sender);
                    }
                    catch(Exception e) {
                        log.error("failed deserializing retransmission list", e);
                    }
                    break;
                case NakAckHeader2.XMIT_RSP:
                    Message xmitted_msg=msgFromXmitRsp(msg, hdr);
                    if(xmitted_msg != null) {
                        if(msgs == null)
                            msgs=new ArrayList<>(size);
                        msgs.add(new LongTuple<>(hdr.seqno, xmitted_msg));
                        got_retransmitted_msg=true;
                    }
                    break;
                case NakAckHeader2.HIGHEST_SEQNO:
                    handleHighestSeqno(batch.sender(), hdr.seqno);
                    break;
                default:
                    log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
            }
        }

        // Process (new and retransmitted) messages:
        if(msgs != null)
            handleMessages(batch.dest(), batch.sender(), msgs, batch.mode() == MessageBatch.Mode.OOB, batch.clusterName());

        // received XMIT-RSPs:
        if(got_retransmitted_msg && rebroadcasting)
            checkForRebroadcasts();

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    // ProbeHandler interface
    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> retval=new HashMap<>();
        for(String key: keys) {
            switch(key) {
                case "digest-history":
                    retval.put(key, printDigestHistory());
                    break;
                case "dump-digest":
                    retval.put(key, "\n" + printMessages());
                    break;
            }
        }
        return retval;
    }

    // ProbeHandler interface
    public String[] supportedKeys() {
        return new String[]{"digest-history", "dump-digest"};
    }




    /* --------------------------------- Private Methods --------------------------------------- */

    protected void queueMessage(Message msg, long seqno) {
        if(become_server_queue != null) {
            become_server_queue.add(msg);
            log.trace("%s: message %s#%d was added to queue (not yet server)", local_addr, msg.getSrc(), seqno);
        }
        else
            log.trace("%s: message %s#%d was discarded (not yet server)", local_addr, msg.getSrc(), seqno);
    }

    protected void unknownMember(Address sender, Object message) {
        if(leaving)
            return;
        if(log_discard_msgs && log.isWarnEnabled()) {
            if(suppress_log_non_member != null)
                suppress_log_non_member.log(SuppressLog.Level.warn, sender, suppress_time_non_member_warnings,
                                            local_addr, message, sender, view);
            else
                log.warn(Util.getMessage("MsgDroppedNak"), local_addr, message, sender, view);
        }
    }

    /**
     * Adds the message to the sent_msgs table and then passes it down the stack. Change Bela Ban May 26 2002: we don't
     * store a copy of the message, but a reference ! This saves us a lot of memory. However, this also means that a
     * message should not be changed after storing it in the sent-table ! See protocols/DESIGN for details.
     * Made seqno increment and adding to sent_msgs atomic, e.g. seqno won't get incremented if adding to
     * sent_msgs fails e.g. due to an OOM (see http://jira.jboss.com/jira/browse/JGRP-179). bela Jan 13 2006
     */
    protected void send(Message msg) {
        if(!running) {
            log.trace("%s: discarded message as we're not in the 'running' state, message: %s", local_addr, msg);
            return;
        }

        long msg_id;
        Table<Message> buf=xmit_table.get(local_addr);
        if(buf == null) // discard message if there is no entry for local_addr
            return;

        if(msg.src() == null)
            msg.src(local_addr); // this needs to be done so we can check whether the message sender is the local_addr

        boolean dont_loopback_set=msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK);
        msg_id=seqno.incrementAndGet();
        long sleep=10;
        do {
            try {
                msg.putHeader(this.id, NakAckHeader2.createMessageHeader(msg_id));
                buf.add(msg_id, msg, dont_loopback_set? dont_loopback_filter : null);
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

        // moved down_prot.down() out of synchronized clause (bela Sept 7 2006) http://jira.jboss.com/jira/browse/JGRP-300
        if(is_trace)
            log.trace("%s --> [all]: #%d", local_addr, msg_id);
        down_prot.down(msg); // if this fails, since msg is in sent_msgs, it can be retransmitted
        num_messages_sent++;

        if(resend_last_seqno && last_seqno_resender != null)
            last_seqno_resender.skipNext();
    }



    /**
     * Finds the corresponding retransmit buffer and adds the message to it (according to seqno). Then removes as many
     * messages as possible and passes them up the stack. Discards messages from non-members.
     */
    protected void handleMessage(Message msg, NakAckHeader2 hdr) {
        Address sender=msg.getSrc();
        Table<Message> buf=xmit_table.get(sender);
        if(buf == null) {  // discard message if there is no entry for sender
            unknownMember(sender, hdr.seqno);
            return;
        }

        num_messages_received++;
        boolean loopback=local_addr.equals(sender);

        // If the message was sent by myself, then it is already in the table and we don't need to add it. If not,
        // and the message is OOB, insert a dummy message (same msg, saving space), deliver it and drop it later on
        // removal. Else insert the real message
        boolean added=loopback || buf.add(hdr.seqno, msg.isFlagSet(Message.Flag.OOB)? DUMMY_OOB_MSG : msg);

        //if(added && is_trace)
          //  log.trace("%s <-- %s: #%d", local_addr, sender, hdr.seqno);

        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && msg.isFlagSet(Message.Flag.OOB)) {
            if(loopback) { // sent by self
                msg=buf.get(hdr.seqno); // we *have* to get a message, because loopback means we didn't add it to win !
                if(msg != null && msg.isFlagSet(Message.Flag.OOB) && msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
                    deliver(msg, sender, hdr.seqno, "OOB message");
            }
            else // sent by someone else
                deliver(msg, sender, hdr.seqno, "OOB message");
        }

        removeAndDeliver(buf, sender, loopback, null); // at most 1 thread will execute this at any given time
    }


    protected void handleMessages(Address dest, Address sender, List<LongTuple<Message>> msgs, boolean oob, AsciiString cluster_name) {
        Table<Message> buf=xmit_table.get(sender);
        if(buf == null) {  // discard message if there is no entry for sender
            unknownMember(sender, "batch");
            return;
        }
        num_messages_received+= msgs.size();
        boolean loopback=local_addr.equals(sender);
        boolean added=loopback || buf.add(msgs, oob, oob? DUMMY_OOB_MSG : null);

        //if(added && is_trace)
          //  log.trace("%s <-- %s: #%d-%d (%d messages)",
            //          local_addr, sender, msgs.get(0).getVal1(), msgs.get(msgs.size() -1).getVal1(), msgs.size());

        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && oob) {
            MessageBatch oob_batch=new MessageBatch(dest, sender, null, dest == null, MessageBatch.Mode.OOB, msgs.size());
            if(loopback) {
                for(LongTuple<Message> tuple: msgs) {
                    long    seq=tuple.getVal1();
                    Message msg=buf.get(seq); // we *have* to get the message, because loopback means we didn't add it to win !
                    if(msg != null && msg.isFlagSet(Message.Flag.OOB) && msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED))
                        oob_batch.add(msg);
                }
            }
            else {
                for(LongTuple<Message> tuple: msgs)
                    oob_batch.add(tuple.getVal2());
            }
            deliverBatch(oob_batch);
        }

        removeAndDeliver(buf, sender, loopback, cluster_name); // at most 1 thread will execute this at any given time
    }


    /** Efficient way of checking whether another thread is already processing messages from sender. If that's the case,
     *  we return immediately and let the existing thread process our message (https://jira.jboss.org/jira/browse/JGRP-829).
     *  Benefit: fewer threads blocked on the same lock, these threads can be returned to the thread pool
     */
    protected void removeAndDeliver(Table<Message> buf, Address sender, boolean loopback, AsciiString cluster_name) {
        AtomicInteger adders=buf.getAdders();
        if(adders.getAndIncrement() != 0)
            return;
        boolean remove_msgs=discard_delivered_msgs && !loopback;
        MessageBatch batch=new MessageBatch(buf.size()).dest(null).sender(sender).clusterName(cluster_name).multicast(true);
        Supplier<MessageBatch> batch_creator=() -> batch;
        do {
            try {
                batch.reset();
                // Don't include DUMMY and OOB_DELIVERED messages in the removed set
                buf.removeMany(remove_msgs, 0, no_dummy_and_no_oob_delivered_msgs_and_no_dont_loopback_msgs,
                               batch_creator, BATCH_ACCUMULATOR);
            }
            catch(Throwable t) {
                log.error("failed removing messages from table for " + sender, t);
            }
            if(!batch.isEmpty())
                deliverBatch(batch);
        }
        while(adders.decrementAndGet() != 0);
        if(rebroadcasting)
            checkForRebroadcasts();
    }



    /**
     * Retransmits messsages first_seqno to last_seqno from original_sender from xmit_table to xmit_requester,
     * called when XMIT_REQ is received.
     * @param xmit_requester The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param missing_msgs A list of seqnos that have to be retransmitted
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    protected void handleXmitReq(Address xmit_requester, SeqnoList missing_msgs, Address original_sender) {
        log.trace("%s <-- %s: XMIT(%s%s)", local_addr, xmit_requester, original_sender, missing_msgs);

        if(stats)
            xmit_reqs_received.add(missing_msgs.size());

        Table<Message> buf=xmit_table.get(original_sender);
        if(buf == null) {
            log.error(Util.getMessage("SenderNotFound"), local_addr, original_sender);
            return;
        }

        for(long i: missing_msgs) {
            Message msg=buf.get(i);
            if(msg == null) {
                if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(xmit_requester) && i > buf.getLow())
                    log.warn(Util.getMessage("MessageNotFound"), local_addr, original_sender, i);
                continue;
            }
            if(is_trace)
                log.trace("%s --> [all]: resending %s#%d", local_addr, original_sender, i);
            sendXmitRsp(xmit_requester, msg);
        }
    }

    protected void deliver(Message msg, Address sender, long seqno, String error_msg) {
        if(is_trace)
            log.trace("%s <-- %s: #%d", local_addr, sender, seqno);
        try {
            up_prot.up(msg);
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, error_msg, msg, t);
        }
    }

    protected void deliverBatch(MessageBatch batch) {
        try {
            if(batch == null || batch.isEmpty())
                return;
            if(is_trace) {
                Message first=batch.first(), last=batch.last();
                StringBuilder sb=new StringBuilder(local_addr + " <-- " + batch.sender() + ": ");
                if(first != null && last != null) {
                    NakAckHeader2 hdr1=first.getHeader(id), hdr2=last.getHeader(id);
                    sb.append("#").append(hdr1.seqno).append("-").append(hdr2.seqno);
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


    /**
     * Flushes the queue. Done in a separate thread as we don't want to block the
     * {@link ClientGmsImpl#installView(org.jgroups.View,org.jgroups.util.Digest)} method (called when a view is installed).
     */
    protected void flushBecomeServerQueue() {
        if(become_server_queue != null && !become_server_queue.isEmpty()) {
            log.trace("%s: flushing become_server_queue (%d elements)", local_addr, become_server_queue.size());

            TP transport=getTransport();
            for(final Message msg: become_server_queue) {
                transport.submitToThreadPool(() -> {
                    try {
                        up(msg);
                    }
                    finally {
                        become_server_queue.remove(msg);
                    }
                }, true);
            }
        }
    }


    protected void cancelRebroadcasting() {
        rebroadcast_lock.lock();
        try {
            rebroadcasting=false;
            rebroadcast_done.signalAll();
        }
        finally {
            rebroadcast_lock.unlock();
        }
    }




    /**
     * Sends a message msg to the requester. We have to wrap the original message into a retransmit message, as we need
     * to preserve the original message's properties, such as src, headers etc.
     * @param dest
     * @param msg
     */
    protected void sendXmitRsp(Address dest, Message msg) {
        if(msg == null)
            return;

        if(stats)
            xmit_rsps_sent.increment();

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        if(use_mcast_xmit) { // we simply send the original multicast message
            down_prot.down(msg);
            return;
        }

        Message xmit_msg=msg.copy(true, true).dest(dest); // copy payload and headers
        NakAckHeader2 hdr=xmit_msg.getHeader(id);
        NakAckHeader2 newhdr=hdr.copy();
        newhdr.type=NakAckHeader2.XMIT_RSP; // change the type in the copy from MSG --> XMIT_RSP
        xmit_msg.putHeader(id, newhdr);
        down_prot.down(xmit_msg);
    }


    protected void handleXmitRsp(Message msg, NakAckHeader2 hdr) {
        if(msg == null)
            return;

        try {
            if(stats)
                xmit_rsps_received.increment();

            msg.setDest(null);
            NakAckHeader2 newhdr=hdr.copy();
            newhdr.type=NakAckHeader2.MSG; // change the type back from XMIT_RSP --> MSG
            msg.putHeader(id, newhdr);
            handleMessage(msg, newhdr);
            if(rebroadcasting)
                checkForRebroadcasts();
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "retransmitted message", msg, ex);
        }
    }

    /**
     * Compares the sender's highest seqno with my highest seqno: if the sender's is higher, ask sender for retransmission
     * @param sender The sender
     * @param seqno The highest seqno sent by sender
     */
    protected void handleHighestSeqno(Address sender, long seqno) {
        // check whether the highest seqno received from sender is > highest seqno received for sender in my digest.
        // If yes, request retransmission (see "Last Message Dropped" topic in DESIGN)
        Table<Message> buf=xmit_table.get(sender);
        if(buf == null)
            return;
        long my_highest_received=buf.getHighestReceived();

        if(my_highest_received >= 0 && seqno > my_highest_received) {
            log.trace("%s: my_highest_rcvd (%s#%d) < highest received (%s#%d): requesting retransmission",
                      local_addr, sender, my_highest_received, sender, seqno);
            retransmit(seqno,seqno,sender);
        }
    }

    protected Message msgFromXmitRsp(Message msg, NakAckHeader2 hdr) {
        if(msg == null)
            return null;

        if(stats)
            xmit_rsps_received.increment();

        msg.setDest(null);
        NakAckHeader2 newhdr=hdr.copy();
        newhdr.type=NakAckHeader2.MSG; // change the type back from XMIT_RSP --> MSG
        msg.putHeader(id,newhdr);
        return msg;
    }


    /**
     * Takes the argument highest_seqnos and compares it to the current digest. If the current digest has fewer messages,
     * then send retransmit messages for the missing messages. Return when all missing messages have been received. If
     * we're waiting for a missing message from P, and P crashes while waiting, we need to exclude P from the wait set.
     */
    protected void rebroadcastMessages() {
        Digest their_digest;
        long sleep=max_rebroadcast_timeout / NUM_REBROADCAST_MSGS;
        long wait_time=max_rebroadcast_timeout, start=System.currentTimeMillis();

        while(wait_time > 0) {
            rebroadcast_digest_lock.lock();
            try {
                if(rebroadcast_digest == null)
                    break;
                their_digest=rebroadcast_digest.copy();
            }
            finally {
                rebroadcast_digest_lock.unlock();
            }
            Digest my_digest=getDigest();
            boolean xmitted=false;

            for(Digest.Entry entry: their_digest) {
                Address member=entry.getMember();
                long[] my_entry=my_digest.get(member);
                if(my_entry == null)
                    continue;
                long their_high=entry.getHighest();

                // Cannot ask for 0 to be retransmitted because the first seqno in NAKACK2 and UNICAST(2) is always 1 !
                // Also, we need to ask for retransmission of my_high+1, because we already *have* my_high, and don't
                // need it, so the retransmission range is [my_high+1 .. their_high]: *exclude* my_high, but *include*
                // their_high
                long my_high=Math.max(my_entry[0], my_entry[1]);
                if(their_high > my_high) {
                    log.trace("%s: fetching %d-%d from %s", local_addr, my_high, their_high, member);
                    retransmit(my_high+1, their_high, member, true); // use multicast to send retransmit request
                    xmitted=true;
                }
            }
            if(!xmitted)
                return; // we're done; no retransmissions are needed anymore. our digest is >= rebroadcast_digest

            rebroadcast_lock.lock();
            try {
                try {
                    my_digest=getDigest();
                    rebroadcast_digest_lock.lock();
                    try {
                        if(!rebroadcasting || isGreaterThanOrEqual(my_digest, rebroadcast_digest))
                            return;
                    }
                    finally {
                        rebroadcast_digest_lock.unlock();
                    }
                    rebroadcast_done.await(sleep, TimeUnit.MILLISECONDS);
                    wait_time-=(System.currentTimeMillis() - start);
                }
                catch(InterruptedException e) {
                }
            }
            finally {
                rebroadcast_lock.unlock();
            }
        }
    }

    protected void checkForRebroadcasts() {
        Digest tmp=getDigest();
        boolean cancel_rebroadcasting=false;
        rebroadcast_digest_lock.lock();
        try {
            cancel_rebroadcasting=isGreaterThanOrEqual(tmp, rebroadcast_digest);
        }
        catch(Throwable t) {
            ;
        }
        finally {
            rebroadcast_digest_lock.unlock();
        }
        if(cancel_rebroadcasting)
            cancelRebroadcasting();
    }

    /**
     * Returns true if all senders of the current digest have their seqnos >= the ones from other
     */
    protected static boolean isGreaterThanOrEqual(Digest first, Digest other) {
        if(other == null)
            return true;

        for(Digest.Entry entry: first) {
            Address sender=entry.getMember();
            long[] their_entry=other.get(sender);
            if(their_entry == null)
                continue;
            long my_highest=entry.getHighest();
            long their_highest=Math.max(their_entry[0],their_entry[1]);
            if(my_highest < their_highest)
                return false;
        }
        return true;
    }


    /**
     * Removes old members from xmit-table and adds new members to xmit-table (at seqnos hd=0, hr=0).
     * This method is not called concurrently
     */
    protected void adjustReceivers(List<Address> members) {
        Set<Address> keys=xmit_table.keySet();

        // remove members which left
        for(Address member: keys) {
            if(!members.contains(member)) {
                if(Objects.equals(local_addr, member))
                    continue;
                Table<Message> buf=xmit_table.remove(member);
                if(buf != null)
                    log.debug("%s: removed %s from xmit_table (not member anymore)", local_addr, member);
            }
        }
        members.stream().filter(mbr -> !keys.contains(mbr)).forEach(mbr -> xmit_table.putIfAbsent(mbr, createTable(0)));
    }


    /**
     * Returns a message digest: for each member P the highest delivered and received seqno is added
     */
    public Digest getDigest() {
        final Map<Address,long[]> map=new HashMap<>();
        for(Map.Entry<Address,Table<Message>> entry: xmit_table.entrySet()) {
            Address sender=entry.getKey(); // guaranteed to be non-null (CCHM)
            Table<Message> buf=entry.getValue(); // guaranteed to be non-null (CCHM)
            long[] seqnos=buf.getDigest();
            map.put(sender, seqnos);
        }
        return new Digest(map);
    }


    public Digest getDigest(Address mbr) {
        if(mbr == null)
            return getDigest();
        Table<Message> buf=xmit_table.get(mbr);
        if(buf == null)
            return null;
        long[] seqnos=buf.getDigest();
        return new Digest(mbr, seqnos[0], seqnos[1]);
    }


    /**
     * Creates a retransmit buffer for each sender in the digest according to the sender's seqno.
     * If a buffer already exists, it resets it.
     */
    protected void setDigest(Digest digest) {
        setDigest(digest,false);
    }


    /**
     * For all members of the digest, adjust the retransmit buffers in xmit_table. If no entry
     * exists, create one with the initial seqno set to the seqno of the member in the digest. If the member already
     * exists, and is not the local address, replace it with the new entry (http://jira.jboss.com/jira/browse/JGRP-699)
     * if the digest's seqno is greater than the seqno in the window.
     */
    protected void mergeDigest(Digest digest) {
        setDigest(digest,true);
    }

    /**
     * Overwrites existing entries, but does NOT remove entries not found in the digest
     * @param digest
     */
    protected void overwriteDigest(Digest digest) {
        if(digest == null)
            return;

        StringBuilder sb=new StringBuilder("\n[overwriteDigest()]\n");
        sb.append("existing digest:  " + getDigest()).append("\nnew digest:       " + digest);

        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;

            long highest_delivered_seqno=entry.getHighestDeliveredSeqno();

            Table<Message> buf=xmit_table.get(member);
            if(buf != null) {
                if(local_addr.equals(member)) {
                    // Adjust the highest_delivered seqno (to send msgs again): https://jira.jboss.org/browse/JGRP-1251
                    buf.setHighestDelivered(highest_delivered_seqno);
                    continue; // don't destroy my own window
                }
                xmit_table.remove(member);
            }
            buf=createTable(highest_delivered_seqno);
            xmit_table.put(member, buf);
        }
        sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
        digest_history.add(sb.toString());
        log.debug(sb.toString());
    }


    /**
     * Sets or merges the digest. If there is no entry for a given member in xmit_table, create a new buffer.
     * Else skip the existing entry, unless it is a merge. In this case, skip the existing entry if its seqno is
     * greater than or equal to the one in the digest, or reset the window and create a new one if not.
     * @param digest The digest
     * @param merge Whether to merge the new digest with our own, or not
     */
    protected void setDigest(Digest digest, boolean merge) {
        if(digest == null)
            return;

        StringBuilder sb=log.isDebugEnabled()?
          new StringBuilder("\n[" + local_addr + (merge? " mergeDigest()]\n" : " setDigest()]\n"))
            .append("existing digest:  " + getDigest()).append("\nnew digest:       " + digest) : null;
        
        boolean set_own_seqno=false;
        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;

            long highest_delivered_seqno=entry.getHighestDeliveredSeqno();

            Table<Message> buf=xmit_table.get(member);
            if(buf != null) {
                // We only reset the window if its seqno is lower than the seqno shipped with the digest. Also, we
                // don't reset our own window (https://jira.jboss.org/jira/browse/JGRP-948, comment 20/Apr/09 03:39 AM)
                if(!merge
                        || (Objects.equals(local_addr, member))                  // never overwrite our own entry
                        || buf.getHighestDelivered() >= highest_delivered_seqno) // my seqno is >= digest's seqno for sender
                    continue;

                xmit_table.remove(member);
                // to get here, merge must be false !
                if(member.equals(local_addr)) { // Adjust the seqno: https://jira.jboss.org/browse/JGRP-1251
                    seqno.set(highest_delivered_seqno);
                    set_own_seqno=true;
                }
            }
            buf=createTable(highest_delivered_seqno);
            xmit_table.put(member, buf);
        }
        if(sb != null) {
            sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
            if(set_own_seqno)
                sb.append("\nnew seqno for " + local_addr + ": " + seqno);
            digest_history.add(sb.toString());
            log.debug(sb.toString());
        }
    }


    protected Table<Message> createTable(long initial_seqno) {
        return new Table<>(xmit_table_num_rows, xmit_table_msgs_per_row,
                                  initial_seqno, xmit_table_resize_factor, xmit_table_max_compaction_time);
    }



    /**
     * Garbage collect messages that have been seen by all members. Update sent_msgs: for the sender P in the digest
     * which is equal to the local address, garbage collect all messages <= seqno at digest[P]. Update xmit_table:
     * for each sender P in the digest and its highest seqno seen SEQ, garbage collect all delivered_msgs in the
     * retransmit buffer corresponding to P which are <= seqno at digest[P].
     */
    protected void stable(Digest digest) {
        if(members == null || local_addr == null || digest == null)
            return;

        log.trace("%s: received stable digest %s", local_addr, digest);
        stability_msgs.add(digest.toString());

        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;
            long hd=entry.getHighestDeliveredSeqno();
            long hr=entry.getHighestReceivedSeqno();

            // check whether the last seqno received for a sender P in the stability digest is > last seqno
            // received for P in my digest. if yes, request retransmission (see "Last Message Dropped" topic in DESIGN)
            Table<Message> buf=xmit_table.get(member);
            if(buf != null) {
                long my_hr=buf.getHighestReceived();
                if(hr >= 0 && hr > my_hr) {
                    log.trace("%s: my_highest_rcvd (%d) < stability_highest_rcvd (%d): requesting retransmission of %s",
                              local_addr, my_hr, hr, member + "#" + hr);
                    retransmit(hr, hr, member);
                }
            }

            // delete *delivered* msgs that are stable (all messages with seqnos <= seqno)
            if(hd >= 0 && buf != null) {
                log.trace("%s: deleting msgs <= %s from %s", local_addr, hd, member);
                buf.purge(hd);
            }
        }
    }


    protected void retransmit(long first_seqno, long last_seqno, Address sender) {
        if(first_seqno <= last_seqno)
            retransmit(first_seqno,last_seqno,sender,false);
    }



    protected void retransmit(long first_seqno, long last_seqno, final Address sender, boolean multicast_xmit_request) {
        SeqnoList list=new SeqnoList((int)(last_seqno - first_seqno +1), first_seqno).add(first_seqno, last_seqno);
        retransmit(list,sender,multicast_xmit_request);
    }

    protected void retransmit(SeqnoList missing_msgs, final Address sender, boolean multicast_xmit_request) {
        Address dest=(multicast_xmit_request || this.use_mcast_xmit_req)? null : sender; // to whom do we send the XMIT request ?

        if(xmit_from_random_member && !local_addr.equals(sender)) {
            Address random_member=Util.pickRandomElement(members);
            if(random_member != null && !local_addr.equals(random_member))
                dest=random_member;
        }

        Message retransmit_msg=new Message(dest).setBuffer(Util.streamableToBuffer(missing_msgs))
          .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(this.id, NakAckHeader2.createXmitRequestHeader(sender));

        log.trace("%s --> %s: XMIT_REQ(%s)", local_addr, dest, missing_msgs);
        down_prot.down(retransmit_msg);
        if(stats)
            xmit_reqs_sent.add(missing_msgs.size());
    }


    protected void reset() {
        seqno.set(0);
        xmit_table.clear();
    }



    protected static long sizeOfAllMessages(Table<Message> buf, boolean include_headers) {
        return buf.stream().reduce(0L, (size,el) -> {
            if(el == null)
                return size;
            else
                return size + (include_headers? el.size() : el.getLength());
        }, (l,r) -> l);
    }

    protected void startRetransmitTask() {
        if(xmit_task == null || xmit_task.isDone())
            xmit_task=timer.scheduleWithFixedDelay(new RetransmitTask(), 0, xmit_interval, TimeUnit.MILLISECONDS, sends_can_block);
    }

    protected void stopRetransmitTask() {
        if(xmit_task != null) {
            xmit_task.cancel(true);
            xmit_task=null;
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
            return NAKACK2.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

    @ManagedOperation(description="Triggers the retransmission task, asking all senders for missing messages")
    public void triggerXmit() {
        SeqnoList missing;

        for(Map.Entry<Address,Table<Message>> entry: xmit_table.entrySet()) {
            Address target=entry.getKey(); // target to send retransmit requests to
            Table<Message> buf=entry.getValue();

            if(buf != null && buf.getNumMissing() > 0 && (missing=buf.getMissing(max_xmit_req_size)) != null) { // getNumMissing() is fast
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
                        retransmit(missing, target, false);
                }
            }
            else if(!xmit_task_map.isEmpty())
                xmit_task_map.remove(target); // no current gaps for target
        }

        if(resend_last_seqno && last_seqno_resender != null)
            last_seqno_resender.execute(seqno.get());
    }


    /** Class which is called by RetransmitTask to resend the last seqno sent (if resend_last_seqno is enabled) */
    protected class LastSeqnoResender {
        // Number of times the same seqno has been sent (acquiesces after resend_last_seqno_max_times)
        protected int                 num_resends;
        protected long                last_seqno_resent; // the last seqno that was resent by this task
        // set to true when a regular msg is sent to prevent the task from running
        protected final AtomicBoolean skip_next_resend=new AtomicBoolean(false);

        protected void skipNext() {
            skip_next_resend.compareAndSet(false,true);
        }

        protected void execute(long seqno) {
            if(seqno == 0 || skip_next_resend.compareAndSet(true,false))
                return;
            if(seqno == last_seqno_resent && num_resends >= resend_last_seqno_max_times)
                return;
            if(seqno > last_seqno_resent) {
                last_seqno_resent=seqno;
                num_resends=1;
            }
            else
                num_resends++;
            Message msg=new Message(null).putHeader(id, NakAckHeader2.createHighestSeqnoHeader(seqno))
              .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
              .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK); // we don't need to receive our own broadcast
            down_prot.down(msg);
        }
    }



}

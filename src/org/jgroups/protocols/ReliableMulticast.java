package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.jgroups.Message.Flag.NO_FC;
import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.*;
import static org.jgroups.conf.AttributeType.SCALAR;


/**
 * Base class for reliable multicast protocols
 * @author Bela Ban
 * @since  5.4
 */
@MBean(description="Reliable multicast (one-to-many) protocol")
public abstract class ReliableMulticast extends Protocol implements DiagnosticsHandler.ProbeHandler {

    /** Retransmit messages using multicast rather than unicast. This has the advantage that, if many receivers
     * lost a message, the sender only retransmits once */
    @Property(description="Retransmit retransmit responses (messages) using multicast rather than unicast")
    protected boolean use_mcast_xmit=true;

    /**
     * Use a multicast to request retransmission of missing messages. This may be costly as every member will send a response
     */
    @Property(description="Use a multicast to request retransmission of missing messages")
    protected boolean use_mcast_xmit_req;

    /**
     * Ask a random member for retransmission of a missing message. If set to true, discard_delivered_msgs will be set to false
     */
    @Property(description="Ask a random member for retransmission of a missing message")
    protected boolean xmit_from_random_member;

    /**
     * Messages that have been received in order are sent up the stack (=delivered to the application). Delivered
     * messages are removed from the retransmit table, so they can get GC'ed by the JVM. When this property is true,
     * everyone (except the sender of a message) removes the message from their retransmit table as soon as it has
     * been delivered to the application
     */
    @Property(description="Should messages delivered to application be discarded")
    protected boolean discard_delivered_msgs=true;

    /** If true, logs messages discarded because received from other members */
    @Property(description="discards warnings about promiscuous traffic")
    protected boolean log_discard_msgs=true;

    @Property(description="If false, skips warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Interval (in milliseconds) at which missing messages (from all retransmit buffers) " +
      "are retransmitted. 0 turns retransmission off",type=AttributeType.TIME)
    protected long    xmit_interval=1000;

    @Property(description="Size of the queue to hold messages received after creating the channel, but before being " +
      "connected (is_server=false). After becoming the server, the messages in the queue are fed into up() and the " +
      "queue is cleared. The motivation is to avoid retransmissions (see https://issues.redhat.com/browse/JGRP-1509 " +
      "for details).")
    protected int     become_server_queue_size=50;

    @Property(description="Time during which identical warnings about messages from a non member will be suppressed. " +
      "0 disables this (every warning will be logged). Setting the log level to ERROR also disables this.",
      type=AttributeType.TIME)
    protected long    suppress_time_non_member_warnings=60000;

    @Property(description="Max number of messages to ask for in a retransmit request. 0 disables this and uses " +
      "the max bundle size in the transport",type=SCALAR)
    protected int     max_xmit_req_size=1024;

    @Property(description="The max size of a message batch when delivering messages. 0 is unbounded")
    protected int     max_batch_size;

    @Property(description="If enabled, multicasts the highest sent seqno every xmit_interval ms. This is skipped if " +
      "a regular message has been multicast, and the task aquiesces if the highest sent seqno hasn't changed for " +
      "resend_last_seqno_max_times times. Used to speed up retransmission of dropped last messages (JGRP-1904)")
    protected boolean resend_last_seqno=true;

    @Property(description="Max number of times the last seqno is resent before acquiescing if last seqno isn't incremented")
    protected int     resend_last_seqno_max_times=1;

    @ManagedAttribute(description="True if sending a message can block at the transport level")
    protected boolean sends_can_block=true;

    /* -------------------------------------------------- JMX ---------------------------------------------------------- */


    @ManagedAttribute(description="Number of messages sent",type=SCALAR)
    protected int     num_messages_sent;

    @ManagedAttribute(description="Number of messages received",type=SCALAR)
    protected int     num_messages_received;

    protected static final Message DUMMY_OOB_MSG=new EmptyMessage().setFlag(OOB);

    // Accepts messages which are (1) non-null, (2) no DUMMY_OOB_MSGs and (3) not OOB_DELIVERED
    protected final Predicate<Message> no_dummy_and_no_oob_delivered_msgs_and_no_dont_loopback_msgs=msg ->
      msg != null && msg != DUMMY_OOB_MSG
        && (!msg.isFlagSet(OOB) || msg.setFlagIfAbsent(OOB_DELIVERED))
        && !(msg.isFlagSet(DONT_LOOPBACK) && Objects.equals(local_addr, msg.getSrc()));

    protected static final Predicate<Message> dont_loopback_filter=m -> m != null
      && (m.isFlagSet(DONT_LOOPBACK) || m == DUMMY_OOB_MSG || m.isFlagSet(OOB_DELIVERED));

    protected static final BiConsumer<MessageBatch,Message> BATCH_ACCUMULATOR=MessageBatch::add;

    protected final Function<Message,Long> SEQNO_GETTER= m -> {
        NakAckHeader hdr=m != null? m.getHeader(id) : null;
        return hdr == null || hdr.getType() != NakAckHeader.MSG? -1 : hdr.getSeqno();
    };
    protected final Predicate<Message> HAS_HEADER=m -> m != null && m.getHeader(id) != null;


    @ManagedAttribute(description="Number of retransmit requests received",type=SCALAR)
    protected final LongAdder xmit_reqs_received=new LongAdder();

    @ManagedAttribute(description="Number of retransmit requests sent",type=SCALAR)
    protected final LongAdder xmit_reqs_sent=new LongAdder();

    @ManagedAttribute(description="Number of retransmit responses received (only when use_macst_xmit=false)",type=SCALAR)
    protected final LongAdder xmit_rsps_received=new LongAdder();

    @ManagedAttribute(description="Number of retransmit responses sent",type=SCALAR)
    protected final LongAdder xmit_rsps_sent=new LongAdder();

    /** The average number of messages in a received {@link MessageBatch} */
    @ManagedAttribute(description="The average number of messages in a batch removed from the table and delivered to the application")
    protected final AverageMinMax avg_batch_size=new AverageMinMax();

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

    @ManagedAttribute(description="Number of messages from non-members",type=SCALAR)
    public int getNonMemberMessages() {
        return suppress_log_non_member != null? suppress_log_non_member.getCache().size() : 0;
    }

    protected abstract Buffer<Message> createXmitWindow(long initial_seqno);
    protected Buffer.Options sendOptions() {return Buffer.Options.DEFAULT();}

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

    public ReliableMulticast resendLastSeqno(boolean flag) {setResendLastSeqno(flag); return this;}

    @ManagedAttribute(description="Whether or not the task to resend the last seqno is running (depends on resend_last_seqno)")
    public boolean resendTaskRunning() {return last_seqno_resender != null;}

    @ManagedAttribute(description="tracing is enabled or disabled for the given log",writable=true)
    protected boolean                   is_trace=log.isTraceEnabled();

    /* -------------------------------------------------    Fields    ------------------------------------------------------------------------- */
    protected volatile boolean          is_server;
    protected volatile List<Address>    members=new ArrayList<>();
    protected volatile View             view;
    private final AtomicLong            seqno=new AtomicLong(0); // current message sequence number (starts with 1)

    /** Map to store sent and received messages (keyed by sender) */
    protected final ConcurrentMap<Address,Buffer<Message>> xmit_table=Util.createConcurrentMap();

    /* Optimization: this is the table for my own messages (used in send()) */
    protected Buffer<Message>           local_xmit_table;

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                 xmit_task;
    /** Used by the retransmit task to keep the last retransmitted seqno per sender (https://issues.redhat.com/browse/JGRP-1539) */
    protected final Map<Address,Long>   xmit_task_map=new ConcurrentHashMap<>();
    //* Used by stable to reduce the number of retransmissions (https://issues.redhat.com/browse/JGRP-2678) */
    protected final Map<Address,Long>   stable_xmit_map=new ConcurrentHashMap<>();

    protected volatile boolean          leaving;
    protected volatile boolean          running;
    protected TimeScheduler             timer;
    protected LastSeqnoResender         last_seqno_resender;

    /** Keeps the last N stability messages */
    protected final BoundedList<String> stability_msgs=new BoundedList<>(10);

    /** Keeps a bounded list of the last N digest sets */
    protected final BoundedList<String> digest_history=new BoundedList<>(10);

    protected Queue<Message>            become_server_queue=new ConcurrentLinkedQueue<>();

     /** Log to suppress identical warnings for messages from non-members */
    protected SuppressLog<Address>      suppress_log_non_member;

    public long              getXmitRequestsReceived()                {return xmit_reqs_received.sum();}
    public long              getXmitRequestsSent()                    {return xmit_reqs_sent.sum();}
    public long              getXmitResponsesReceived()               {return xmit_rsps_received.sum();}
    public long              getXmitResponsesSent()                   {return xmit_rsps_sent.sum();}
    public boolean           useMcastXmit()                           {return use_mcast_xmit;}
    public ReliableMulticast useMcastXmit(boolean u)                  {this.use_mcast_xmit=u; return this;}
    public boolean           useMcastXmitReq()                        {return use_mcast_xmit_req;}
    public ReliableMulticast useMcastXmitReq(boolean flag)            {this.use_mcast_xmit_req=flag; return this;}
    public boolean           xmitFromRandomMember()                   {return xmit_from_random_member;}
    public ReliableMulticast xmitFromRandomMember(boolean x)          {this.xmit_from_random_member=x; return this;}
    public boolean           discardDeliveredMsgs()                   {return discard_delivered_msgs;}
    public ReliableMulticast discardDeliveredMsgs(boolean d)          {this.discard_delivered_msgs=d; return this;}
    public boolean           logDiscardMessages()                     {return log_discard_msgs;}
    public ReliableMulticast logDiscardMessages(boolean l)            {this.log_discard_msgs=l; return this;}
    public boolean           logNotFoundMessages()                    {return log_not_found_msgs;}
    public ReliableMulticast logNotFoundMessages(boolean flag)        {log_not_found_msgs=flag; return this;}
    public ReliableMulticast setResendLastSeqnoMaxTimes(int n)        {this.resend_last_seqno_max_times=n; return this;}
    public int               getResendLastSeqnoMaxTimes()             {return resend_last_seqno_max_times;}
    public ReliableMulticast setXmitFromRandomMember(boolean r)       {this.xmit_from_random_member=r; return this;}
    public ReliableMulticast setDiscardDeliveredMsgs(boolean d)       {this.discard_delivered_msgs=d;return this;}
    public long              getXmitInterval()                        {return xmit_interval;}
    public ReliableMulticast setXmitInterval(long x)                  {this.xmit_interval=x; return this;}
    public int               getBecomeServerQueueSize()               {return become_server_queue_size;}
    public ReliableMulticast setBecomeServerQueueSize(int b)          {this.become_server_queue_size=b; return this;}
    public long              getSuppressTimeNonMemberWarnings()       {return suppress_time_non_member_warnings;}
    public ReliableMulticast setSuppressTimeNonMemberWarnings(long s) {this.suppress_time_non_member_warnings=s; return this;}
    public int               getMaxXmitReqSize()                      {return max_xmit_req_size;}
    public ReliableMulticast setMaxXmitReqSize(int m)                 {this.max_xmit_req_size=m; return this;}
    public boolean           sendsCanBlock()                          {return sends_can_block;}
    public ReliableMulticast sendsCanBlock(boolean s)                 {this.sends_can_block=s; return this;}
    public int               getNumMessagesSent()                     {return num_messages_sent;}
    public ReliableMulticast setNumMessagesSent(int n)                {this.num_messages_sent=n; return this;}
    public int               getNumMessagesReceived()                 {return num_messages_received;}
    public ReliableMulticast setNumMessagesReceived(int n)            {this.num_messages_received=n; return this;}
    public boolean           isTrace() {return is_trace;}
    public ReliableMulticast isTrace(boolean i) {this.is_trace=i; return this;}

    public <T extends Protocol> T setLevel(String level) {
        T retval=super.setLevel(level);
        is_trace=log.isTraceEnabled();
        return retval;
    }

    @ManagedAttribute(description="Actual size of the become_server_queue",type=SCALAR)
    public int getBecomeServerQueueSizeActual() {
        return become_server_queue.size();
    }

    /** Returns the receive window for sender; only used for testing. Do not use ! */
    public <T extends Buffer<Message>> T getWindow(Address sender) {
        return (T)xmit_table.get(sender);
    }

    /** Only used for unit tests, don't use ! */
    public void setTimer(TimeScheduler timer) {this.timer=timer;}

    @ManagedAttribute(description="Total number of undelivered messages in all retransmit buffers",type=SCALAR)
    public int getXmitTableUndeliveredMsgs() {
        return xmit_table.values().stream().map(Buffer::size).reduce(Integer::sum).orElse(0);
    }

    @ManagedAttribute(description="Total number of missing messages in all retransmit buffers",type=SCALAR)
    public int getXmitTableMissingMessages() {
        return xmit_table.values().stream().map(Buffer::numMissing).reduce(Integer::sum).orElse(0);
    }

    @ManagedAttribute(description="Capacity of the retransmit buffer")
    public long getXmitTableCapacity() {
        Buffer<Message> win=local_addr != null? xmit_table.get(local_addr) : null;
        return win != null? win.capacity() : 0;
    }

    @ManagedAttribute(description="Returns the number of bytes of all messages in all retransmit buffers. " +
      "To compute the size, Message.getLength() is used",type=AttributeType.BYTES)
    public long getSizeOfAllMessages() {
        return xmit_table.values().stream()
          .map(win -> sizeOfAllMessages(win, false)).reduce(0L, Long::sum);
    }

    @ManagedAttribute(description="Returns the number of bytes of all messages in all retransmit buffers. " +
      "To compute the size, Message.size() is used",type=AttributeType.BYTES)
    public long getSizeOfAllMessagesInclHeaders() {
        return xmit_table.values().stream()
          .map(win -> sizeOfAllMessages(win, true)).reduce(0L, Long::sum);
    }

    @ManagedOperation(description="Prints the contents of the receiver windows for all members")
    public String printMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,Buffer<Message>> entry: xmit_table.entrySet()) {
            Address addr=entry.getKey();
            Buffer<Message> win=entry.getValue();
            ret.append(addr).append(": ").append(win.toString()).append('\n');
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

    @ManagedOperation(description="Resets all statistics")
    public void resetStats() {
        num_messages_sent=num_messages_received=0;
        xmit_reqs_received.reset();
        xmit_reqs_sent.reset();
        xmit_rsps_received.reset();
        xmit_rsps_sent.reset();
        stability_msgs.clear();
        digest_history.clear();
        avg_batch_size.clear();
        Buffer<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
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

        if(xmit_interval <= 0) {
            // https://issues.redhat.com/browse/JGRP-2675
            become_server_queue=new ConcurrentLinkedQueue<>();
            RejectedExecutionHandler handler=transport.getThreadPool().getRejectedExecutionHandler();
            if(handler != null && !isCallerRunsHandler(handler)) {
                log.warn("%s: xmit_interval of %d requires a CallerRunsPolicy in the thread pool; replacing %s",
                         local_addr, xmit_interval, handler.getClass().getSimpleName());
                transport.getThreadPool().setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            }
            Class<RED> cl=RED.class;
            if(stack.findProtocol(cl) != null) {
                String e=String.format("found %s: when retransmission is disabled (xmit_interval=0), this can lead " +
                                         "to message loss. Please remove %s, or enable retransmission",
                                       cl.getSimpleName(), cl.getSimpleName());
                throw new IllegalStateException(e);
            }
        }
        else {
            if(become_server_queue_size <= 0) {
                log.warn("%s: %s.become_server_queue_size is <= 0; setting it to 10", local_addr, ReliableMulticast.class.getSimpleName());
                become_server_queue_size=10;
            }
            become_server_queue=new ArrayBlockingQueue<>(become_server_queue_size);
        }

        if(suppress_time_non_member_warnings > 0)
            suppress_log_non_member=new SuppressLog<>(log, "MsgDroppedNak");

        // max bundle size (minus overhead) divided by <long size> times bits per long
        int estimated_max_msgs_in_xmit_req=(transport.getBundler().getMaxSize() -50) * Global.LONG_SIZE;
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
        become_server_queue.clear();
        stopRetransmitTask();
        xmit_task_map.clear();
        stable_xmit_map.clear();
        local_xmit_table=null; // fixes https://issues.redhat.com/browse/JGRP-2720
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
                stable_xmit_map.keySet().retainAll(mbrs);
                break;

            case Event.BECOME_SERVER:
                is_server=true;
                flushBecomeServerQueue();
                break;

            case Event.DISCONNECT:
                leaving=true;
                reset();
                break;
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
        }
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        if(msg.isFlagSet(Message.Flag.NO_RELIABILITY))
            return up_prot.up(msg);
        NakAckHeader hdr=msg.getHeader(this.id);
        if(hdr == null)
            return up_prot.up(msg);  // pass up (e.g. unicast msg)

        if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
            queueMessage(msg, hdr.seqno);
            return null;
        }

        switch(hdr.type) {

            case NakAckHeader.MSG:
                handleMessage(msg, hdr);
                return null;

            case NakAckHeader.XMIT_REQ:
                try {
                    SeqnoList missing=msg.getObject();
                    if(missing != null)
                        handleXmitReq(msg.getSrc(), missing, hdr.sender);
                }
                catch(Exception e) {
                    log.error("failed deserializing retransmission list", e);
                }
                return null;

            case NakAckHeader.XMIT_RSP:
                handleXmitRsp(msg, hdr);
                return null;

            case NakAckHeader.HIGHEST_SEQNO:
                handleHighestSeqno(msg.getSrc(), hdr.seqno);
                return null;

            default:
                log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
                return null;
        }
    }

    public void up(MessageBatch mb) {
        boolean found_nackack_msg=mb.anyMatch(HAS_HEADER); // found at least 1 msg with a NAKACK2 hdr
        if(!found_nackack_msg) {
            if(!mb.isEmpty())
                up_prot.up(mb);
            return;
        }
        for(FastArray<Message>.FastIterator it=(FastArray<Message>.FastIterator)mb.iterator(); it.hasNext();) {
            final Message msg=it.next();
            NakAckHeader hdr;
            if(msg == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY) || (hdr=msg.getHeader(id)) == null)
                continue;

            if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
                queueMessage(msg, hdr.seqno);
                it.remove();
                continue;
            }

            switch(hdr.type) {
                case NakAckHeader.MSG:
                    break;
                case NakAckHeader.XMIT_REQ:
                    it.remove();
                    try {
                        SeqnoList missing=msg.getObject();
                        if(missing != null)
                            handleXmitReq(msg.getSrc(), missing, hdr.sender);
                    }
                    catch(Exception e) {
                        log.error("failed deserializing retransmission list", e);
                    }
                    break;
                case NakAckHeader.XMIT_RSP:
                    Message xmitted_msg=msgFromXmitRsp(msg, hdr);
                    if(xmitted_msg != null)
                        it.replace(xmitted_msg);
                    break;
                case NakAckHeader.HIGHEST_SEQNO:
                    it.remove();
                    handleHighestSeqno(mb.sender(), hdr.seqno);
                    break;
                default:
                    log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
            }
        }

        if(!mb.isEmpty())
            handleMessageBatch(mb);

        if(!mb.isEmpty())
            up_prot.up(mb);
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
        if(become_server_queue.offer(msg)) // discards item if queue is full
            log.trace("%s: message %s#%d was queued (not yet server)", local_addr, msg.getSrc(), seqno);
        else
            log.trace("%s: message %s#%d was discarded (not yet server, queue full)", local_addr, msg.getSrc(), seqno);
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
     * sent_msgs fails e.g. due to an OOM (see https://issues.redhat.com/browse/JGRP-179). bela Jan 13 2006
     */
    protected void send(Message msg) {
        if(!running) {
            log.trace("%s: discarded message as we're not in the 'running' state, message: %s", local_addr, msg);
            return;
        }

        Buffer<Message> win=local_xmit_table;
        if(win == null && (win=local_xmit_table=xmit_table.get(local_addr)) == null) // discard message if there is no entry for local_addr
            return;

        if(msg.getSrc() == null)
            msg.setSrc(local_addr); // this needs to be done so we can check whether the message sender is the local_addr

        boolean dont_loopback_set=msg.isFlagSet(DONT_LOOPBACK);
        long msg_id=seqno.incrementAndGet();
        msg.putHeader(this.id, NakAckHeader.createMessageHeader(msg_id));
        long sleep=10;
        do {
            try {
                win.add(msg_id, msg, dont_loopback_set? dont_loopback_filter : null, sendOptions());
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

        // moved down_prot.down() out of synchronized clause (bela Sept 7 2006) https://issues.redhat.com/browse/JGRP-300
        if(is_trace)
            log.trace("%s --> [all]: #%d", local_addr, msg_id);
        down_prot.down(msg); // if this fails, since msg is in sent_msgs, it can be retransmitted
        num_messages_sent++;

        if(resend_last_seqno && last_seqno_resender != null)
            last_seqno_resender.skipNext();
    }


    protected void resend(Message msg) { // needed for byteman ProtPerf script - don't remove!
        down_prot.down(msg);
    }

    /**
     * Finds the corresponding retransmit buffer and adds the message to it (according to seqno). Then removes as many
     * messages as possible and passes them up the stack. Discards messages from non-members.
     */
    protected void handleMessage(Message msg, NakAckHeader hdr) {
        Address sender=msg.getSrc();
        Buffer<Message> win=xmit_table.get(sender);
        if(win == null) {  // discard message if there is no entry for sender
            unknownMember(sender, hdr.seqno);
            return;
        }

        num_messages_received++;
        boolean loopback=local_addr.equals(sender);

        // If the message was sent by myself, then it is already in the table and we don't need to add it. If not,
        // and the message is OOB, insert a dummy message (same msg, saving space), deliver it and drop it later on
        // removal. Else insert the real message
        boolean added=loopback || win.add(hdr.seqno, msg.isFlagSet(OOB)? DUMMY_OOB_MSG : msg);

        // OOB msg is passed up. When removed, we discard it. Affects ordering: https://issues.redhat.com/browse/JGRP-379
        if(added && msg.isFlagSet(OOB)) {
            if(loopback) { // sent by self
                msg=win.get(hdr.seqno); // we *have* to get a message, because loopback means we didn't add it to win !
                if(msg != null && msg.isFlagSet(OOB) && msg.setFlagIfAbsent(OOB_DELIVERED))
                    deliver(msg, sender, hdr.seqno, "OOB message");
            }
            else // sent by someone else
                deliver(msg, sender, hdr.seqno, "OOB message");
        }
        removeAndDeliver(win, sender, loopback, null); // at most 1 thread will execute this at any given time
    }


    protected void handleMessageBatch(MessageBatch mb) {
        Address             sender=mb.sender();
        Buffer<Message> win=xmit_table.get(sender);
        if(win == null) {  // discard message if there is no entry for sender
            mb.removeIf(HAS_HEADER, true);
            unknownMember(sender, "batch");
            return;
        }
        int size=mb.size();
        num_messages_received+=size;
        boolean loopback=local_addr.equals(sender), oob=mb.mode() == MessageBatch.Mode.OOB;
        boolean added=loopback || win.add(mb, SEQNO_GETTER, !oob, oob? DUMMY_OOB_MSG : null);

        // OOB msg is passed up. When removed, we discard it. Affects ordering: https://issues.redhat.com/browse/JGRP-379
        if(added && oob) {
            Address dest=mb.dest();
            MessageBatch oob_batch=loopback? new MessageBatch(dest, sender, null, dest == null, MessageBatch.Mode.OOB, size) : mb;
            if(loopback) {
                for(Message m: mb) {
                    long seq=SEQNO_GETTER.apply(m);
                    Message msg=win.get(seq); // we *have* to get the message, because loopback means we didn't add it to win !
                    if(msg != null && msg.isFlagSet(OOB) && msg.setFlagIfAbsent(OOB_DELIVERED))
                        oob_batch.add(msg);
                }
            }
            deliverBatch(oob_batch);
        }
        removeAndDeliver(win, sender, loopback, mb.clusterName()); // at most 1 thread will execute this at any given time
        if(oob || loopback)
            mb.removeIf(HAS_HEADER, true);
    }


    /** Efficient way of checking whether another thread is already processing messages from sender. If that's the case,
     *  we return immediately and let the existing thread process our message (https://issues.redhat.com/browse/JGRP-829).
     *  Benefit: fewer threads blocked on the same lock, these threads can be returned to the thread pool
     */
    protected void removeAndDeliver(Buffer<Message> win, Address sender, boolean loopback, AsciiString cluster_name) {
        AtomicInteger adders=win.getAdders();
        if(adders.getAndIncrement() != 0)
            return;
        boolean remove_msgs=discard_delivered_msgs && !loopback;
        MessageBatch batch=new MessageBatch(win.size()).dest(null).sender(sender).clusterName(cluster_name).multicast(true);
        Supplier<MessageBatch> batch_creator=() -> batch;
        MessageBatch mb=null;
        do {
            try {
                batch.reset();
                // Don't include DUMMY and OOB_DELIVERED messages in the removed set
                mb=win.removeMany(remove_msgs, max_batch_size, no_dummy_and_no_oob_delivered_msgs_and_no_dont_loopback_msgs,
                                  batch_creator, BATCH_ACCUMULATOR);
            }
            catch(Throwable t) {
                log.error("failed removing messages from table for " + sender, t);
            }
            int size=batch.size();
            if(size > 0) {
                if(stats)
                    avg_batch_size.add(size);
                deliverBatch(batch);
            }
        }
        while(mb != null || adders.decrementAndGet() != 0);
    }



    /**
     * Retransmits messsages first_seqno to last_seqno from original_sender from xmit_table to xmit_requester,
     * called when XMIT_REQ is received.
     * @param xmit_requester The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param missing_msgs A list of seqnos that have to be retransmitted
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    protected void handleXmitReq(Address xmit_requester, SeqnoList missing_msgs, Address original_sender) {
        if(is_trace)
            log.trace("%s <-- %s: XMIT(%s%s)", local_addr, xmit_requester, original_sender, missing_msgs);

        if(stats)
            xmit_reqs_received.add(missing_msgs.size());

        Buffer<Message> win=xmit_table.get(original_sender);
        if(win == null) {
            log.error(Util.getMessage("SenderNotFound"), local_addr, original_sender);
            return;
        }

        if(is_trace)
            log.trace("%s --> [all]: resending to %s %s", local_addr, original_sender, missing_msgs);
        for(long i: missing_msgs) {
            Message msg=win.get(i);
            if(msg == null) {
                if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(xmit_requester) && i > win.low())
                    log.warn(Util.getMessage("MessageNotFound"), local_addr, original_sender, i);
                continue;
            }
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
                    NakAckHeader hdr1=first.getHeader(id), hdr2=last.getHeader(id);
                    if(hdr1 != null && hdr2 != null)
                        sb.append("#").append(hdr1.seqno).append("-").append(hdr2.seqno);
                }
                sb.append(" (" + batch.size()).append(" messages)");
                log.trace(sb);
            }
            up_prot.up(batch);
            batch.clear();
        }
        catch(Throwable t) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "batch", batch, t);
        }
    }


    /**
     * Flushes the queue. Done in a separate thread as we don't want to block the
     * {@link GMS#installView(View, Digest)} method (called when a view is installed).
     */
    protected void flushBecomeServerQueue() {
        if(!become_server_queue.isEmpty()) {
            log.trace("%s: flushing become_server_queue (%d elements)", local_addr, become_server_queue.size());
            TP transport=getTransport();
            for(;;) {
                final Message msg=become_server_queue.poll();
                if(msg == null)
                    break;
                transport.getThreadPool().execute(() -> up(msg));
            }
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
            // we modify the original message (instead of copying it) by setting flag DONT_BLOCK: this is fine because
            // the original sender will send the message without this flag; only retransmissions will carry the flag
            msg.setFlag(DONT_BLOCK);
            resend(msg);
            return;
        }

        Message xmit_msg=msg.copy(true, true).setDest(dest) // copy payload and headers
          .setFlag(DONT_BLOCK);
        NakAckHeader hdr=xmit_msg.getHeader(id);
        NakAckHeader newhdr=hdr.copy();
        newhdr.type=NakAckHeader.XMIT_RSP; // change the type in the copy from MSG --> XMIT_RSP
        xmit_msg.putHeader(id, newhdr);
        resend(xmit_msg);
    }

    protected void handleXmitRsp(Message msg, NakAckHeader hdr) {
        if(msg == null)
            return;

        try {
            if(stats)
                xmit_rsps_received.increment();

            msg.setDest(null);
            NakAckHeader newhdr=hdr.copy();
            newhdr.type=NakAckHeader.MSG; // change the type back from XMIT_RSP --> MSG
            msg.putHeader(id, newhdr);
            handleMessage(msg, newhdr);
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
        Buffer<Message> win=xmit_table.get(sender);
        if(win == null)
            return;
        long my_highest_received=win.high();
        if(my_highest_received >= 0 && seqno > my_highest_received) {
            log.trace("%s: my_highest_rcvd (%s#%d) < highest received (%s#%d): requesting retransmission",
                      local_addr, sender, my_highest_received, sender, seqno);
            retransmit(seqno, seqno, sender, false);
        }
    }

    protected Message msgFromXmitRsp(Message msg, NakAckHeader hdr) {
        if(msg == null)
            return null;

        if(stats)
            xmit_rsps_received.increment();

        msg.setDest(null);
        NakAckHeader newhdr=hdr.copy();
        newhdr.type=NakAckHeader.MSG; // change the type back from XMIT_RSP --> MSG
        msg.putHeader(id,newhdr);
        return msg;
    }

    protected static boolean isCallerRunsHandler(RejectedExecutionHandler h) {
        return h instanceof ThreadPoolExecutor.CallerRunsPolicy ||
          (h instanceof ShutdownRejectedExecutionHandler
            && ((ShutdownRejectedExecutionHandler)h).handler() instanceof ThreadPoolExecutor.CallerRunsPolicy);
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
                Buffer<Message> win=xmit_table.remove(member);
                if(win != null)
                    log.debug("%s: removed %s from xmit_table (not member anymore)", local_addr, member);
            }
        }
        members.stream().filter(mbr -> !keys.contains(mbr)).forEach(mbr -> xmit_table.putIfAbsent(mbr, createXmitWindow(0)));
    }

    /** Returns a message digest: for each member P the highest delivered and received seqno is added */
    public Digest getDigest() {
        final Map<Address,long[]> map=new HashMap<>();
        for(Map.Entry<Address,Buffer<Message>> entry: xmit_table.entrySet()) {
            Address sender=entry.getKey(); // guaranteed to be non-null (CCHM)
            Buffer<Message> win=entry.getValue(); // guaranteed to be non-null (CCHM)
            long[] seqnos=win.getDigest();
            map.put(sender, seqnos);
        }
        return new Digest(map);
    }

    public Digest getDigest(Address mbr) {
        if(mbr == null)
            return getDigest();
        Buffer<Message> win=xmit_table.get(mbr);
        if(win == null)
            return null;
        long[] seqnos=win.getDigest();
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
     * exists, and is not the local address, replace it with the new entry (https://issues.redhat.com/browse/JGRP-699)
     * if the digest's seqno is greater than the seqno in the window.
     */
    protected void mergeDigest(Digest digest) {
        setDigest(digest,true);
    }

    /** Overwrites existing entries, but does NOT remove entries not found in the digest */
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

            Buffer<Message> win=xmit_table.get(member);
            if(win != null) {
                if(local_addr.equals(member)) {
                    // Adjust the highest_delivered seqno (to send msgs again): https://issues.redhat.com/browse/JGRP-1251
                    win.highestDelivered(highest_delivered_seqno);
                    continue; // don't destroy my own window
                }
                xmit_table.remove(member);
            }
            win=createXmitWindow(highest_delivered_seqno);
            xmit_table.put(member, win);
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

            Buffer<Message> win=xmit_table.get(member);
            if(win != null) {
                // We only reset the window if its seqno is lower than the seqno shipped with the digest. Also, we
                // don't reset our own window (https://issues.redhat.com/browse/JGRP-948, comment 20/Apr/09 03:39 AM)
                if(!merge
                  || (Objects.equals(local_addr, member))                  // never overwrite our own entry
                  || win.highestDelivered() >= highest_delivered_seqno) // my seqno is >= digest's seqno for sender
                    continue;

                xmit_table.remove(member);
                // to get here, merge must be false!
                if(member.equals(local_addr)) { // Adjust the seqno: https://issues.redhat.com/browse/JGRP-1251
                    seqno.set(highest_delivered_seqno);
                    set_own_seqno=true;
                }
            }
            win=createXmitWindow(highest_delivered_seqno);
            xmit_table.put(member, win);
        }
        if(sb != null) {
            sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
            if(set_own_seqno)
                sb.append("\nnew seqno for " + local_addr + ": " + seqno);
            digest_history.add(sb.toString());
            log.debug(sb.toString());
        }
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
            Buffer<Message> win=xmit_table.get(member);
            if(win != null && xmit_interval > 0) {
                long my_hr=win.high();
                Long prev_hr=stable_xmit_map.get(member);
                if(prev_hr != null && prev_hr > my_hr) {
                    log.trace("%s: my_highest_rcvd (%d) < stability_highest_rcvd (%d): requesting retransmission of %s",
                              local_addr, my_hr, prev_hr, member + "#" + prev_hr);
                    retransmit(prev_hr, prev_hr, member, false);
                }
                stable_xmit_map.put(member, hr);
            }

            // delete *delivered* msgs that are stable (all messages with seqnos <= seqno)
            if(hd >= 0 && win != null) {
                // buf.forEach(buf.getLow(), hd, null);
                log.trace("%s: deleting msgs <= %s from %s", local_addr, hd, member);
                win.purge(hd);
            }
        }
    }



    protected void retransmit(long first_seqno, long last_seqno, final Address sender, boolean multicast_xmit_request) {
        if(first_seqno > last_seqno)
            return;
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

        Message retransmit_msg=new ObjectMessage(dest, missing_msgs).setFlag(OOB, NO_FC).setFlag(DONT_BLOCK)
          .putHeader(this.id, NakAckHeader.createXmitRequestHeader(sender));

        if(is_trace)
            log.trace("%s --> %s: XMIT_REQ(%s)", local_addr, dest, missing_msgs);
        down_prot.down(retransmit_msg);
        if(stats)
            xmit_reqs_sent.add(missing_msgs.size());
    }

    protected void reset() {
        seqno.set(0);
        xmit_table.clear();
    }

    protected static long sizeOfAllMessages(Buffer<Message> win, boolean include_headers) {
        return win.stream().filter(Objects::nonNull)
          .map(m -> include_headers? Long.valueOf(m.size()) : Long.valueOf(m.getLength()))
          .reduce(0L, Long::sum);
    }

    protected void startRetransmitTask() {
        if(xmit_interval > 0 && (xmit_task == null || xmit_task.isDone()))
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
            return ReliableMulticast.class.getSimpleName() + ": RetransmitTask (interval=" + xmit_interval + " ms)";
        }
    }

    @ManagedOperation(description="Triggers the retransmission task, asking all senders for missing messages")
    public void triggerXmit() {
        for(Map.Entry<Address,Buffer<Message>> entry: xmit_table.entrySet()) {
            Address target=entry.getKey(); // target to send retransmit requests to
            Buffer<Message> win=entry.getValue();
            SeqnoList missing;
            if(win != null && win.numMissing() > 0 && (missing=win.getMissing(max_xmit_req_size)) != null) { // getNumMissing() is fast
                long highest=missing.getLast();
                Long prev_seqno=xmit_task_map.get(target);
                if(prev_seqno == null) {
                    xmit_task_map.put(target, highest); // no retransmission
                }
                else {
                    missing.removeHigherThan(prev_seqno); // we only retransmit the 'previous batch'
                    if(highest > prev_seqno)
                        xmit_task_map.put(target, highest);
                    if(!missing.isEmpty()) {
                        // remove msgs that are <= highest-delivered (https://issues.redhat.com/browse/JGRP-2574)
                        long highest_deliverable=win.getHighestDeliverable(), first=missing.getFirst();
                        if(first < highest_deliverable)
                            missing.removeLowerThan(highest_deliverable + 1);
                        retransmit(missing, target, false);
                    }
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
            Message msg=new EmptyMessage(null).putHeader(id, NakAckHeader.createHighestSeqnoHeader(seqno))
              .setFlag(OOB, NO_FC).setFlag(DONT_LOOPBACK,DONT_BLOCK); // we don't need to receive our own broadcast
            down_prot.down(msg);
        }
    }

}

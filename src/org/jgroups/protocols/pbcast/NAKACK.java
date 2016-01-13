package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.protocols.TP;
import org.jgroups.stack.*;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


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
 * @deprecated Will be removed in 4.0
 */
@Deprecated
@MBean(description="Reliable transmission multipoint FIFO protocol")
public class NAKACK extends Protocol implements Retransmitter.RetransmitCommand, DiagnosticsHandler.ProbeHandler {
    private static final int NUM_REBROADCAST_MSGS=3;

    /* -----------------------------------------------------    Properties     --------------------- ------------------------------------ */


    @Property(name="retransmit_timeout", converter=PropertyConverters.IntegerArray.class, description="Timeout before requesting retransmissions")
    private int[] retransmit_timeouts= { 600, 1200, 2400, 4800 }; // time(s) to wait before requesting retransmission


    @Property(description="Max number of messages to be removed from a NakReceiverWindow. This property might " +
      "get removed anytime, so don't use it !")
    private int max_msg_batch_size=100;
    
    /**
     * Retransmit messages using multicast rather than unicast. This has the advantage that, if many receivers
     * lost a message, the sender only retransmits once
     */
    @Property(description="Retransmit retransmit responses (messages) using multicast rather than unicast")
    private boolean use_mcast_xmit=true;

    /**
     * Use a multicast to request retransmission of missing messages. This may
     * be costly as every member in the cluster will send a response
     */
    @Property(description="Use a multicast to request retransmission of missing messages")
    private boolean use_mcast_xmit_req=false;


    @Property(description="Number of milliseconds to delay the sending of an XMIT request. We pick a random number " +
      "in the range [1 .. xmit_req_stagger_timeout] and add this to the scheduling time of an XMIT request. " +
      "When use_mcast_xmit is enabled, if a number of members drop messages from the same member, then chances are that, " +
      "if staggering is enabled, somebody else already sent the XMIT request (via mcast) and we can cancel the XMIT " +
      "request once we receive the missing messages. For unicast XMIT responses (use_mcast_xmit=false), we still have " +
      "an advantage by not overwhelming the receiver with XMIT requests, all at the same time. 0 disabless staggering.")
    protected long xmit_stagger_timeout=200;
    

    /**
     * Ask a random member for retransmission of a missing message. If set to
     * true, discard_delivered_msgs will be set to false
     */
    @Property(description="Ask a random member for retransmission of a missing message. Default is false")
    private boolean xmit_from_random_member=false;

    /**
     * The first value (in milliseconds) to use in the exponential backoff
     * retransmission mechanism. Only enabled if the value is > 0
     */
    @Property(description="The first value (in milliseconds) to use in the exponential backoff. Enabled if greater than 0")
    private int exponential_backoff=300;

    @Property(description="Whether to use the old retransmitter which retransmits individual messages or the new one " +
            "which uses ranges of retransmitted messages. Default is true. Note that this property will be removed in 3.0; " +
            "it is only used to switch back to the old (and proven) retransmitter mechanism if issues occur")
    private boolean use_range_based_retransmitter=true;


    /**
     * Messages that have been received in order are sent up the stack (=
     * delivered to the application). Delivered messages are removed from
     * NakReceiverWindow.xmit_table and moved to
     * NakReceiverWindow.delivered_msgs, where they are later garbage collected
     * (by STABLE). Since we do retransmits only from sent messages, never
     * received or delivered messages, we can turn the moving to delivered_msgs
     * off, so we don't keep the message around, and don't need to wait for
     * garbage collection to remove them.
     */
    @Property(description="Should messages delivered to application be discarded")
    private boolean discard_delivered_msgs=true;

    @Property(description="Timeout to rebroadcast messages. Default is 2000 msec")
    private long max_rebroadcast_timeout=2000;

    /** When not finding a message on an XMIT request, include the last N stability messages in the error message */
    @Property(description="Should stability history be printed if we fail in retransmission. Default is false")
    protected boolean print_stability_history_on_failed_xmit=false;


    /** If true, logs messages discarded because received from other members */
    @Property(description="discards warnings about promiscuous traffic")
    private boolean log_discard_msgs=true;

    @Property(description="If true, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    private boolean log_not_found_msgs=true;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    int xmit_table_num_rows=5;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    int xmit_table_msgs_per_row=10000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    double xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    long xmit_table_max_compaction_time=10 * 60 * 1000;
    
    @Property(description="Size of the queue to hold messages received after creating the channel, but before being " +
      "connected (is_server=false). After becoming the server, the messages in the queue are fed into up() and the " +
      "queue is cleared. The motivation is to avoid retransmissions (see https://issues.jboss.org/browse/JGRP-1509 " +
      "for details). 0 disables the queue.")
    protected int become_server_queue_size=50;

    @Property(description="Time during which identical warnings about messages from a non member will be suppressed. " +
      "0 disables this (every warning will be logged). Setting the log level to ERROR also disables this.")
    protected long suppress_time_non_member_warnings=60000;

    /* -------------------------------------------------- JMX ---------------------------------------------------------- */



    @ManagedAttribute(description="Number of retransmit requests received")
    private final AtomicLong xmit_reqs_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit requests sent")
    private final AtomicLong xmit_reqs_sent=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses received")
    private final AtomicLong xmit_rsps_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses sent")
    private final AtomicLong xmit_rsps_sent=new AtomicLong(0);

    @ManagedAttribute(description="Number of messages sent")
    protected int num_messages_sent=0;

    @ManagedAttribute(description="Number of messages received")
    protected int num_messages_received=0;

    @ManagedAttribute(description="Number of messages from non-members")
    public int getNonMemberMessages() {
        return suppress_log_non_member != null? suppress_log_non_member.getCache().size() : 0;
    }

    @ManagedOperation(description="Clears the cache for messages from non-members")
    public void clearNonMemberCache() {
        if(suppress_log_non_member != null)
            suppress_log_non_member.getCache().clear();
    }


    /* -------------------------------------------------    Fields    ------------------------------------------------------------------------- */
    private volatile boolean    is_server=false;
    private Address             local_addr=null;
    private final List<Address> members=new CopyOnWriteArrayList<>();
    private View                view;
    private final AtomicLong    seqno=new AtomicLong(0); // current message sequence number (starts with 1)

    /** Map to store sent and received messages (keyed by sender) */
    private final ConcurrentMap<Address,NakReceiverWindow> xmit_table=Util.createConcurrentMap();

    private volatile boolean leaving=false;
    private volatile boolean running=false;
    private TimeScheduler timer=null;

    private final Lock rebroadcast_lock=new ReentrantLock();

    private final Condition rebroadcast_done=rebroadcast_lock.newCondition();

    // set during processing of a rebroadcast event
    private volatile boolean rebroadcasting=false;

    private final Lock rebroadcast_digest_lock=new ReentrantLock();
    @GuardedBy("rebroadcast_digest_lock")
    private Digest rebroadcast_digest=null;

    /** Keeps the last 10 stability messages */
    protected final BoundedList<String> stability_msgs=new BoundedList<>(10);

    /** Keeps a bounded list of the last N digest sets */
    protected final BoundedList<String> digest_history=new BoundedList<>(10);

    protected BoundedList<Message>      become_server_queue;

    /** Log to suppress identical warnings for messages from non-members */
    protected SuppressLog<Address>      suppress_log_non_member;


    public long getXmitRequestsReceived() {return xmit_reqs_received.get();}
    public long getXmitRequestsSent() {return xmit_reqs_sent.get();}
    public long getXmitResponsesReceived() {return xmit_rsps_received.get();}
    public long getXmitResponsesSent() {return xmit_rsps_sent.get();}

    @ManagedAttribute(description="Total number of missing messages")
    public int getPendingXmitRequests() {
        int num=0;
        for(NakReceiverWindow win: xmit_table.values()) {
            num+=win.getPendingXmits();
        }
        return num;
    }

    @ManagedAttribute
    public int getXmitTableSize() {
        int num=0;
        for(NakReceiverWindow win: xmit_table.values()) {
            num+=win.size();
        }
        return num;
    }

    @ManagedAttribute
    public int getXmitTableMissingMessages() {
        int num=0;
        for(NakReceiverWindow win: xmit_table.values()) {
            num+=win.getMissingMessages();
        }
        return num;
    }


    @ManagedAttribute(description="Returns the number of bytes of all messages in all NakReceiverWindows. To compute " +
      "the size, Message.getLength() is used")
    public long getSizeOfAllMessages() {
        long retval=0;
        for(NakReceiverWindow win: xmit_table.values())
            retval+=win.sizeOfAllMessages(false);
        return retval;
    }

    @ManagedAttribute(description="Returns the number of bytes of all messages in all NakReceiverWindows. To compute " +
      "the size, Message.size() is used")
    public long getSizeOfAllMessagesInclHeaders() {
        long retval=0;
        for(NakReceiverWindow win: xmit_table.values())
            retval+=win.sizeOfAllMessages(true);
        return retval;
    }


    @ManagedAttribute
    public long getCurrentSeqno() {return seqno.get();}

    @ManagedOperation
    public String printRetransmitStats() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet())
            sb.append(entry.getKey()).append(": ").append(entry.getValue().printRetransmitStats()).append("\n");
        return sb.toString();
    }


    /**
     * Please don't use this method; it is only provided for unit testing !
     * @param mbr
     * @return
     */
    public NakReceiverWindow getWindow(Address mbr) {
        return xmit_table.get(mbr);
    }


    /**
     * Only used for unit tests, don't use !
     * @param timer
     */
    public void setTimer(TimeScheduler timer) {
        this.timer=timer;
    }

    public void resetStats() {
        num_messages_sent=num_messages_received=0;
        xmit_reqs_received.set(0);
        xmit_reqs_sent.set(0);
        xmit_rsps_received.set(0);
        xmit_rsps_sent.set(0);
        stability_msgs.clear();
        digest_history.clear();
    }

    public void init() throws Exception {
        if(xmit_from_random_member) {
            if(discard_delivered_msgs) {
                discard_delivered_msgs=false;
                log.debug("xmit_from_random_member set to true: changed discard_delivered_msgs to false");
            }
        }

        TP transport=getTransport();
        if(transport != null) {
            transport.registerProbeHandler(this);
            if(!transport.supportsMulticasting()) {
                if(use_mcast_xmit) {
                    log.warn("use_mcast_xmit should not be used because the transport (" + transport.getName() +
                            ") does not support IP multicasting; setting use_mcast_xmit to false");
                    use_mcast_xmit=false;
                }
                if(use_mcast_xmit_req) {
                    log.warn("use_mcast_xmit_req should not be used because the transport (" + transport.getName() +
                            ") does not support IP multicasting; setting use_mcast_xmit_req to false");
                    use_mcast_xmit_req=false;
                }
            }
        }

        if(become_server_queue_size > 0)
            become_server_queue=new BoundedList<>(become_server_queue_size);

        if(suppress_time_non_member_warnings > 0)
            suppress_log_non_member=new SuppressLog<>(log, "MsgDroppedNak", "SuppressMsg");
    }


    public boolean isUseMcastXmit() {
        return use_mcast_xmit;
    }

    public void setUseMcastXmit(boolean use_mcast_xmit) {
        this.use_mcast_xmit=use_mcast_xmit;
    }

    public boolean isXmitFromRandomMember() {
        return xmit_from_random_member;
    }

    public void setXmitFromRandomMember(boolean xmit_from_random_member) {
        this.xmit_from_random_member=xmit_from_random_member;
    }

    public boolean isDiscardDeliveredMsgs() {
        return discard_delivered_msgs;
    }

    public void setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        this.discard_delivered_msgs=discard_delivered_msgs;
    }

    public void setLogDiscardMessages(boolean flag) {
        log_discard_msgs=flag;
    }

    public void setLogDiscardMsgs(boolean flag) {
        setLogDiscardMessages(flag);
    }

    public boolean getLogDiscardMessages() {
        return log_discard_msgs;
    }

    public Map<String,Object> dumpStats() {
        Map<String,Object> retval=super.dumpStats();
        retval.put("msgs", printMessages());
        return retval;
    }

    public String printStats() {
        StringBuilder sb=new StringBuilder();
        sb.append("\nStability messages received\n");
        sb.append(printStabilityMessages()).append("\n");

        return sb.toString();
    }

    @ManagedOperation(description="TODO")
    public String printStabilityMessages() {
        StringBuilder sb=new StringBuilder();
        sb.append(Util.printListWithDelimiter(stability_msgs, "\n"));
        return sb.toString();
    }

    public String printStabilityHistory() {
        StringBuilder sb=new StringBuilder();
        int i=1;
        for(String digest: stability_msgs) {
            sb.append(i++).append(": ").append(digest).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Keeps information about the last N times a digest was set or merged")
    public String printDigestHistory() {
        StringBuilder sb=new StringBuilder(local_addr + ":\n");
        for(String tmp: digest_history)
            sb.append(tmp).append("\n");
        return sb.toString();
    }

    @ManagedOperation(description="TODO")
    public String printLossRates() {
        StringBuilder sb=new StringBuilder();
        NakReceiverWindow win;
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet()) {
            win=entry.getValue();
            sb.append(entry.getKey()).append(": ").append(win.printLossRate()).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Returns the sizes of all NakReceiverWindow.RetransmitTables")
    public String printRetransmitTableSizes() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet()) {
            NakReceiverWindow win=entry.getValue();
            sb.append(entry.getKey() + ": ").append(win.getRetransmitTableSize())
              .append(", offset=").append(win.getRetransmitTableOffset())
              .append(" (capacity=" + win.getRetransmitTableCapacity())
              .append(", fill factor=" + win.getRetransmitTableFillFactor() + "%)\n");
        }
        return sb.toString();
    }


    @ManagedOperation(description="Compacts the retransmission tables")
    public void compact() {
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet()) {
            NakReceiverWindow win=entry.getValue();
            win.compact();
        }
    }


    public List<Integer> providedUpServices() {
        return Arrays.asList(Event.GET_DIGEST, Event.SET_DIGEST, Event.OVERWRITE_DIGEST, Event.MERGE_DIGEST);
    }


    public void start() throws Exception {
        if((timer=getTransport().getTimer()) == null)
            throw new Exception("timer is null");
        running=true;
        leaving=false;
    }


    public void stop() {
        running=false;
        is_server=false;
        if(become_server_queue != null)
            become_server_queue.clear();
        reset();  // clears sent_msgs and destroys all NakReceiverWindows
    }


    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>down_prot.down()</code> in this
     * method as the event is passed down by default by the superclass after this method returns !</b>
     */
    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null || msg.isFlagSet(Message.NO_RELIABILITY))
                    break; // unicast address: not null and not mcast, pass down unchanged

                send(evt, msg);
                return null;    // don't pass down the stack

            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable((Digest)evt.getArg());
                return null;  // do not pass down further (Bela Aug 7 2001)

            case Event.GET_DIGEST:
                return getDigest((Address)evt.getArg());

            case Event.SET_DIGEST:
                setDigest((Digest)evt.getArg());
                return null;

            case Event.OVERWRITE_DIGEST:
                overwriteDigest((Digest)evt.getArg());
                return null;

            case Event.MERGE_DIGEST:
                mergeDigest((Digest)evt.getArg());
                return null;

            case Event.TMP_VIEW:
                View tmp_view=(View)evt.getArg();
                List<Address> mbrs=tmp_view.getMembers();
                members.clear();
                members.addAll(mbrs);
                break;

            case Event.VIEW_CHANGE:
                tmp_view=(View)evt.getArg();
                mbrs=tmp_view.getMembers();
                members.clear();
                members.addAll(mbrs);
                view=tmp_view;
                adjustReceivers(members);
                is_server=true;  // check vids from now on
                if(suppress_log_non_member != null)
                    suppress_log_non_member.removeExpired(suppress_time_non_member_warnings);
                break;

            case Event.BECOME_SERVER:
                is_server=true;
                flushBecomeServerQueue();
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.DISCONNECT:
                leaving=true;
                reset();
                break;

            case Event.REBROADCAST:
                rebroadcasting=true;
                rebroadcast_digest=(Digest)evt.getArg();
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




    /**
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>PassUp</code> in this
     * method as the event is passed up by default by the superclass after this method returns !</b>
     */
    public Object up(Event evt) {
        switch(evt.getType()) {

        case Event.MSG:
            Message msg=(Message)evt.getArg();
            if(msg.isFlagSet(Message.Flag.NO_RELIABILITY))
                break;
            NakAckHeader hdr=(NakAckHeader)msg.getHeader(this.id);
            if(hdr == null)
                break;  // pass up (e.g. unicast msg)

            if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
                if(become_server_queue != null) {
                    become_server_queue.add(msg);
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": message " + msg.getSrc() + "::" + hdr.seqno + " was added to queue (not yet server)");
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace(local_addr + ": message " + msg.getSrc() + "::" + hdr.seqno + " was discarded (not yet server)");
                }
                return null;
            }

            // Changed by bela Jan 29 2003: we must not remove the header, otherwise further xmit requests will fail !
            //hdr=(NakAckHeader)msg.removeHeader(getName());

            switch(hdr.type) {

            case NakAckHeader.MSG:
                handleMessage(msg, hdr);
                return null;        // transmitter passes message up for us !

            case NakAckHeader.XMIT_REQ:
                if(hdr.range == null) {
                    if(log.isErrorEnabled()) {
                        log.error(Util.getMessage("XMITREQRangeOfXmitMsgIsNull"), msg.getSrc());
                    }
                    return null;
                }
                handleXmitReq(msg.getSrc(), hdr.range.low, hdr.range.high, hdr.sender);
                return null;

            case NakAckHeader.XMIT_RSP:
                handleXmitRsp(msg, hdr);
                return null;

            default:
                if(log.isErrorEnabled()) {
                    log.error(Util.getMessage("NakAckHeaderType"), hdr.type);
                }
                return null;
            }

        case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
            stable((Digest)evt.getArg());
            return null;  // do not pass up further (Bela Aug 7 2001)

        case Event.SUSPECT:
            // release the promise if rebroadcasting is in progress... otherwise we wait forever. there will be a new
            // flush round anyway
            if(rebroadcasting) {
                cancelRebroadcasting();
            }
            break;
        }
        return up_prot.up(evt);
    }






    /* --------------------------------- Private Methods --------------------------------------- */

    /**
     * Adds the message to the sent_msgs table and then passes it down the stack. Change Bela Ban May 26 2002: we don't
     * store a copy of the message, but a reference ! This saves us a lot of memory. However, this also means that a
     * message should not be changed after storing it in the sent-table ! See protocols/DESIGN for details.
     * Made seqno increment and adding to sent_msgs atomic, e.g. seqno won't get incremented if adding to
     * sent_msgs fails e.g. due to an OOM (see http://jira.jboss.com/jira/browse/JGRP-179). bela Jan 13 2006
     */
    protected void send(Event evt, Message msg) {
        if(msg == null)
            throw new NullPointerException("msg is null; event is " + evt);

        if(!running) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": discarded message as we're not in the 'running' state, message: " + msg);
            return;
        }

        long msg_id;
        NakReceiverWindow win=xmit_table.get(local_addr);
        if(win == null) {  // discard message if there is no entry for local_addr
            if(log.isWarnEnabled() && log_discard_msgs)
                log.warn(local_addr + ": discarded message to " + local_addr + " with no window, my view is " + view);
            return;
        }

        if(msg.getSrc() == null)
            msg.setSrc(local_addr); // this needs to be done so we can check whether the message sender is the local_addr

        msg_id=seqno.incrementAndGet();
        long sleep=10;
        while(running) {
            try {
                msg.putHeader(this.id, NakAckHeader.createMessageHeader(msg_id));
                win.add(msg_id, msg);
                break;
            }
            catch(Throwable t) {
                if(running)
                    Util.sleep(sleep);
                sleep=Math.min(5000, sleep*2);
            }
        }

         // moved down_prot.down() out of synchronized clause (bela Sept 7 2006) http://jira.jboss.com/jira/browse/JGRP-300
        if(log.isTraceEnabled())
            log.trace("sending " + local_addr + "#" + msg_id);
        down_prot.down(evt); // if this fails, since msg is in sent_msgs, it can be retransmitted
        num_messages_sent++;
    }



    /**
     * Finds the corresponding NakReceiverWindow and adds the message to it (according to seqno). Then removes as many
     * messages as possible from the NRW and passes them up the stack. Discards messages from non-members.
     */
    private void handleMessage(Message msg, NakAckHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(Util.getMessage("SenderOfMessageIsNull"));
            return;
        }

        NakReceiverWindow win=xmit_table.get(sender);
        if(win == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(log.isWarnEnabled() && log_discard_msgs) {
                if(suppress_log_non_member != null) {
                    suppress_log_non_member.log(SuppressLog.Level.WARN, sender, suppress_time_non_member_warnings,
                                                local_addr, hdr.seqno, sender, view);
                }
                else
                    log.warn(Util.getMessage("MsgDroppedNak"), local_addr, hdr.seqno, sender, view);
            }
            return;
        }

        num_messages_received++;
        boolean loopback=local_addr.equals(sender);
        boolean added=loopback || win.add(hdr.seqno, msg);

        if(added && log.isTraceEnabled())
            log.trace(new StringBuilder().append(local_addr).append(": received ").append(sender).append('#').append(hdr.seqno));


        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && msg.isFlagSet(Message.Flag.OOB)) {
            if(loopback)
                msg=win.get(hdr.seqno); // we *have* to get a message, because loopback means we didn't add it to win !
            if(msg != null && msg.isFlagSet(Message.Flag.OOB)) {
                if(msg.setTransientFlagIfAbsent(Message.OOB_DELIVERED)) {
                    if(log.isTraceEnabled())
                        log.trace(new StringBuilder().append(local_addr).append(": delivering ").append(sender).append('#').append(hdr.seqno));
                    try {
                        up_prot.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("FailedToDeliverOOBMessage"), msg, t);
                    }
                }
            }
        }

        // Efficient way of checking whether another thread is already processing messages from 'sender'.
        // If that's the case, we return immediately and let the existing thread process our message
        // (https://jira.jboss.org/jira/browse/JGRP-829). Benefit: fewer threads blocked on the same lock, these threads
        // can be returned to the thread pool
        final AtomicBoolean processing=win.getProcessing();
        if(!processing.compareAndSet(false, true)) {
            return;
        }

        boolean remove_msgs=discard_delivered_msgs && !loopback;
        boolean released_processing=false;
        try {
            while(true) {
                // we're removing a msg and set processing to false (if null) *atomically* (wrt to add())
                List<Message> msgs=win.removeMany(processing, remove_msgs, max_msg_batch_size);
                if(msgs == null || msgs.isEmpty()) {
                    released_processing=true;
                    if(rebroadcasting)
                        checkForRebroadcasts();
                    return;
                }

                for(final Message msg_to_deliver: msgs) {
                    // discard OOB msg if it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-379)
                    if(msg_to_deliver.isFlagSet(Message.Flag.OOB) && !msg_to_deliver.setTransientFlagIfAbsent(Message.OOB_DELIVERED))
                        continue;

                    if(msg_to_deliver.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK)
                      && local_addr != null && local_addr.equals(msg.getSrc()))
                        continue;

                    //msg_to_deliver.removeHeader(getName()); // Changed by bela Jan 29 2003: not needed (see above)
                    try {
                        if(log.isTraceEnabled()) {
                            NakAckHeader header=(NakAckHeader)msg_to_deliver.getHeader(this.id);
                            log.trace(new StringBuilder().append(local_addr).append(": delivering ").append(sender).append('#').append(header.seqno));
                        }
                        up_prot.up(new Event(Event.MSG, msg_to_deliver));
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("FailedToDeliverMessage"), msg_to_deliver, t);
                    }
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



    /**
     * Retransmits messsages first_seqno to last_seqno from original_sender from xmit_table to xmit_requester,
     * called when XMIT_REQ is received.
     * @param xmit_requester        The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param first_seqno The first sequence number to be retransmitted (<= last_seqno)
     * @param last_seqno  The last sequence number to be retransmitted (>= first_seqno)
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    private void handleXmitReq(Address xmit_requester, long first_seqno, long last_seqno, Address original_sender) {
        if(first_seqno > last_seqno)
            return;

        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append(local_addr).append(": received xmit request from ").append(xmit_requester).append(" for ");
            sb.append(original_sender).append(" [").append(first_seqno).append(" - ").append(last_seqno).append("]");
            log.trace(sb.toString());
        }

        if(stats)
            xmit_reqs_received.addAndGet(last_seqno - first_seqno +1);

        NakReceiverWindow win=xmit_table.get(original_sender);
        if(win == null) {
            if(log.isErrorEnabled()) {
                StringBuilder sb=new StringBuilder();
                sb.append("(requester=").append(xmit_requester).append(", local_addr=").append(this.local_addr);
                sb.append(") ").append(original_sender).append(" not found in retransmission table");
                // don't print the table unless we are in trace mode because it can be LARGE
                if (log.isTraceEnabled()) {
                    sb.append(":\n").append(printMessages());
                } 
                if(print_stability_history_on_failed_xmit) {
                    sb.append(" (stability history:\n").append(printStabilityHistory());
                }
                log.error(sb.toString());
            }
            return;
        }

        long diff=last_seqno - first_seqno +1;
        if(diff >= 10) {
            List<Message> msgs=win.get(first_seqno, last_seqno);
            if(msgs != null) {
                for(Message msg: msgs)
                    sendXmitRsp(xmit_requester, msg);
            }
        }
        else {
            for(long i=first_seqno; i <= last_seqno; i++) {
                Message msg=win.get(i);
                if(msg == null) {
                    if(log.isWarnEnabled() && log_not_found_msgs && !local_addr.equals(xmit_requester)) {
                        StringBuilder sb=new StringBuilder();
                        sb.append("(requester=").append(xmit_requester).append(", local_addr=").append(this.local_addr);
                        sb.append(") message ").append(original_sender).append("::").append(i);
                        sb.append(" not found in retransmission table of ").append(original_sender).append(":\n").append(win);
                        if(print_stability_history_on_failed_xmit) {
                            sb.append(" (stability history:\n").append(printStabilityHistory());
                        }
                        log.warn(sb.toString());
                    }
                    continue;
                }
                sendXmitRsp(xmit_requester, msg);
            }
        }
    }


    private void cancelRebroadcasting() {
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
     * Flushes the queue. Done in a separate thread as we don't want to block the
     * {@link ClientGmsImpl#installView(org.jgroups.View,org.jgroups.util.Digest)} method (called when a view is installed).
     */
    protected void flushBecomeServerQueue() {
        if(become_server_queue != null && !become_server_queue.isEmpty()) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing become_server_queue (" + become_server_queue.size() + " elements)");

            TP transport=getTransport();
            Executor thread_pool=transport.getDefaultThreadPool(), oob_thread_pool=transport.getOOBThreadPool();

            for(final Message msg: become_server_queue) {
                Executor pool=msg.isFlagSet(Message.Flag.OOB)? oob_thread_pool : thread_pool;
                pool.execute(new Runnable() {
                    public void run() {
                        try {
                            up(new Event(Event.MSG, msg));
                        }
                        finally {
                            become_server_queue.remove(msg);
                        }
                    }
                });
            }
        }
    }


    /**
     * Sends a message msg to the requester. We have to wrap the original message into a retransmit message, as we need
     * to preserve the original message's properties, such as src, headers etc.
     * @param dest
     * @param msg
     */
    private void sendXmitRsp(Address dest, Message msg) {
        if(msg == null) {
            if(log.isErrorEnabled())
                log.error(Util.getMessage("MessageIsNullCannotSendRetransmission"));
            return;
        }

        if(stats)
            xmit_rsps_sent.incrementAndGet();

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        if(use_mcast_xmit) { // we simply send the original multicast message
            down_prot.down(new Event(Event.MSG, msg));
            return;
        }

        Message xmit_msg=msg.copy(true, true).dest(dest).setFlag(Message.Flag.INTERNAL); // copy payload and headers
        NakAckHeader hdr=(NakAckHeader)xmit_msg.getHeader(id);
        NakAckHeader newhdr=hdr.copy();    // create a copy of the header: https://issues.jboss.org/browse/JGRP-1502
        newhdr.type=NakAckHeader.XMIT_RSP; // change the type in the copy from MSG --> XMIT_RSP
        xmit_msg.putHeader(id, newhdr);
        down_prot.down(new Event(Event.MSG, xmit_msg));
    }


    private void handleXmitRsp(Message msg, NakAckHeader hdr) {
        if(msg == null)
            return;

        try {
            if(stats)
                xmit_rsps_received.incrementAndGet();

            msg.setDest(null);
            NakAckHeader newhdr=hdr.copy();
            newhdr.type=NakAckHeader.MSG; // change the type back from XMIT_RSP --> MSG
            msg.putHeader(id, newhdr);
            up(new Event(Event.MSG, msg));
            if(rebroadcasting)
                checkForRebroadcasts();
        }
        catch(Exception ex) {
            if(log.isErrorEnabled()) {
                log.error(Util.getMessage("FailedReadingRetransmittedMessage"), ex);
            }
        }
    }


    /**
     * Takes the argument highest_seqnos and compares it to the current digest. If the current digest has fewer messages,
     * then send retransmit messages for the missing messages. Return when all missing messages have been received. If
     * we're waiting for a missing message from P, and P crashes while waiting, we need to exclude P from the wait set.
     */
    private void rebroadcastMessages() {
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

                // Cannot ask for 0 to be retransmitted because the first seqno in NAKACK and UNICAST(2) is always 1 !
                // Also, we need to ask for retransmission of my_high+1, because we already *have* my_high, and don't
                // need it, so the retransmission range is [my_high+1 .. their_high]: *exclude* my_high, but *include*
                // their_high
                long my_high=Math.max(my_entry[0], my_entry[1]);
                if(their_high > my_high) {
                    if(log.isTraceEnabled())
                        log.trace("[" + local_addr + "] fetching " + my_high + "-" + their_high + " from " + member);
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
                        if(!rebroadcasting || isGreaterThanOrEqual(my_digest,rebroadcast_digest))
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
        boolean cancel_rebroadcasting;
        rebroadcast_digest_lock.lock();
        try {
            cancel_rebroadcasting=isGreaterThanOrEqual(tmp,rebroadcast_digest);
        }
        finally {
            rebroadcast_digest_lock.unlock();
        }
        if(cancel_rebroadcasting) {
            cancelRebroadcasting();
        }
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
     * Remove old members from NakReceiverWindows. Essentially removes all entries from xmit_table that are not
     * in <code>members</code>. This method is not called concurrently multiple times
     */
    private void adjustReceivers(List<Address> members) {
        Set<Address> keys=xmit_table.keySet();

        for(Address member: keys) {
            if(!members.contains(member)) {
                if(local_addr != null && local_addr.equals(member))
                    continue;
                NakReceiverWindow win=xmit_table.remove(member);
                if(win != null) {
                    win.destroy();
                    log.debug("%s: removed %s from xmit_table (not member anymore)", local_addr, member);
                }
            }
        }

        for(Address member: this.members)
            if(!keys.contains(member))
                xmit_table.putIfAbsent(member, createNakReceiverWindow(member, 0));
    }


    /**
     * Returns a message digest: for each member P the highest delivered and received seqno is added
     */
    public Digest getDigest() {
        final Map<Address,long[]> map=new HashMap<>();
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet()) {
            Address sender=entry.getKey(); // guaranteed to be non-null (CCHM)
            NakReceiverWindow win=entry.getValue(); // guaranteed to be non-null (CCHM)
            long[] seqnos=win.getDigest();
            map.put(sender, seqnos);
        }
        return new Digest(map);
    }


    public Digest getDigest(Address mbr) {
        if(mbr == null)
            return getDigest();
        NakReceiverWindow win=xmit_table.get(mbr);
        if(win == null)
            return null;
        long[] seqnos=win.getDigest();
        return new Digest(mbr, seqnos[0], seqnos[1]);
    }



    /**
     * Creates a NakReceiverWindow for each sender in the digest according to the sender's seqno. If NRW already exists,
     * reset it.
     */
    private void setDigest(Digest digest) {
        setDigest(digest, false);
    }


    /**
     * For all members of the digest, adjust the NakReceiverWindows in xmit_table. If no entry
     * exists, create one with the initial seqno set to the seqno of the member in the digest. If the member already
     * exists, and is not the local address, replace it with the new entry (http://jira.jboss.com/jira/browse/JGRP-699)
     * if the digest's seqno is greater than the seqno in the window.
     */
    private void mergeDigest(Digest digest) {
        setDigest(digest, true);
    }

    /**
     * Overwrites existing entries, but does NOT remove entries not found in the digest
     * @param digest
     */
    private void overwriteDigest(final Digest digest) {
        if(digest == null)
            return;

        StringBuilder sb=new StringBuilder("\n[overwriteDigest()]\n");
        sb.append("existing digest:  " + getDigest()).append("\nnew digest:       " + digest);

        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;

            long highest_delivered_seqno=entry.getHighestDeliveredSeqno();

            NakReceiverWindow win=xmit_table.get(member);
            if(win != null) {
                if(local_addr.equals(member)) {
                    // Adjust the highest_delivered seqno (to send msgs again): https://jira.jboss.org/browse/JGRP-1251
                    win.setHighestDelivered(highest_delivered_seqno);
                    continue; // don't destroy my own window
                }
                xmit_table.remove(member);
                win.destroy(); // stops retransmission
            }
            win=createNakReceiverWindow(member, highest_delivered_seqno);
            xmit_table.put(member, win);
        }
        sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
        digest_history.add(sb.toString());
        if(log.isDebugEnabled())
            log.debug(sb.toString());
    }


    /**
     * Sets or merges the digest. If there is no entry for a given member in xmit_table, create a new NakReceiverWindow.
     * Else skip the existing entry, unless it is a merge. In this case, skip the existing entry if its seqno is
     * greater than or equal to the one in the digest, or reset the window and create a new one if not.
     * @param digest The digest
     * @param merge Whether to merge the new digest with our own, or not
     */
    private void setDigest(final Digest digest, boolean merge) {
        if(digest == null)
            return;

        StringBuilder sb=new StringBuilder(merge? "\n[" + local_addr + " mergeDigest()]\n" : "\n["+local_addr + " setDigest()]\n");
        sb.append("existing digest:  " + getDigest()).append("\nnew digest:       " + digest);

        boolean set_own_seqno=false;
        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;

            long highest_delivered_seqno=entry.getHighestDeliveredSeqno();

            NakReceiverWindow win=xmit_table.get(member);
            if(win != null) {
                // We only reset the window if its seqno is lower than the seqno shipped with the digest. Also, we
                // don't reset our own window (https://jira.jboss.org/jira/browse/JGRP-948, comment 20/Apr/09 03:39 AM)
                if(!merge
                        || (local_addr != null && local_addr.equals(member)) // never overwrite our own entry
                        || win.getHighestDelivered() >= highest_delivered_seqno) // my seqno is >= digest's seqno for sender
                    continue;

                xmit_table.remove(member);
                win.destroy(); // stops retransmission
                // to get here, merge must be false !
                if(member.equals(local_addr)) { // Adjust the seqno: https://jira.jboss.org/browse/JGRP-1251
                    seqno.set(highest_delivered_seqno);
                    set_own_seqno=true;
                }
            }
            win=createNakReceiverWindow(member, highest_delivered_seqno);
            xmit_table.put(member, win);
        }
        sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
        if(set_own_seqno)
            sb.append("\nnew seqno for " + local_addr + ": " + seqno);
        digest_history.add(sb.toString());
        if(log.isDebugEnabled())
            log.debug(sb.toString());
    }


    private NakReceiverWindow createNakReceiverWindow(Address sender, long initial_seqno) {
        NakReceiverWindow win=new NakReceiverWindow(sender, this, initial_seqno, timer, use_range_based_retransmitter,
                                                    xmit_table_num_rows, xmit_table_msgs_per_row,
                                                    xmit_table_resize_factor, xmit_table_max_compaction_time, false);

        if(exponential_backoff > 0)
            win.setRetransmitTimeouts(new ExponentialInterval(exponential_backoff));
        else
            win.setRetransmitTimeouts(new StaticInterval(retransmit_timeouts));

        if(xmit_stagger_timeout > 0)
            win.setXmitStaggerTimeout(xmit_stagger_timeout);
        return win;
    }



    /**
     * Garbage collect messages that have been seen by all members. Update sent_msgs: for the sender P in the digest
     * which is equal to the local address, garbage collect all messages <= seqno at digest[P]. Update xmit_table:
     * for each sender P in the digest and its highest seqno seen SEQ, garbage collect all delivered_msgs in the
     * NakReceiverWindow corresponding to P which are <= seqno at digest[P].
     */
    private void stable(Digest digest) {
        NakReceiverWindow recv_win;
        long my_highest_rcvd;        // highest seqno received in my digest for a sender P
        long stability_highest_rcvd; // highest seqno received in the stability vector for a sender P

        if(members == null || local_addr == null || digest == null) {
            log.warn("members, local_addr or digest are null !");
            return;
        }

        if(log.isTraceEnabled()) {
            log.trace("received stable digest " + digest);
        }

        stability_msgs.add(digest.toString());

        long high_seqno_delivered, high_seqno_received;

        for(Digest.Entry entry: digest) {
            Address member=entry.getMember();
            if(member == null)
                continue;
            high_seqno_delivered=entry.getHighestDeliveredSeqno();
            high_seqno_received=entry.getHighestReceivedSeqno();


            // check whether the last seqno received for a sender P in the stability vector is > last seqno
            // received for P in my digest. if yes, request retransmission (see "Last Message Dropped" topic
            // in DESIGN)
            recv_win=xmit_table.get(member);
            if(recv_win != null) {
                my_highest_rcvd=recv_win.getHighestReceived();
                stability_highest_rcvd=high_seqno_received;

                if(stability_highest_rcvd >= 0 && stability_highest_rcvd > my_highest_rcvd) {
                    if(log.isTraceEnabled()) {
                        log.trace("my_highest_rcvd (" + my_highest_rcvd + ") < stability_highest_rcvd (" +
                                stability_highest_rcvd + "): requesting retransmission of " +
                                    member + '#' + stability_highest_rcvd);
                    }
                    retransmit(stability_highest_rcvd, stability_highest_rcvd, member);
                }
            }

            if(high_seqno_delivered < 0)
                continue;

            if(log.isTraceEnabled())
                log.trace("deleting msgs <= " + high_seqno_delivered + " from " + member);

            // delete *delivered* msgs that are stable
            if(recv_win != null)
                recv_win.stable(high_seqno_delivered);  // delete all messages with seqnos <= seqno
        }
    }



    /* ---------------------- Interface Retransmitter.RetransmitCommand ---------------------- */


    /**
     * Implementation of Retransmitter.RetransmitCommand. Called by retransmission thread when gap is detected.
     */
    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        if(first_seqno <= last_seqno)
            retransmit(first_seqno, last_seqno, sender, false);
    }



    protected void retransmit(long first_seqno, long last_seqno, final Address sender, boolean multicast_xmit_request) {
        Address dest=sender; // to whom do we send the XMIT request ?

        if(multicast_xmit_request || this.use_mcast_xmit_req) {
            dest=null;
        }
        else {
            if(xmit_from_random_member && !local_addr.equals(sender)) {
                Address random_member=(Address)Util.pickRandomElement(members);
                if(random_member != null && !local_addr.equals(random_member)) {
                    dest=random_member;
                    if(log.isTraceEnabled())
                        log.trace("picked random member " + dest + " to send XMIT request to");
                }
            }
        }

        NakAckHeader hdr=NakAckHeader.createXmitRequestHeader(first_seqno, last_seqno, sender);
        Message retransmit_msg=new Message(dest).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL);
        if(log.isTraceEnabled())
            log.trace(local_addr + ": sending XMIT_REQ ([" + first_seqno + ", " + last_seqno + "]) to " + dest);
        retransmit_msg.putHeader(this.id, hdr);

        down_prot.down(new Event(Event.MSG, retransmit_msg));
        if(stats)
            xmit_reqs_sent.addAndGet(last_seqno - first_seqno +1);
    }
    /* ------------------- End of Interface Retransmitter.RetransmitCommand -------------------- */



    private void reset() {
        seqno.set(0);
        for(NakReceiverWindow win: xmit_table.values())
            win.destroy();
        xmit_table.clear();
    }


    @ManagedOperation(description="Prints the contents of the receiver windows for all members")
    public String printMessages() {
        StringBuilder ret=new StringBuilder(local_addr + ":\n");
        for(Map.Entry<Address,NakReceiverWindow> entry: xmit_table.entrySet()) {
            Address addr=entry.getKey();
            NakReceiverWindow win=entry.getValue();
            ret.append(addr).append(": ").append(win.toString()).append('\n');
        }
        return ret.toString();
    }



    // ProbeHandler interface
    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> retval=new HashMap<>();
        for(String key: keys) {
            if(key.equals("digest-history"))
                retval.put(key, printDigestHistory());
            if(key.equals("dump-digest"))
                retval.put(key, "\n" + printMessages());
        }

        return retval;
    }

    // ProbeHandler interface
    public String[] supportedKeys() {
        return new String[]{"digest-history", "dump-digest"};
    }


    /* ----------------------------- End of Private Methods ------------------------------------ */


}

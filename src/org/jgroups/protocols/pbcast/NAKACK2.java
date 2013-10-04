package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.util.*;
import java.util.concurrent.*;
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
 */
@MBean(description="Reliable transmission multipoint FIFO protocol")
public class NAKACK2 extends Protocol implements DiagnosticsHandler.ProbeHandler {
    protected static final int NUM_REBROADCAST_MSGS=3;

    /* -----------------------------------------------------    Properties     --------------------- ------------------------------------ */



    @Property(description="Max number of messages to be removed from a RingBuffer. This property might " +
      "get removed anytime, so don't use it !")
    protected int max_msg_batch_size=100;
    
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
    protected long max_rebroadcast_timeout=2000;

    /**
     * When not finding a message on an XMIT request, include the last N
     * stability messages in the error message
     */
    @Property(description="Should stability history be printed if we fail in retransmission. Default is false")
    @Deprecated
    protected boolean print_stability_history_on_failed_xmit=false;


    /** If true, logs messages discarded because received from other members */
    @Property(description="discards warnings about promiscuous traffic")
    protected boolean log_discard_msgs=true;

    @Property(description="If true, trashes warnings about retransmission messages not found in the xmit_table (used for testing)")
    protected boolean log_not_found_msgs=true;

    @Property(description="Interval (in milliseconds) at which missing messages (from all retransmit buffers) " +
      "are retransmitted")
    protected long xmit_interval=1000;

    @Property(description="Number of rows of the matrix in the retransmission table (only for experts)",writable=false)
    protected int xmit_table_num_rows=50;

    @Property(description="Number of elements of a row of the matrix in the retransmission table (only for experts). " +
      "The capacity of the matrix is xmit_table_num_rows * xmit_table_msgs_per_row",writable=false)
    protected int xmit_table_msgs_per_row=10000;

    @Property(description="Resize factor of the matrix in the retransmission table (only for experts)",writable=false)
    protected double xmit_table_resize_factor=1.2;

    @Property(description="Number of milliseconds after which the matrix in the retransmission table " +
      "is compacted (only for experts)",writable=false)
    protected long xmit_table_max_compaction_time=10000;

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
    protected final AtomicLong xmit_reqs_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit requests sent")
    protected final AtomicLong xmit_reqs_sent=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses received")
    protected final AtomicLong xmit_rsps_received=new AtomicLong(0);

    @ManagedAttribute(description="Number of retransmit responses sent")
    protected final AtomicLong xmit_rsps_sent=new AtomicLong(0);

    @ManagedAttribute(description="Is the retransmit task running")
    public boolean isXmitTaskRunning() {return xmit_task != null && !xmit_task.isDone();}

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
    protected volatile boolean          is_server=false;
    protected Address                   local_addr=null;
    protected volatile List<Address>    members=new ArrayList<Address>();
    protected volatile View             view;
    private final AtomicLong            seqno=new AtomicLong(0); // current message sequence number (starts with 1)

    /** Map to store sent and received messages (keyed by sender) */
    protected final ConcurrentMap<Address,Table<Message>> xmit_table=Util.createConcurrentMap();

    /** RetransmitTask running every xmit_interval ms */
    protected Future<?>                 xmit_task;
    /** Used by the retransmit task to keep the last retransmitted seqno per sender (https://issues.jboss.org/browse/JGRP-1539) */
    protected final Map<Address,Long>   xmit_task_map=new ConcurrentHashMap<Address,Long>();

    protected volatile boolean          leaving=false;
    protected volatile boolean          running=false;
    protected TimeScheduler             timer=null;

    protected final Lock                rebroadcast_lock=new ReentrantLock();
    protected final Condition           rebroadcast_done=rebroadcast_lock.newCondition();

    // set during processing of a rebroadcast event
    protected volatile boolean          rebroadcasting=false;
    protected final Lock                rebroadcast_digest_lock=new ReentrantLock();
    @GuardedBy("rebroadcast_digest_lock")
    protected Digest                    rebroadcast_digest=null;

    /** Keeps the last N stability messages */
    protected final BoundedList<String> stability_msgs=new BoundedList<String>(10);

    /** Keeps a bounded list of the last N digest sets */
    protected final BoundedList<String> digest_history=new BoundedList<String>(10);

    protected BoundedList<Message>      become_server_queue;

     /** Log to suppress identical warnings for messages from non-members */
    protected SuppressLog<Address>      suppress_log_non_member;


    public long    getXmitRequestsReceived()  {return xmit_reqs_received.get();}
    public long    getXmitRequestsSent()      {return xmit_reqs_sent.get();}
    public long    getXmitResponsesReceived() {return xmit_rsps_received.get();}
    public long    getXmitResponsesSent()     {return xmit_rsps_sent.get();}
    public boolean isUseMcastXmit()           {return use_mcast_xmit;}
    public boolean isXmitFromRandomMember()   {return xmit_from_random_member;}
    public boolean isDiscardDeliveredMsgs()   {return discard_delivered_msgs;}
    public boolean getLogDiscardMessages()    {return log_discard_msgs;}
    public void    setUseMcastXmit(boolean use_mcast_xmit) {this.use_mcast_xmit=use_mcast_xmit;}
    public void    setUseMcastXmitReq(boolean flag) {this.use_mcast_xmit_req=flag;}
    public void    setLogDiscardMessages(boolean flag) {log_discard_msgs=flag;}
    public void    setLogNotFoundMessages(boolean flag) {log_not_found_msgs=flag;}
    public void    setXmitFromRandomMember(boolean xmit_from_random_member) {
        this.xmit_from_random_member=xmit_from_random_member;
    }
    public void    setDiscardDeliveredMsgs(boolean discard_delivered_msgs) {
        this.discard_delivered_msgs=discard_delivered_msgs;
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
        StringBuilder sb=new StringBuilder();
        sb.append(Util.printListWithDelimiter(stability_msgs, "\n"));
        return sb.toString();
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
        for(Map.Entry<Address,Table<Message>> entry: xmit_table.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue().getNumRows()).append("\n");
        }
        return sb.toString();
    }

    @ManagedOperation(description="Resets all statistics")
    public void resetStats() {
        num_messages_sent=num_messages_received=0;
        xmit_reqs_received.set(0);
        xmit_reqs_sent.set(0);
        xmit_rsps_received.set(0);
        xmit_rsps_sent.set(0);
        stability_msgs.clear();
        digest_history.clear();
        Table<Message> table=local_addr != null? xmit_table.get(local_addr) : null;
        if(table != null)
            table.resetStats();
    }

    public void init() throws Exception {
        if(xmit_from_random_member) {
            if(discard_delivered_msgs) {
                discard_delivered_msgs=false;
                log.debug("%s: xmit_from_random_member set to true: changed discard_delivered_msgs to false", local_addr);
            }
        }

        TP transport=getTransport();
        if(transport != null) {
            transport.registerProbeHandler(this);
            if(!transport.supportsMulticasting()) {
                if(use_mcast_xmit) {
                    log.warn(Util.getMessage("NoMulticastTransport"), "use_mcast_xmit", transport.getName(), "use_mcast_xmit");
                    use_mcast_xmit=false;
                }
                if(use_mcast_xmit_req) {
                    log.warn(Util.getMessage("NoMulticastTransport"), "use_mcast_xmit_req", transport.getName(), "use_mcast_xmit_req");
                    use_mcast_xmit_req=false;
                }
            }
        }

        if(become_server_queue_size > 0)
            become_server_queue=new BoundedList<Message>(become_server_queue_size);

        if(suppress_time_non_member_warnings > 0)
            suppress_log_non_member=new SuppressLog<Address>(log, "MsgDroppedNak", "SuppressMsg");
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



    public String printStabilityHistory() {
        StringBuilder sb=new StringBuilder();
        int i=1;
        for(String digest: stability_msgs)
            sb.append(i++).append(": ").append(digest).append("\n");
        return sb.toString();
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
     * <b>Callback</b>. Called by superclass when event may be handled.<p> <b>Do not use <code>down_prot.down()</code> in this
     * method as the event is passed down by default by the superclass after this method returns !</b>
     */
    public Object down(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest != null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
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
                members=tmp_view.getMembers();
                break;

            case Event.VIEW_CHANGE:
                tmp_view=(View)evt.getArg();
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
                NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(this.id);
                if(hdr == null)
                    break;  // pass up (e.g. unicast msg)

                if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
                    if(become_server_queue != null) {
                        become_server_queue.add(msg);
                        log.trace("%s: message %s::%d was added to queue (not yet server)", local_addr, msg.getSrc(), hdr.seqno);
                    }
                    else
                        log.trace("%s: message %s::%d was discarded (not yet server)", local_addr, msg.getSrc(), hdr.seqno);
                    return null;
                }

                // Changed by bela Jan 29 2003: we must not remove the header, otherwise further xmit requests will fail !
                //hdr=(NakAckHeader2)msg.removeHeader(getName());

                switch(hdr.type) {

                    case NakAckHeader2.MSG:
                        handleMessage(msg, hdr);
                        return null;        // transmitter passes message up for us !

                    case NakAckHeader2.XMIT_REQ:
                        SeqnoList missing=(SeqnoList)msg.getObject();
                        if(missing == null)
                            return null;
                        handleXmitReq(msg.getSrc(), missing, hdr.sender);
                        return null;

                    case NakAckHeader2.XMIT_RSP:
                        handleXmitRsp(msg, hdr);
                        return null;

                    default:
                        log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
                        return null;
                }

            case Event.STABLE:  // generated by STABLE layer. Delete stable messages passed in arg
                stable((Digest)evt.getArg());
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


    public void up(MessageBatch batch) {
        int                       size=batch.size();
        boolean                   got_retransmitted_msg=false; // if at least 1 XMIT-RSP was received
        List<Tuple<Long,Message>> msgs=null;      // regular or retransmitted messages

        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            final Message msg=it.next();
            if(msg == null || msg.isFlagSet(Message.Flag.NO_RELIABILITY))
                continue;
            NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(id);
            if(hdr == null)
                continue;
            it.remove(); // remove the message from the batch, so it won't be passed up the stack

            if(!is_server) { // discard messages while not yet server (i.e., until JOIN has returned)
                if(become_server_queue != null) {
                    become_server_queue.add(msg);
                    log.trace("%s: message %s::%d was added to queue (not yet server)", local_addr, msg.getSrc(), hdr.seqno);
                }
                else
                    log.trace("%s: message %s::%d was discarded (not yet server)", local_addr, msg.getSrc(), hdr.seqno);
                continue;
            }

            switch(hdr.type) {
                case NakAckHeader2.MSG:
                    if(msgs == null)
                        msgs=new ArrayList<Tuple<Long,Message>>(size);
                    msgs.add(new Tuple<Long,Message>(hdr.seqno, msg));
                    break;
                case NakAckHeader2.XMIT_REQ:
                    SeqnoList missing=(SeqnoList)msg.getObject();
                    if(missing != null)
                        handleXmitReq(msg.getSrc(), missing, hdr.sender);
                    break;
                case NakAckHeader2.XMIT_RSP:
                    Message xmitted_msg=msgFromXmitRsp(msg, hdr);
                    if(xmitted_msg != null) {
                        if(msgs == null)
                            msgs=new ArrayList<Tuple<Long,Message>>(size);
                        msgs.add(new Tuple<Long,Message>(hdr.seqno, xmitted_msg));
                        got_retransmitted_msg=true;
                    }
                    break;
                default:
                    log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
            }
        }

        // Process (new and retransmitted) messages:
        if(msgs != null)
            handleMessages(batch.sender(), msgs, batch.mode() == MessageBatch.Mode.OOB, batch.clusterName());

        // received XMIT-RSPs:
        if(got_retransmitted_msg && rebroadcasting)
            checkForRebroadcasts();

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    // ProbeHandler interface
    public Map<String, String> handleProbe(String... keys) {
        Map<String,String> retval=new HashMap<String,String>();
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
            log.trace("%s: discarded message as we're not in the 'running' state, message: %s", local_addr, msg);
            return;
        }

        long msg_id;
        Table<Message> buf=xmit_table.get(local_addr);
        if(buf == null) // discard message if there is no entry for local_addr
            return;

        if(msg.getSrc() == null)
            msg.setSrc(local_addr); // this needs to be done so we can check whether the message sender is the local_addr

        msg_id=seqno.incrementAndGet();
        long sleep=10;
        while(running) {
            try {
                msg.putHeader(this.id, NakAckHeader2.createMessageHeader(msg_id));
                buf.add(msg_id, msg);
                break;
            }
            catch(Throwable t) {
                if(!running)
                    break;
                Util.sleep(sleep);
                sleep=Math.min(5000, sleep*2);
            }
        }

        // moved down_prot.down() out of synchronized clause (bela Sept 7 2006) http://jira.jboss.com/jira/browse/JGRP-300
        if(log.isTraceEnabled())
            log.trace("%s: sending %s#%d", local_addr, local_addr, msg_id);
        down_prot.down(evt); // if this fails, since msg is in sent_msgs, it can be retransmitted
        num_messages_sent++;
    }



    /**
     * Finds the corresponding retransmit buffer and adds the message to it (according to seqno). Then removes as many
     * messages as possible and passes them up the stack. Discards messages from non-members.
     */
    protected void handleMessage(Message msg, NakAckHeader2 hdr) {
        Address sender=msg.getSrc();
        Table<Message> buf=xmit_table.get(sender);
        if(buf == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(log.isWarnEnabled() && log_discard_msgs) {
                if(suppress_log_non_member != null)
                    suppress_log_non_member.log(SuppressLog.Level.warn, sender, suppress_time_non_member_warnings,
                                                local_addr, hdr.seqno, sender, view);
                else
                    log.warn(Util.getMessage("MsgDroppedNak"), local_addr, hdr.seqno, sender, view);
            }
            return;
        }

        num_messages_received++;
        boolean loopback=local_addr.equals(sender);
        boolean added=loopback || buf.add(hdr.seqno, msg);

        if(added && log.isTraceEnabled())
            log.trace("%s: received %s#%d", local_addr, sender, hdr.seqno);


        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && msg.isFlagSet(Message.Flag.OOB)) {
            if(loopback)
                msg=buf.get(hdr.seqno); // we *have* to get a message, because loopback means we didn't add it to win !
            if(msg != null && msg.isFlagSet(Message.Flag.OOB)) {
                if(msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED)) {
                    if(log.isTraceEnabled())
                        log.trace("%s: delivering %s#%d", local_addr, sender, hdr.seqno);
                    try {
                        up_prot.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "OOB message", msg, t);
                    }
                }
            }
        }

        removeAndPassUp(buf, sender, loopback, null); // at most 1 thread will execute this at any given time
    }


    protected void handleMessages(Address sender, List<Tuple<Long,Message>> msgs, boolean oob, String cluster_name) {
        Table<Message> buf=xmit_table.get(sender);
        if(buf == null) {  // discard message if there is no entry for sender
            if(leaving)
                return;
            if(log.isWarnEnabled() && log_discard_msgs) {
                if(suppress_log_non_member != null)
                    suppress_log_non_member.log(SuppressLog.Level.warn, sender, suppress_time_non_member_warnings,
                                                local_addr, "batch", sender, view);
                else
                    log.warn(Util.getMessage("MsgDroppedNak"), local_addr, "batch", sender, view);
            }
            return;
        }
        int size=msgs.size();
        num_messages_received+=size;
        boolean loopback=local_addr.equals(sender);
        boolean added=loopback || (oob ? buf.add(msgs, true) : buf.add(msgs));

        if(added && log.isTraceEnabled())
            log.trace("%s: received %s#%d - #%d (%d messages)",
                      local_addr, sender, msgs.get(0).getVal1(), msgs.get(size-1).getVal1(), msgs.size());


        // OOB msg is passed up. When removed, we discard it. Affects ordering: http://jira.jboss.com/jira/browse/JGRP-379
        if(added && oob) {
            for(Tuple<Long,Message> tuple: msgs) {
                long    seq=tuple.getVal1();
                Message msg=loopback? buf.get(seq) : tuple.getVal2(); // we *have* to get the message, because loopback means we didn't add it to win !
                if(msg != null && msg.isFlagSet(Message.Flag.OOB)) {
                    if(msg.setTransientFlagIfAbsent(Message.TransientFlag.OOB_DELIVERED)) {
                        if(log.isTraceEnabled())
                            log.trace("%s: delivering %s#%d", local_addr, sender, seq);
                        try {
                            up_prot.up(new Event(Event.MSG, msg));
                        }
                        catch(Throwable t) {
                            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "OOB message", msg, t);
                        }
                    }
                }
            }
        }

        removeAndPassUp(buf,sender,loopback,cluster_name); // at most 1 thread will execute this at any given time
    }


    /** Efficient way of checking whether another thread is already processing messages from sender. If that's the case,
     *  we return immediately and let the existing thread process our message (https://jira.jboss.org/jira/browse/JGRP-829).
     *  Benefit: fewer threads blocked on the same lock, these threads an be returned to the thread pool */
    protected void removeAndPassUp(Table<Message> buf, Address sender, boolean loopback, String cluster_name) {
        final AtomicBoolean processing=buf.getProcessing();
        if(!processing.compareAndSet(false, true))
            return;

        boolean remove_msgs=discard_delivered_msgs && !loopback;
        boolean released_processing=false;
        try {
            while(true) {
                // we're removing a msg and set processing to false (if null) *atomically* (wrt to add())
                List<Message> msgs=buf.removeMany(processing, remove_msgs, max_msg_batch_size);
                if(msgs == null || msgs.isEmpty()) {
                    released_processing=true;
                    if(rebroadcasting)
                        checkForRebroadcasts();
                    return;
                }

                MessageBatch batch=new MessageBatch(null, sender, cluster_name, true, msgs);
                for(Message msg_to_deliver: batch) {
                    // discard OOB msg if it has already been delivered (http://jira.jboss.com/jira/browse/JGRP-379)
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
                            NakAckHeader2 hdr1=(NakAckHeader2)first.getHeader(id), hdr2=(NakAckHeader2)last.getHeader(id);
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



    /**
     * Retransmits messsages first_seqno to last_seqno from original_sender from xmit_table to xmit_requester,
     * called when XMIT_REQ is received.
     * @param xmit_requester The sender of the XMIT_REQ, we have to send the requested copy of the message to this address
     * @param missing_msgs A list of seqnos that have to be retransmitted
     * @param original_sender The member who originally sent the messsage. Guaranteed to be non-null
     */
    protected void handleXmitReq(Address xmit_requester, SeqnoList missing_msgs, Address original_sender) {
        log.trace("%s: received xmit request from %s for %s%s", local_addr, xmit_requester, original_sender, missing_msgs);

        if(stats)
            xmit_reqs_received.addAndGet(missing_msgs.size());

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
            if(log.isTraceEnabled())
                log.trace(local_addr + ": resending " + original_sender + "::" + i);
            sendXmitRsp(xmit_requester, msg);
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
            xmit_rsps_sent.incrementAndGet();

        if(msg.getSrc() == null)
            msg.setSrc(local_addr);

        if(use_mcast_xmit) { // we simply send the original multicast message
            down_prot.down(new Event(Event.MSG, msg));
            return;
        }

        Message xmit_msg=msg.copy(true, true).dest(dest).setFlag(Message.Flag.INTERNAL); // copy payload and headers
        NakAckHeader2 hdr=(NakAckHeader2)xmit_msg.getHeader(id);
        NakAckHeader2 newhdr=hdr.copy();
        newhdr.type=NakAckHeader2.XMIT_RSP; // change the type in the copy from MSG --> XMIT_RSP
        xmit_msg.putHeader(id, newhdr);
        down_prot.down(new Event(Event.MSG,xmit_msg));
    }


    protected void handleXmitRsp(Message msg, NakAckHeader2 hdr) {
        if(msg == null)
            return;

        try {
            if(stats)
                xmit_rsps_received.incrementAndGet();

            msg.setDest(null);
            NakAckHeader2 newhdr=hdr.copy();
            newhdr.type=NakAckHeader2.MSG; // change the type back from XMIT_RSP --> MSG
            msg.putHeader(id, newhdr);
            up(new Event(Event.MSG, msg));
            if(rebroadcasting)
                checkForRebroadcasts();
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedToDeliverMsg"), local_addr, "retransmitted message", msg, ex);
        }
    }

    protected Message msgFromXmitRsp(Message msg, NakAckHeader2 hdr) {
        if(msg == null)
            return null;

        if(stats)
            xmit_rsps_received.incrementAndGet();

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
        boolean cancel_rebroadcasting;
        rebroadcast_digest_lock.lock();
        try {
            cancel_rebroadcasting=isGreaterThanOrEqual(tmp, rebroadcast_digest);
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
     * Removes old members from xmit-table and adds new members to xmit-table (at seqnos hd=0, hr=0).
     * This method is not called concurrently
     */
    protected void adjustReceivers(List<Address> members) {
        Set<Address> keys=xmit_table.keySet();

        // remove members which left
        for(Address member: keys) {
            if(!members.contains(member)) {
                if(local_addr != null && local_addr.equals(member))
                    continue;
                Table<Message> buf=xmit_table.remove(member);
                if(buf != null)
                    log.debug("%s: removed %s from xmit_table (not member anymore)", local_addr, member);
            }
        }

        for(Address member: members)
            if(!keys.contains(member))
                xmit_table.putIfAbsent(member, createTable(0));
    }


    /**
     * Returns a message digest: for each member P the highest delivered and received seqno is added
     */
    public Digest getDigest() {
        final Map<Address,long[]> map=new HashMap<Address,long[]>();
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
        if(log.isDebugEnabled())
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
          new StringBuilder(merge? "\n[" + local_addr + " mergeDigest()]\n" : "\n["+local_addr + " setDigest()]\n")
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
                        || (local_addr != null && local_addr.equals(member)) // never overwrite our own entry
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
        if(sb != null)
            sb.append("\n").append("resulting digest: " + getDigest().toString(digest));
        if(set_own_seqno && sb != null)
            sb.append("\nnew seqno for " + local_addr + ": " + seqno);
        if(sb != null)
            digest_history.add(sb.toString());
        if(log.isDebugEnabled())
            log.debug(sb.toString());
    }


    protected Table<Message> createTable(long initial_seqno) {
        return new Table<Message>(xmit_table_num_rows, xmit_table_msgs_per_row,
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
                    log.trace("%s: my_highest_rcvd (%) < stability_highest_rcvd (%): requesting retransmission of %s",
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
        retransmit(new SeqnoList(first_seqno,last_seqno),sender,multicast_xmit_request);
    }

    protected void retransmit(SeqnoList missing_msgs, final Address sender, boolean multicast_xmit_request) {
        Address dest=(multicast_xmit_request || this.use_mcast_xmit_req)? null : sender; // to whom do we send the XMIT request ?

        if(xmit_from_random_member && !local_addr.equals(sender)) {
            Address random_member=Util.pickRandomElement(members);
            if(random_member != null && !local_addr.equals(random_member))
                dest=random_member;
        }

        Message retransmit_msg=new Message(dest, missing_msgs).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL)
          .putHeader(this.id, NakAckHeader2.createXmitRequestHeader(sender));

        log.trace("%s: sending XMIT_REQ (%s) to %s", local_addr, missing_msgs, dest);
        down_prot.down(new Event(Event.MSG, retransmit_msg));
        if(stats)
            xmit_reqs_sent.addAndGet(missing_msgs.size());
    }


    protected void reset() {
        seqno.set(0);
        xmit_table.clear();
    }



    protected static long sizeOfAllMessages(Table<Message> buf, boolean include_headers) {
        Counter counter=new Counter(include_headers);
        buf.forEach(buf.getHighestDelivered() + 1,buf.getHighestReceived(),counter);
        return counter.getResult();
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
                    if(missing.size() > 0)
                        retransmit(missing, target, false);
                }
            }
            else if(!xmit_task_map.isEmpty())
                xmit_task_map.remove(target); // no current gaps for target
        }
    }




    protected static class Counter implements Table.Visitor<Message> {
        protected final boolean count_size; // use size() or length()
        protected long          result=0;

        public Counter(boolean count_size) {
            this.count_size=count_size;
        }

        public long getResult() {return result;}

        public boolean visit(long seqno, Message element, int row, int column) {
            if(element != null)
                result+=count_size? element.size() : element.getLength();
            return true;
        }
    }



}

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender. Works for both unicast and multicast messages.
 * <p/>
 * Note that this protocol must be located towards the top of the stack, or all down_threads from JChannel to this
 * protocol must be set to false ! This is in order to block JChannel.send()/JChannel.down().
 * <br/>This is the second simplified implementation of the same model. The algorithm is sketched out in
 * doc/FlowControl.txt
 * <br/>
 * Changes (Brian) April 2006:
 * <ol>
 * <li>Receivers now send credits to a sender when more than min_credits have been received (rather than when min_credits
 * are left)
 * <li>Receivers don't send the full credits (max_credits), but rather tha actual number of bytes received
 * <ol/>
 * @author Bela Ban
 * @version $Id: FC.java,v 1.102 2009/03/13 09:17:18 belaban Exp $
 */
@MBean(description="Simple flow control protocol based on a credit system")
public class FC extends Protocol {

    private final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
    private final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);  
    private final static String name="FC";
    
    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    /**
     * Max number of bytes to send per receiver until an ack must
     * be received before continuing sending
     */
    @ManagedAttribute(description="Max number of bytes to send per receiver until an ack must " + 
                                   "be received before continuing sending",writable=true)
    @Property(description="Max number of bytes to send per receiver until an ack must be received to proceed. Default is 500000 bytes")                                   
    private long max_credits=500000;

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to
     * wait forever.
     */
    @ManagedAttribute(description="Max time (in milliseconds) to block",writable=true)
    @Property(description="Max time (in milliseconds) to block. Default is 5000 msec")
    private long max_block_time=5000;

    /**
     * Defines the max number of milliseconds for a message to block before being sent, based on the length of
     * the message. The property is defined as a comma-separated list of values (separated by ':'), where the key
     * is the size in bytes and the value is the number of milliseconds to block.
     * Example: max_block_times="50:1,500:3,1500:5,10000:10,100000:100". This means that messages up to 50 bytes wait
     * 1 ms max until they get sent, messages up to 500 bytes 3 ms, and so on.
     * If a message's length (size of the payload in bytes) is for example 15'000 bytes,
     * FC blocks it for a max of 100 ms.
     */
    private Map<Long,Long> max_block_times=null;

    /** Keeps track of the end time after which a message should not get blocked anymore */
    private static final ThreadLocal<Long> end_time=new ThreadLocal<Long>();


    /**
     * If credits fall below this limit, we send more credits to the sender. (We also send when
     * credits are exhausted (0 credits left))
     */
    @ManagedAttribute(description="If credits fall below this limit, we send more credits to the sender",writable=true)
    @Property(description="If credits fall below this limit, we send more credits to the sender. Default is 0.25")
    private double min_threshold=0.25;

    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    @ManagedAttribute(description="Computed as max_credits x min_theshold",writable=true)
    @Property(description="Computed as max_credits x min_theshold unless explicitely set")
    private long min_credits=0;
    
    /**
     * Whether an up thread that comes back down should be allowed to
     * bypass blocking if all credits are exhausted. Avoids JGRP-465.
     * Set to false by default in 2.5 because we have OOB messages for credit replenishments - this flag should not be set
     * to true if the concurrent stack is used
     */
    @Property
    private boolean ignore_synchronous_response=false;
    
    
    
    
    /* ---------------------------------------------   JMX      ------------------------------------------------------ */
    
    
    private int num_blockings=0;
    private int num_credit_requests_received=0, num_credit_requests_sent=0;
    private int num_credit_responses_sent=0, num_credit_responses_received=0;
    private long total_time_blocking=0;

    private final BoundedList<Long> last_blockings=new BoundedList<Long>(50);
    
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
    /**
     * Map<Address,Long>: keys are members, values are credits left. For each send, the
     * number of credits is decremented by the message size. A HashMap rather than a ConcurrentHashMap is
     * currently used as there might be null values
     */
    @GuardedBy("sent_lock")
    private final Map<Address, Long> sent=new HashMap<Address, Long>(11);   

    /**
     * Map<Address,Long>: keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits are 0, we refill and send a CREDIT message to the sender. Sender blocks until CREDIT
     * is received after reaching <tt>min_credits</tt> credits.
     */
    @GuardedBy("received_lock")
    private final Map<Address, Long> received=new ConcurrentHashMap<Address, Long>(11);


    /**
     * List of members from whom we expect credits
     */
    @GuardedBy("sent_lock")
    private final Set<Address> creditors=new HashSet<Address>(11);

    
    /** Peers who have asked for credit that we didn't have */
    private final Set<Address> pending_requesters=new HashSet<Address>(11);

    /**
     * Whether FC is still running, this is set to false when the protocol terminates (on stop())
     */
    private volatile boolean running=true;


    private boolean frag_size_received=false;

   
    /**
     * the lowest credits of any destination (sent_msgs)
     */
    @GuardedBy("sent_lock")
    private long lowest_credit=max_credits;

    /** Lock protecting sent credits table and some other vars (creditors for example) */
    private final Lock sent_lock=new ReentrantLock();

    /** Lock protecting received credits table */
    private final Lock received_lock=new ReentrantLock();


    /** Mutex to block on down() */
    private final Condition credits_available=sent_lock.newCondition();
   

    /**
     * Thread that carries messages through up() and shouldn't be blocked
     * in down() if ignore_synchronous_response==true. JGRP-465.
     */
    private final ThreadLocal<Boolean> ignore_thread=new ThreadLocal<Boolean>() {
        protected Boolean initialValue() {
            return false;
        }
    };   

    /** Last time a credit request was sent. Used to prevent credit request storms */
    @GuardedBy("sent_lock")
    private long last_credit_request=0;   


    public final String getName() {
        return name;
    }

    public void resetStats() {
        super.resetStats();
        num_blockings=0;
        num_credit_responses_sent=num_credit_responses_received=num_credit_requests_received=num_credit_requests_sent=0;
        total_time_blocking=0;
        last_blockings.clear();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
    }

    public double getMinThreshold() {
        return min_threshold;
    }

    public void setMinThreshold(double min_threshold) {
        this.min_threshold=min_threshold;
    }

    public long getMinCredits() {
        return min_credits;
    }

    public void setMinCredits(long min_credits) {
        this.min_credits=min_credits;
    }

    @ManagedAttribute(description="Number of times flow control blocks sender")
    public int getNumberOfBlockings() {
        return num_blockings;
    }

    public long getMaxBlockTime() {
        return max_block_time;
    }

    public void setMaxBlockTime(long t) {
        max_block_time=t;
    }

    @ManagedAttribute(writable=true)
    @Property(description="Max times to block for the listed messages sizes (Message.getLength())")
    public void setMaxBlockTimes(String str) {
        if(str == null) return;
        Long prev_key=null, prev_val=null;
        List<String> vals=Util.parseCommaDelimitedStrings(str);
        if(max_block_times == null)
            max_block_times=new TreeMap<Long,Long>();
        for(String tmp: vals) {
            int index=tmp.indexOf(':');
            if(index == -1)
                throw new IllegalArgumentException("element '" + tmp + "'  is missing a ':' separator");
            Long key=Long.parseLong(tmp.substring(0, index).trim());
            Long val=Long.parseLong(tmp.substring(index +1).trim());

            // sanity checks:
            if(key < 0 || val < 0)
                throw new IllegalArgumentException("keys and values must be >= 0");

            if(prev_key != null) {
                if(key <= prev_key)
                    throw new IllegalArgumentException("keys are not sorted: " + vals);
            }
            prev_key=key;

            if(prev_val != null) {
                if(val <= prev_val)
                    throw new IllegalArgumentException("values are not sorted: " + vals);
            }
            prev_val=val;
            max_block_times.put(key, val);
        }
        if(log.isDebugEnabled())
            log.debug("max_block_times: " + max_block_times);
    }

    @ManagedAttribute
    public String getMaxBlockTimes() {
        if(max_block_times == null) return "n/a";
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(Map.Entry<Long,Long> entry: max_block_times.entrySet()) {
            if(!first) {
                sb.append(", ");
            }
            else {
                first=false;
            }
            sb.append(entry.getKey()).append(":").append(entry.getValue());
        }
        return sb.toString();
    }
    
    @ManagedAttribute(description="Total time (ms) spent in flow control block")
    public long getTotalTimeBlocked() {
        return total_time_blocking;
    }

    @ManagedAttribute(description="Average time spent in a flow control block")
    public double getAverageTimeBlocked() {
        return num_blockings == 0? 0.0 : total_time_blocking / (double)num_blockings;
    }

    @ManagedAttribute(description="Number of credit requests received")
    public int getNumberOfCreditRequestsReceived() {
        return num_credit_requests_received;
    }
    
    @ManagedAttribute(description="Number of credit requests sent")
    public int getNumberOfCreditRequestsSent() {
        return num_credit_requests_sent;
    }

    @ManagedAttribute(description="Number of credit responses received")
    public int getNumberOfCreditResponsesReceived() {
        return num_credit_responses_received;
    }

    @ManagedAttribute(description="Number of credit responses sent")
    public int getNumberOfCreditResponsesSent() {
        return num_credit_responses_sent;
    }

    @ManagedOperation(description="Print sender credits")
    public String printSenderCredits() {
        return printMap(sent);
    }

    @ManagedOperation(description="Print receiver credits")
    public String printReceiverCredits() {
        return printMap(received);
    }

    @ManagedOperation(description="Print credits")
    public String printCredits() {
        StringBuilder sb=new StringBuilder();
        sb.append("senders:\n").append(printMap(sent)).append("\n\nreceivers:\n").append(printMap(received));
        return sb.toString();
    }

    public Map<String, Object> dumpStats() {
        Map<String, Object> retval=super.dumpStats();      
        retval.put("senders", printMap(sent));
        retval.put("receivers", printMap(received));       
        return retval;
    }

    @ManagedOperation(description="Print last blocking times")
    public String showLastBlockingTimes() {
        return last_blockings.toString();
    }


    private long getMaxBlockTime(long length) {
        if(max_block_times == null)
            return 0;
        Long retval=null;
        for(Map.Entry<Long,Long> entry: max_block_times.entrySet()) {
            retval=entry.getValue();
            if(length <= entry.getKey())
                break;
        }

        return retval != null? retval : 0;
    }

    /**
     * Allows to unblock a blocked sender from an external program, e.g. JMX
     */
    @ManagedOperation(description="Unblock a sender")
    public void unblock() {
        sent_lock.lock();
        try {
            if(log.isTraceEnabled())
                log.trace("unblocking the sender and replenishing all members, creditors are " + creditors);

            for(Map.Entry<Address, Long> entry: sent.entrySet()) {
                entry.setValue(max_credits);
            }

            lowest_credit=computeLowestCredit(sent);
            creditors.clear();
            credits_available.signalAll();
        }
        finally {
            sent_lock.unlock();
        }
    }

    public void init() throws Exception {
        boolean min_credits_set = min_credits != 0;
        if(!min_credits_set)
            min_credits=(long)(max_credits * min_threshold);
        
        Util.checkBufferSize("FC.max_credits", max_credits);       
    }

    public void start() throws Exception {
        super.start();
        if(!frag_size_received) {
            log.warn("No fragmentation protocol was found. When flow control (e.g. FC or SFC) is used, we recommend " +
                    "a fragmentation protocol, due to http://jira.jboss.com/jira/browse/JGRP-590");
        }

        sent_lock.lock();
        try {
            running=true;
            lowest_credit=max_credits;
        }
        finally {
            sent_lock.unlock();
        }
    }

    public void stop() {
        super.stop();
        sent_lock.lock();
        try {
            running=false;
            ignore_thread.set(false);
            credits_available.signalAll(); // notify all threads waiting on the mutex that we are done
        }
        finally {
            sent_lock.unlock();
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                return handleDownMessage(evt);
            case Event.CONFIG:
                handleConfigEvent((Map<String,Object>)evt.getArg()); 
                break;
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:

                // JGRP-465. We only deal with msgs to avoid having to use a concurrent collection; ignore views,
                // suspicions, etc which can come up on unusual threads.
                Message msg=(Message)evt.getArg();
                FcHeader hdr=(FcHeader)msg.getHeader(name);
                if(hdr != null) {
                    switch(hdr.type) {
                        case FcHeader.REPLENISH:
                            num_credit_responses_received++;
                            handleCredit(msg.getSrc(), (Number)msg.getObject());
                            break;
                        case FcHeader.CREDIT_REQUEST:
                            num_credit_requests_received++;
                            Address sender=msg.getSrc();
                            Long sent_credits=(Long)msg.getObject();
                            handleCreditRequest(received, received_lock, sender, sent_credits);
                            break;
                        default:
                            log.error("header type " + hdr.type + " not known");
                            break;
                    }
                    return null; // don't pass message up
                }

                Address sender=msg.getSrc();
                long new_credits=adjustCredit(received, received_lock, sender, msg.getLength());
                
                // JGRP-928: changed ignore_thread to a ThreadLocal: multiple threads can access it with the
                // introduction of the concurrent stack
                if(ignore_synchronous_response)
                    ignore_thread.set(true);
                try {
                    return up_prot.up(evt);
                }
                finally {
                    if(ignore_synchronous_response)
                        ignore_thread.set(false); // need to revert because the thread is placed back into the pool
                    if(new_credits > 0) {
                        if(log.isTraceEnabled()) log.trace("sending " + new_credits + " credits to " + sender);
                        sendCredit(sender, new_credits);
                    }
                }

            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;

            case Event.CONFIG:
                Map<String,Object> map=(Map<String,Object>)evt.getArg();
                handleConfigEvent(map);
                break;
        }
        return up_prot.up(evt);
    }


    private void handleConfigEvent(Map<String,Object> info) {
        if(info != null) {
            Integer frag_size=(Integer)info.get("frag_size");
            if(frag_size != null) {
                if(frag_size > max_credits) {
                    log.warn("The fragmentation size of the fragmentation protocol is " + frag_size +
                            ", which is greater than the max credits. While this is not incorrect, " +
                            "it may lead to long blockings. Frag size should be less than max_credits " +
                            "(http://jira.jboss.com/jira/browse/JGRP-590)");
                }
                frag_size_received=true;
            }
        }
    }

    private Object handleDownMessage(Event evt) {
        Message msg=(Message)evt.getArg();
        int length=msg.getLength();
        Address dest=msg.getDest();

        if(max_block_times != null) {
            long tmp=getMaxBlockTime(length);
            if(tmp > 0)
                end_time.set(System.currentTimeMillis() + tmp);
        }

        sent_lock.lock();
        try {
            if(length > lowest_credit) { // then block and loop asking for credits until enough credits are available
                if(ignore_synchronous_response && ignore_thread.get()) { // JGRP-465
                    if(log.isTraceEnabled())
                        log.trace("bypassing blocking to avoid deadlocking " + Thread.currentThread());
                }
                else {
                    determineCreditors(dest, length);
                    long start_blocking=System.currentTimeMillis();
                    num_blockings++; // we count overall blockings, not blockings for *all* threads
                    if(log.isTraceEnabled())
                        log.trace("Starting blocking. lowest_credit=" + lowest_credit + "; msg length =" + length);

                    while(length > lowest_credit && running) {
                        try {
                            long block_time=max_block_time;
                            if(max_block_times != null) {
                                Long tmp=end_time.get();
                                if(tmp != null) {
                                    // Negative value means we don't wait at all ! If the end_time already elapsed
                                    // (because we waited for other threads to get processed), the message will not
                                    // block at all and get sent immediately
                                    block_time=tmp - start_blocking;
                                }
                            }

                            boolean rc=credits_available.await(block_time, TimeUnit.MILLISECONDS);
                            if(rc || length <= lowest_credit || !running)
                                break;

                            // if we use max_block_times, then we do *not* send credit requests, even if we run
                            // into timeouts: in this case, it is up to the receivers to send new credits
                            if(!rc && max_block_times != null)
                                break;

                            long wait_time=System.currentTimeMillis() - last_credit_request;
                            if(wait_time >= max_block_time) {

                                // we have to set this var now, because we release the lock below (for sending a
                                // credit request), so all blocked threads would send a credit request, leading to
                                // a credit request storm
                                last_credit_request=System.currentTimeMillis();

                                // we need to send the credit requests down *without* holding the sent_lock, otherwise we might
                                // run into the deadlock described in http://jira.jboss.com/jira/browse/JGRP-292
                                Map<Address,Long> sent_copy=new HashMap<Address,Long>(sent);
                                sent_copy.keySet().retainAll(creditors);
                                sent_lock.unlock();
                                try {
                                    // System.out.println(new Date() + " --> credit request");
                                    for(Map.Entry<Address,Long> entry: sent_copy.entrySet()) {
                                        sendCreditRequest(entry.getKey(), entry.getValue());
                                    }
                                }
                                finally {
                                    sent_lock.lock();
                                }
                            }
                        }
                        catch(InterruptedException e) {
                            // set the interrupted flag again, so the caller's thread can handle the interrupt as well

                            // bela June 15 2007: don't do this as this will trigger an infinite loop !!
                            // (http://jira.jboss.com/jira/browse/JGRP-536)
                            // Thread.currentThread().interrupt();
                        }
                    }
                    // if(!running) // don't send the message if not running anymore
                       // return null;

                    long block_time=System.currentTimeMillis() - start_blocking;
                    if(log.isTraceEnabled())
                        log.trace("total time blocked: " + block_time + " ms");
                    total_time_blocking+=block_time;
                    last_blockings.add(block_time);
                }
            }

            long tmp=decrementCredit(sent, dest, length);
            if(tmp != -1)
                lowest_credit=Math.min(tmp, lowest_credit);
        }
        finally {
            sent_lock.unlock();
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(evt);
    }

    /**
     * Checks whether one member (unicast msg) or all members (multicast msg) have enough credits. Add those
     * that don't to the creditors list. Called with sent_lock held
     * @param dest
     * @param length
     */
    private void determineCreditors(Address dest, int length) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        Address mbr;
        Long credits;
        if(multicast) {
            for(Map.Entry<Address,Long> entry: sent.entrySet()) {
                mbr=entry.getKey();
                credits=entry.getValue();
                if(credits <= length)
                    creditors.add(mbr);
            }
        }
        else {
            credits=sent.get(dest);
            if(credits != null && credits <= length)
                creditors.add(dest);
        }
    }


    /**
     * Decrements credits from a single member, or all members in sent_msgs, depending on whether it is a multicast
     * or unicast message. No need to acquire mutex (must already be held when this method is called)
     * @param dest
     * @param credits
     * @return The lowest number of credits left, or -1 if a unicast member was not found
     */
    private long decrementCredit(Map<Address, Long> m, Address dest, long credits) {
        boolean multicast=dest == null || dest.isMulticastAddress();
        long lowest=max_credits, new_credit;
        Long val;

        if(multicast) {
            if(m.isEmpty())
                return -1;
            for(Map.Entry<Address, Long> entry: m.entrySet()) {
                val=entry.getValue();
                new_credit=val - credits;
                entry.setValue(new_credit);
                lowest=Math.min(new_credit, lowest);
            }
            return lowest;
        }
        else {
            val=m.get(dest);
            if(val != null) {
                lowest=val;
                lowest-=credits;
                m.put(dest, lowest);
                if(log.isTraceEnabled())
                	log.trace("sender " + dest + " minus " + credits
							+ " credits, " + lowest + " remaining");
                return lowest;
            }
        }
        return -1;
    }


    private void handleCredit(Address sender, Number increase) {
        if(sender == null) return;
        StringBuilder sb=null;

        sent_lock.lock();
        try {
            Long old_credit=sent.get(sender);
            if(old_credit == null)
                return;
            Long new_credit=Math.min(max_credits, old_credit + increase.longValue());

            if(log.isTraceEnabled()) {
                sb=new StringBuilder();
                sb.append("received credit from ").append(sender).append(", old credit was ").append(old_credit)
                        .append(", new credits are ").append(new_credit).append(".\nCreditors before are: ").append(creditors);
            }

            sent.put(sender, new_credit);
            lowest_credit=computeLowestCredit(sent);
            // boolean was_empty=true;
            if(!creditors.isEmpty()) {  // we are blocked because we expect credit from one or more members
                // was_empty=false;
                creditors.remove(sender);
                if(log.isTraceEnabled()) {
                    sb.append("\nCreditors after removal of ").append(sender).append(" are: ").append(creditors);
                    log.trace(sb);
                }
            }
            if(creditors.isEmpty()) {// && !was_empty) {
                credits_available.signalAll();
            }
        }
        finally {
            sent_lock.unlock();
        }
    }

    private static long computeLowestCredit(Map<Address, Long> m) {
        Collection<Long> credits=m.values(); // List of Longs (credits)
        return Collections.min(credits);
    }


    /**
     * Check whether sender has enough credits left. If not, send him some more
     * @param map The hashmap to use
     * @param lock The lock which can be used to lock map
     * @param sender The address of the sender
     * @param length The number of bytes received by this message. We don't care about the size of the headers for
     * the purpose of flow control
     * @return long Number of credits to be sent. Greater than 0 if credits needs to be sent, 0 otherwise
     */
    private long adjustCredit(Map<Address,Long> map, final Lock lock, Address sender, int length) {
        if(sender == null) {
            if(log.isErrorEnabled()) log.error("src is null");
            return 0;
        }

        if(length == 0)
            return 0; // no effect

        lock.lock();
        try {
            long remaining_cred=decrementCredit(map, sender, length);
            if(log.isTraceEnabled())
                log.trace("sender " + sender + " minus " + length
						+ " credits, " + remaining_cred + " remaining");
            if(remaining_cred == -1)
                return 0;
            long credit_response=max_credits - remaining_cred;
            if(credit_response >= min_credits) {
                map.put(sender, max_credits);
                return credit_response; // this will trigger sending of new credits as we have received more than min_credits bytes from src
            }
        }
        finally {
            lock.unlock();
        }
        return 0;
    }

    /**
     * @param map The map to modify
     * @param lock The lock to lock map
     * @param sender The sender who requests credits
     * @param left_credits Number of bytes that the sender has left to send messages to us
     */
    private void handleCreditRequest(Map<Address,Long> map, Lock lock, Address sender, Long left_credits) {
        if(sender == null) return;
        long credit_response=0;

        lock.lock();
        try {
            Long old_credit=map.get(sender);
            if(old_credit != null) {
                credit_response=Math.min(max_credits, max_credits - old_credit);
            }

            if(credit_response > 0) {
                if(log.isTraceEnabled())
                    log.trace("received credit request from " + sender + ": sending " + credit_response + " credits");
                map.put(sender, max_credits);
                pending_requesters.remove(sender);
            }
            else {
                if(pending_requesters.contains(sender)) {
                    // a sender might have negative credits, e.g. -20000. If we subtracted -20000 from max_credits,
                    // we'd end up with max_credits + 20000, and send too many credits back. So if the sender's
                    // credits is negative, we simply send max_credits back
                    long credits_left=Math.max(0, left_credits.longValue());
                    credit_response=max_credits - credits_left;
                    // credit_response = max_credits;
                    map.put(sender, max_credits);
                    pending_requesters.remove(sender);
                    if(log.isWarnEnabled())
                        log.warn("Received two credit requests from " + sender +
                                " without any intervening messages; sending " + credit_response + " credits");
                }
                else {
                    pending_requesters.add(sender);
                    if(log.isTraceEnabled())
                        log.trace("received credit request from " + sender + " but have no credits available");
                }
            }
        }
        finally {
            lock.unlock();
        }

        if(credit_response > 0)
            sendCredit(sender, credit_response);
    }


    private void sendCredit(Address dest, long credit) {
        if(log.isTraceEnabled())
            log.trace("replentished " + dest + " with " + credit
					+ " credits");
        Number number;
        if(credit < Integer.MAX_VALUE)
            number=(int)credit;
        else
            number=credit;
        Message msg=new Message(dest, null, number);
        msg.setFlag(Message.OOB);
        msg.putHeader(name, REPLENISH_HDR);
        down_prot.down(new Event(Event.MSG, msg));
        num_credit_responses_sent++;
    }

    /**
     * We cannot send this request as OOB messages, as the credit request needs to queue up behind the regular messages;
     * if a receiver cannot process the regular messages, that is a sign that the sender should be throttled !
     * @param dest The member to which we send the credit request
     * @param credits_left The number of bytes (of credits) left for dest
     */
    private void sendCreditRequest(final Address dest, Long credits_left) {
        if(log.isTraceEnabled())
            log.trace("sending credit request to " + dest);
        Message msg=new Message(dest, null, credits_left);
        msg.putHeader(name, CREDIT_REQUEST_HDR);
        down_prot.down(new Event(Event.MSG, msg));
        num_credit_requests_sent++;
    }


    private void handleViewChange(Vector<Address> mbrs) {
        Address addr;
        if(mbrs == null) return;
        if(log.isTraceEnabled()) log.trace("new membership: " + mbrs);

        sent_lock.lock();
        received_lock.lock();
        try {
            // add members not in membership to received and sent hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=mbrs.elementAt(i);
                if(!received.containsKey(addr))
                    received.put(addr, max_credits);
                if(!sent.containsKey(addr))
                    sent.put(addr, max_credits);
            }
            // remove members that left
            for(Iterator<Address> it=received.keySet().iterator(); it.hasNext();) {
                addr=it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }

            // remove members that left
            for(Iterator<Address> it=sent.keySet().iterator(); it.hasNext();) {
                addr=it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }

            // remove all creditors which are not in the new view
            /*for(Address creditor: creditors) {
                if(!mbrs.contains(creditor))
                    creditors.remove(creditor);
            }*/
            // fixed http://jira.jboss.com/jira/browse/JGRP-754 (CCME)
            for(Iterator<Address> it=creditors.iterator(); it.hasNext();) {
                Address creditor=it.next();
                if(!mbrs.contains(creditor))
                    it.remove();
            }

            if(log.isTraceEnabled()) log.trace("creditors are " + creditors);
            if(creditors.isEmpty()) {
                lowest_credit=computeLowestCredit(sent);
                credits_available.signalAll();
            }
        }
        finally {
            sent_lock.unlock();
            received_lock.unlock();
        }
    }

    private static String printMap(Map<Address,Long> m) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Long> entry: m.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }


    public static class FcHeader extends Header implements Streamable {
        public static final byte REPLENISH=1;
        public static final byte CREDIT_REQUEST=2; // the sender of the message is the requester

        byte type=REPLENISH;
        private static final long serialVersionUID=8226510881574318828L;

        public FcHeader() {

        }

        public FcHeader(byte type) {
            this.type=type;
        }

        public int size() {
            return Global.BYTE_SIZE;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
        }

        public String toString() {
            switch(type) {
                case REPLENISH:
                    return "REPLENISH";
                case CREDIT_REQUEST:
                    return "CREDIT_REQUEST";
                default:
                    return "<invalid type>";
            }
        }
    }


}

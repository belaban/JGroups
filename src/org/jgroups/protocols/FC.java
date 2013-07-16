package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.BoundedList;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
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
 * @deprecated Succeeded by MFC and UFC
 */
@MBean(description="Simple flow control protocol based on a credit system")
@Deprecated
public class FC extends Protocol {

    private final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
    private final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);  

    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    /**
     * Max number of bytes to send per receiver until an ack must be received before continuing sending
     */
    @Property(description="Max number of bytes to send per receiver until an ack must be received to proceed. Default is 500000 bytes")
    private long max_credits=500000;

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to wait forever.
     */
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
     * If we've received (min_threshold * max_credits) bytes from P, we send more credits to P. Example: if
     * max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits to P once we've
     * received 250'000 bytes from P. 
     */
    @Property(description="The threshold (as a percentage of max_credits) at which a receiver sends more credits to " +
            "a sender. Example: if max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits " +
            "to P once we've received 250'000 bytes from P")
    private double min_threshold=0.60;

    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    @Property(description="Computed as max_credits x min_theshold unless explicitly set")
    private long min_credits=0;
    
    /**
     * Whether an up thread that comes back down should be allowed to
     * bypass blocking if all credits are exhausted. Avoids JGRP-465.
     * Set to false by default in 2.5 because we have OOB messages for credit replenishments - this flag should not be set
     * to true if the concurrent stack is used
     */
    @Property(description="Does not block a down message if it is a result of handling an up message in the" +
            "same thread. Fixes JGRP-928", deprecatedMessage="not used any longer")
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
    @GuardedBy("lock")
    private final ConcurrentMap<Address,Credit> sent=Util.createConcurrentMap();

    /**
     * Keeps track of credits / member at the receiver's side. Keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits fall below the threshold, we refill and send a REPLENISH message to the sender.
     * The sender blocks until REPLENISH message is received.
     */
    private final ConcurrentMap<Address,Credit> received=Util.createConcurrentMap();


    /**
     * List of members from whom we expect credits
     */
    @GuardedBy("lock")
    private final Set<Address> creditors=new HashSet<Address>(11);

    
    /**
     * Whether FC is still running, this is set to false when the protocol terminates (on stop())
     */
    private volatile boolean running=true;


    private boolean frag_size_received=false;

   
    /**
     * the lowest credits of any destination (sent_msgs)
     */
    @GuardedBy("lock")
    @ManagedAttribute(writable=false)
    private long lowest_credit=max_credits;

    /** Lock protecting sent credits table and some other vars (creditors for example) */
    private final Lock lock=new ReentrantLock();


    /** Mutex to block on down() */
    private final Condition credits_available=lock.newCondition();
   


    /** Last time a credit request was sent. Used to prevent credit request storms */
    @GuardedBy("lock")
    private long last_credit_request=0;   

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

    @Property(description="Max times to block for the listed messages sizes (Message.getLength()). Example: \"1000:10,5000:30,10000:500\"")
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

    @ManagedOperation(description="Prints the creditors")
    public String printCreditors() {
        return creditors.toString();
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
        lock.lock();
        try {
            if(log.isTraceEnabled())
                log.trace("unblocking the sender and replenishing all members, creditors are " + creditors);

            for(Map.Entry<Address,Credit> entry: sent.entrySet())
                entry.getValue().set(max_credits);

            lowest_credit=computeLowestCredit(sent);
            creditors.clear();
            credits_available.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public void init() throws Exception {
        boolean min_credits_set = min_credits != 0;
        if(!min_credits_set)
            min_credits=(long)(max_credits * min_threshold);
        lowest_credit=max_credits;
    }

    public void start() throws Exception {
        super.start();
        if(!frag_size_received) {
            log.warn("No fragmentation protocol was found. When flow control (e.g. FC or SFC) is used, we recommend " +
                    "a fragmentation protocol, due to http://jira.jboss.com/jira/browse/JGRP-590");
        }

        lock.lock();
        try {
            running=true;
            lowest_credit=max_credits;
        }
        finally {
            lock.unlock();
        }
    }

    public void stop() {
        super.stop();
        lock.lock();
        try {
            running=false;
            credits_available.signalAll(); // notify all threads waiting on the mutex that we are done
        }
        finally {
            lock.unlock();
        }
    }


    @SuppressWarnings("unchecked")
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_FC))
                    break;
                int length=msg.getLength();
                if(length == 0)
                    break;
                return handleDownMessage(evt, msg, length);
            case Event.CONFIG:
                handleConfigEvent((Map<String,Object>)evt.getArg()); 
                break;
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }


    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:

                // JGRP-465. We only deal with msgs to avoid having to use a concurrent collection; ignore views,
                // suspicions, etc which can come up on unusual threads.
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_FC))
                    break;
                FcHeader hdr=(FcHeader)msg.getHeader(this.id);
                if(hdr != null) {
                    handleUpEvent(hdr, msg);
                    return null; // don't pass message up
                }

                Address sender=msg.getSrc();
                long new_credits=adjustCredit(received, sender, msg.getLength());
                
                try {
                    return up_prot.up(evt);
                }
                finally {
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


    public void up(MessageBatch batch) {
        int length=0;
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_FC))
                continue;
            FcHeader hdr=(FcHeader)msg.getHeader(this.id);
            if(hdr != null) {
                batch.remove(msg); // don't pass message up as part of the batch
                handleUpEvent(hdr, msg);
                continue;
            }
            length+=msg.getLength();
        }

        Address sender=batch.sender();
        long new_credits=0;
        if(length > 0)
            new_credits=adjustCredit(received, sender, length);


        if(!batch.isEmpty()) {
            try {
                up_prot.up(batch);
            }
            finally {
                if(new_credits > 0)
                    sendCredit(sender, new_credits);
            }
        }
    }

    protected void handleUpEvent(FcHeader hdr, Message msg) {
        switch(hdr.type) {
            case FcHeader.REPLENISH:
                num_credit_responses_received++;
                handleCredit(msg.getSrc(), (Number)msg.getObject());
                break;
            case FcHeader.CREDIT_REQUEST:
                num_credit_requests_received++;
                Address sender=msg.getSrc();
                Long sent_credits=(Long)msg.getObject();
                if(sent_credits != null)
                    handleCreditRequest(received, sender,sent_credits);
                break;
            default:
                log.error("header type " + hdr.type + " not known");
                break;
        }
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

    private Object handleDownMessage(final Event evt, final Message msg, int length) {
        Address dest=msg.getDest();

        if(max_block_times != null) {
            long tmp=getMaxBlockTime(length);
            if(tmp > 0)
                end_time.set(System.currentTimeMillis() + tmp);
        }

        lock.lock();
        try {
            if(length > lowest_credit) { // then block and loop asking for credits until enough credits are available
                determineCreditors(dest, length);
                long start_blocking=System.currentTimeMillis();
                num_blockings++; // we count overall blockings, not blockings for *all* threads
                if(log.isTraceEnabled())
                    log.trace("Blocking (lowest_credit=" + lowest_credit + "; length=" + length + ")");

                while(length > lowest_credit && running) {
                    try {
                        long block_time=max_block_time;
                        if(max_block_times != null) {
                            Long tmp=end_time.get();
                            if(tmp != null) {
                                // A negative block_time means we don't wait at all ! If the end_time already elapsed
                                // (because we waited for other threads to get processed), the message will not
                                // block at all and get sent immediately
                                block_time=tmp - start_blocking;
                            }
                        }

                        boolean rc=credits_available.await(block_time, TimeUnit.MILLISECONDS);
                        if(length <= lowest_credit || rc || !running)
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

                            // we need to send the credit requests down *without* holding the lock, otherwise we might
                            // run into the deadlock described in http://jira.jboss.com/jira/browse/JGRP-292
                            Map<Address,Credit> sent_copy=new HashMap<Address,Credit>(sent);
                            sent_copy.keySet().retainAll(creditors);
                            lock.unlock();
                            try {
                                for(Map.Entry<Address,Credit> entry: sent_copy.entrySet())
                                    sendCreditRequest(entry.getKey(), entry.getValue().get());
                            }
                            finally {
                                lock.lock();
                            }
                        }
                    }
                    catch(InterruptedException e) {
                        // bela June 15 2007: don't interrupt the thread again, as this will trigger an infinite loop !!
                        // (http://jira.jboss.com/jira/browse/JGRP-536)
                        // Thread.currentThread().interrupt();
                    }
                }
                long block_time=System.currentTimeMillis() - start_blocking;
                if(log.isTraceEnabled())
                    log.trace("total time blocked: " + block_time + " ms");
                total_time_blocking+=block_time;
                last_blockings.add(block_time);
            }

            long tmp=decrementCredit(sent, dest, length);
            if(tmp != -1)
                lowest_credit=Math.min(tmp, lowest_credit);
        }
        finally {
            lock.unlock();
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(evt);
    }

    /**
     * Checks whether one member (unicast msg) or all members (multicast msg) have enough credits. Add those
     * that don't to the creditors list. Called with lock held
     * @param dest
     * @param length
     */
    private void determineCreditors(Address dest, int length) {
        if(dest == null) {
            for(Map.Entry<Address,Credit> entry: sent.entrySet()) {
                if(entry.getValue().get() <= length)
                    creditors.add(entry.getKey());
            }
        }
        else {
            Credit cred=sent.get(dest);
            if(cred != null && cred.get() <= length)
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
    private long decrementCredit(Map<Address,Credit> map, Address dest, long credits) {
        long lowest=max_credits;
        if(dest == null) {
            if(map.isEmpty())
                return -1;
            for(Credit cred: map.values())
                lowest=Math.min(cred.decrement(credits), lowest);
            return lowest;
        }
        else {
            Credit cred=map.get(dest);
            if(cred != null)
                return lowest=cred.decrement(credits);
            }
        return -1;
    }


    private void handleCredit(Address sender, Number increase) {
        if(sender == null) return;
        StringBuilder sb=null;

        lock.lock();
        try {
            Credit cred=sent.get(sender);
            if(cred == null)
                return;
            long new_credit=Math.min(max_credits, cred.get() + increase.longValue());

            if(log.isTraceEnabled()) {
                sb=new StringBuilder();
                sb.append("received credit from ").append(sender).append(", old credit was ").append(cred)
                        .append(", new credits are ").append(new_credit).append(".\nCreditors before are: ").append(creditors);
            }

            cred.increment(increase.longValue());

            lowest_credit=computeLowestCredit(sent);
            if(!creditors.isEmpty() && creditors.remove(sender) && creditors.isEmpty())
                credits_available.signalAll();
            }
        finally {
            lock.unlock();
        }
    }

    private static long computeLowestCredit(Map<Address,Credit> m) {
        Collection<Credit> credits=m.values();
        return Collections.min(credits).get();
    }


    /**
     * Check whether sender has enough credits left. If not, send it some more
     * @param map The hashmap to use
     * @param sender The address of the sender
     * @param length The number of bytes received by this message. We don't care about the size of the headers for
     * the purpose of flow control
     * @return long Number of credits to be sent. Greater than 0 if credits needs to be sent, 0 otherwise
     */
    private long adjustCredit(Map<Address,Credit> map, Address sender, int length) {
        if(sender == null || length == 0)
            return 0;

        Credit cred=map.get(sender);
        if(cred == null)
            return 0;

        if(log.isTraceEnabled())
            log.trace("sender " + sender + " minus " + length + " credits, " + (cred.get() - length) + " remaining");

        return cred.decrementAndGet(length);
    }

    /**
     * @param map The map to modify
     * @param sender The sender who requests credits
     * @param left_credits Number of bytes that the sender has left to send messages to us
     */
    private void handleCreditRequest(Map<Address,Credit> map, Address sender, long left_credits) {
        if(sender == null) return;
        Credit cred=map.get(sender);
        if(cred == null) return;
        long credit_response=Math.min(max_credits - left_credits, max_credits);

        if(log.isTraceEnabled())
            log.trace("received credit request from " + sender + ": sending " + credit_response + " credits");
        cred.set(max_credits);
        sendCredit(sender, credit_response);
    }


    private void sendCredit(Address dest, long credit) {
        if(log.isTraceEnabled())
            log.trace("replenishing " + dest + " with " + credit	+ " credits");
        Number number;
        if(credit < Integer.MAX_VALUE)
            number=(int)credit;
        else
            number=credit;
        Message msg=new Message(dest, number).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id,REPLENISH_HDR);
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
        Message msg=new Message(dest, credits_left).setFlag(Message.Flag.DONT_BUNDLE, Message.Flag.INTERNAL)
          .putHeader(this.id,CREDIT_REQUEST_HDR);
        down_prot.down(new Event(Event.MSG, msg));
        num_credit_requests_sent++;
    }


    private void handleViewChange(List<Address> mbrs) {
        if(mbrs == null) return;
        if(log.isTraceEnabled()) log.trace("new membership: " + mbrs);

        lock.lock();
        try {
            // add members not in membership to received and sent hashmap (with full credits)
            for(Address addr: mbrs) {
                if(!received.containsKey(addr))
                    received.put(addr, new Credit(max_credits));
                if(!sent.containsKey(addr))
                    sent.put(addr, new Credit(max_credits));
            }
            // remove members that left
            for(Iterator<Address> it=received.keySet().iterator(); it.hasNext();) {
                Address addr=it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }

            // remove members that left
            for(Iterator<Address> it=sent.keySet().iterator(); it.hasNext();) {
                Address addr=it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }

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
            lock.unlock();
        }
    }

    private static String printMap(Map<Address,Credit> m) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Credit> entry: m.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }



    private class Credit implements Comparable {
        private long credits_left;

        private Credit(long credits) {
            this.credits_left=credits;
        }

        private synchronized long decrementAndGet(long credits) {
            credits_left=Math.max(0, credits_left - credits);
            long credit_response=max_credits - credits_left;
            if(credit_response >= min_credits) {
                credits_left=max_credits;
                return credit_response;
            }
            return 0;
        }

        private synchronized long decrement(long credits) {
            return credits_left=Math.max(0, credits_left - credits);
        }

        private synchronized long get() {return credits_left;}

        private synchronized void set(long new_credits) {credits_left=Math.min(max_credits, new_credits);}

        private synchronized long increment(long credits) {
            return credits_left=Math.min(max_credits, credits_left + credits);
        }

        public String toString() {
            return String.valueOf(credits_left);
        }

        public int compareTo(Object o) {
            Credit other=(Credit)o;
            return credits_left < other.credits_left ? -1 : credits_left > other.credits_left ? 1 : 0;
        }
    }





}

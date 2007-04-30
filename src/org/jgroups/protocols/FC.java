package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.GuardedBy;
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
 * @version $Id: FC.java,v 1.79 2007/04/30 08:31:19 belaban Exp $
 */
public class FC extends Protocol {

    /**
     * Map<Address,Long>: keys are members, values are credits left. For each send, the
     * number of credits is decremented by the message size. A HashMap rather than a ConcurrentHashMap is
     * currently used as there might be null values
     */
    @GuardedBy("lock")
    final Map<Address, Long> sent=new HashMap<Address, Long>(11);
    // final Map sent=new ConcurrentHashMap(11);

    /**
     * Map<Address,Long>: keys are members, values are credits left (in bytes).
     * For each receive, the credits for the sender are decremented by the size of the received message.
     * When the credits are 0, we refill and send a CREDIT message to the sender. Sender blocks until CREDIT
     * is received after reaching <tt>min_credits</tt> credits.
     */
    final Map<Address, Long> received=new ConcurrentHashMap<Address, Long>(11);


    /**
     * List of members from whom we expect credits
     */
    @GuardedBy("lock")
    final Set<Address> creditors=new HashSet<Address>(11);

    /** Peers who have asked for credit that we didn't have */
    final Set<Address> pending_requesters=new HashSet<Address>(11);

    /**
     * Max number of bytes to send per receiver until an ack must
     * be received before continuing sending
     */
    private long max_credits=500000;
    private Long max_credits_constant=new Long(max_credits);

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to
     * wait forever.
     */
    private long max_block_time=5000;

    /**
     * If credits fall below this limit, we send more credits to the sender. (We also send when
     * credits are exhausted (0 credits left))
     */
    private double min_threshold=0.25;

    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    private long min_credits=0;

    /**
     * Whether FC is still running, this is set to false when the protocol terminates (on stop())
     */
    private boolean running=true;

    /**
     * Determines whether or not to block on down(). Set when not enough credit is available to send a message
     * to all or a single member
     */
    @GuardedBy("lock")
    private boolean insufficient_credit=false;

    /**
     * the lowest credits of any destination (sent_msgs)
     */
    @GuardedBy("lock")
    private long lowest_credit=max_credits;

    /**
     * Lock to be used with the Condvar below
     */
    final Lock lock=new ReentrantLock();

    /**
     * Mutex to block on down()
     */
    final Condition mutex=lock.newCondition();

    /**
     * Whether an up thread that comes back down should be allowed to
     * bypass blocking if all credits are exhausted. Avoids JGRP-465.
     * Set to false by default in 2.5 because we have OOB messages for credit replenishments - this flag should not be set
     * to true if the concurrent stack is used
     */
    private boolean ignore_synchronous_response=false;

    /**
     * Thread that carries messages through up() and shouldn't be blocked
     * in down() if ignore_synchronous_response==true. JGRP-465.
     */
    private Thread ignore_thread;

    static final String name="FC";

    /** Last time a credit request was sent. Used to prevent credit request storms */
    private long last_credit_request=0;

    private int num_blockings=0;
    private int num_credit_requests_received=0, num_credit_requests_sent=0;
    private int num_credit_responses_sent=0, num_credit_responses_received=0;
    private long total_time_blocking=0;

    final BoundedList last_blockings=new BoundedList(50);

    final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
    final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);


    public final String getName() {
        return name;
    }

    public void resetStats() {
        super.resetStats();
        num_blockings=0;
        num_credit_responses_sent=num_credit_responses_received=num_credit_requests_received=num_credit_requests_sent=0;
        total_time_blocking=0;
        last_blockings.removeAll();
    }

    public long getMaxCredits() {
        return max_credits;
    }

    public void setMaxCredits(long max_credits) {
        this.max_credits=max_credits;
        max_credits_constant=new Long(this.max_credits);
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

    public boolean isBlocked() {
        return insufficient_credit;
    }

    public int getNumberOfBlockings() {
        return num_blockings;
    }

    public long getMaxBlockTime() {
        return max_block_time;
    }

    public void setMaxBlockTime(long t) {
        max_block_time=t;
    }

    public long getTotalTimeBlocked() {
        return total_time_blocking;
    }

    public double getAverageTimeBlocked() {
        return num_blockings == 0? 0.0 : total_time_blocking / (double)num_blockings;
    }

    public int getNumberOfCreditRequestsReceived() {
        return num_credit_requests_received;
    }

    public int getNumberOfCreditRequestsSent() {
        return num_credit_requests_sent;
    }

    public int getNumberOfCreditResponsesReceived() {
        return num_credit_responses_received;
    }

    public int getNumberOfCreditResponsesSent() {
        return num_credit_responses_sent;
    }

    public String printSenderCredits() {
        return printMap(sent);
    }

    public String printReceiverCredits() {
        return printMap(received);
    }

    public String printCredits() {
        StringBuilder sb=new StringBuilder();
        sb.append("senders:\n").append(printMap(sent)).append("\n\nreceivers:\n").append(printMap(received));
        return sb.toString();
    }

    public Map<String, Object> dumpStats() {
        Map<String, Object> retval=super.dumpStats();
        if(retval == null)
            retval=new HashMap<String, Object>();
        retval.put("senders", printMap(sent));
        retval.put("receivers", printMap(received));
        retval.put("num_blockings", new Integer(this.num_blockings));
        retval.put("avg_time_blocked", new Double(getAverageTimeBlocked()));
        retval.put("num_replenishments", new Integer(this.num_credit_responses_received));
        retval.put("total_time_blocked", new Long(total_time_blocking));
        return retval;
    }

    public String showLastBlockingTimes() {
        return last_blockings.toString();
    }


    /**
     * Allows to unblock a blocked sender from an external program, e.g. JMX
     */
    public void unblock() {
        lock.lock();
        try {
            if(log.isTraceEnabled())
                log.trace("unblocking the sender and replenishing all members, creditors are " + creditors);

            for(Map.Entry<Address, Long> entry: sent.entrySet()) {
                entry.setValue(max_credits_constant);
            }

            lowest_credit=computeLowestCredit(sent);
            creditors.clear();
            insufficient_credit=false;
            mutex.signalAll();
        }
        finally {
            lock.unlock();
        }
    }


    public boolean setProperties(Properties props) {
        String str;
        boolean min_credits_set=false;

        super.setProperties(props);
        str=props.getProperty("max_credits");
        if(str != null) {
            max_credits=Long.parseLong(str);
            props.remove("max_credits");
        }

        str=props.getProperty("min_threshold");
        if(str != null) {
            min_threshold=Double.parseDouble(str);
            props.remove("min_threshold");
        }

        str=props.getProperty("min_credits");
        if(str != null) {
            min_credits=Long.parseLong(str);
            props.remove("min_credits");
            min_credits_set=true;
        }

        if(!min_credits_set)
            min_credits=(long)((double)max_credits * min_threshold);

        str=props.getProperty("max_block_time");
        if(str != null) {
            max_block_time=Long.parseLong(str);
            props.remove("max_block_time");
        }

        Util.checkBufferSize("FC.max_credits", max_credits);
        str=props.getProperty("ignore_synchronous_response");
        if(str != null) {
            ignore_synchronous_response=Boolean.valueOf(str).booleanValue();
            props.remove("ignore_synchronous_response");
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        max_credits_constant=new Long(max_credits);
        return true;
    }

    public void start() throws Exception {
        super.start();
        lock.lock();
        try {
            running=true;
            insufficient_credit=false;
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
            ignore_thread=null;
            mutex.signalAll(); // notify all threads waiting on the mutex that we are done
        }
        finally {
            lock.unlock();
        }
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                return handleDownMessage(evt);
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }


    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:

                // JGRP-465. We only deal with msgs to avoid having to use a concurrent collection; ignore views,
                // suspicions, etc which can come up on unusual threads.
                if(ignore_thread == null && ignore_synchronous_response)
                    ignore_thread=Thread.currentThread();

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
                            handleCreditRequest(sender, sent_credits);
                            break;
                        default:
                            log.error("header type " + hdr.type + " not known");
                            break;
                    }
                    return null; // don't pass message up
                }
                else {
                    Address sender=msg.getSrc();
                    long new_credits=adjustCredit(sender, msg.getLength());
                    try {
                        return up_prot.up(evt);
                    }
                    finally {
                        if(new_credits > 0) {
                            if(log.isTraceEnabled()) log.trace("sending " + new_credits + " credits to " + sender);
                            sendCredit(sender, new_credits);
                        }
                    }
                }

            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return up_prot.up(evt);
    }


    private Object handleDownMessage(Event evt) {
        Message msg=(Message)evt.getArg();
        int length=msg.getLength();
        Address dest=msg.getDest();

        lock.lock();
        try {
            if(length > lowest_credit) { // then block and loop asking for credits until enough credits are available
                if(ignore_synchronous_response && ignore_thread == Thread.currentThread()) { // JGRP-465
                    if(log.isTraceEnabled())
                        log.trace("Bypassing blocking to avoid deadlocking " + Thread.currentThread());
                }
                else {
                    determineCreditors(dest, length);
                    insufficient_credit=true;
                    long start_blocking=System.currentTimeMillis();
                    num_blockings++; // we count overall blockings, not blockings for *all* threads
                    if(log.isTraceEnabled())
                        log.trace("Starting blocking. lowest_credit=" + lowest_credit + "; msg length =" + length);

                    while(insufficient_credit && running) {
                        try {
                            mutex.await(max_block_time, TimeUnit.MILLISECONDS);
                        }
                        catch(InterruptedException e) {
                            // set the interrupted flag again, so the caller's thread can handle the interrupt as well
                            Thread.currentThread().interrupt();
                        }
                        if(insufficient_credit && running) {
                            long wait_time=System.currentTimeMillis() - last_credit_request;
                            if(wait_time >= max_block_time) {
                                // we need to send the credit requests down *without* holding the lock, otherwise we might
                                // run into the deadlock described in http://jira.jboss.com/jira/browse/JGRP-292
                                Map<Address,Long> sent_copy=new HashMap<Address,Long>(sent);
                                sent_copy.keySet().retainAll(creditors);
                                lock.unlock();
                                try {
                                    // System.out.println(new Date() + " --> credit request");
                                    for(Map.Entry<Address,Long> entry: sent_copy.entrySet()) {
                                        sendCreditRequest(entry.getKey(), entry.getValue());
                                    }
                                }
                                finally {
                                    lock.lock();
                                }
                                last_credit_request=System.currentTimeMillis();
                            }
                        }
                    }

                    long block_time=System.currentTimeMillis() - start_blocking;
                    if(log.isTraceEnabled())
                        log.trace("total time blocked: " + block_time + " ms");
                    total_time_blocking+=block_time;
                    last_blockings.add(new Long(block_time));
                }
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
        boolean multicast=dest == null || dest.isMulticastAddress();
        Address mbr;
        Long credits;
        if(multicast) {
            for(Map.Entry<Address,Long> entry: sent.entrySet()) {
                mbr=entry.getKey();
                credits=entry.getValue();
                if(credits.longValue() <= length)
                    creditors.add(mbr);
            }
        }
        else {
            credits=sent.get(dest);
            if(credits != null && credits.longValue() <= length)
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
                lowest=val.longValue();
                lowest-=credits;
                m.put(dest, lowest);
                return lowest;
            }
        }
        return -1;
    }


    private void handleCredit(Address sender, Number increase) {
        if(sender == null) return;
        StringBuilder sb=null;

        lock.lock();
        try {
            Long old_credit=sent.get(sender);
            Long new_credit=Math.min(max_credits, old_credit.longValue() + increase.longValue());

            if(log.isTraceEnabled()) {
                sb=new StringBuilder();
                sb.append("received credit from ").append(sender).append(", old credit was ").append(old_credit)
                        .append(", new credits are ").append(new_credit).append(".\nCreditors before are: ").append(creditors);
            }

            sent.put(sender, new_credit);
            lowest_credit=computeLowestCredit(sent);
            if(!creditors.isEmpty()) {  // we are blocked because we expect credit from one or more members
                creditors.remove(sender);
                if(log.isTraceEnabled()) {
                    sb.append("\nCreditors after removal of ").append(sender).append(" are: ").append(creditors);
                    log.trace(sb);
                }
            }
            if(insufficient_credit && lowest_credit > 0 && creditors.isEmpty()) {
                insufficient_credit=false;
                mutex.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
    }

    private static long computeLowestCredit(Map<Address, Long> m) {
        Collection<Long> credits=m.values(); // List of Longs (credits)
        Long retval=Collections.min(credits);
        return retval.longValue();
    }


    /**
     * Check whether sender has enough credits left. If not, send him some more
     * @param src The address of the sender
     * @param length The number of bytes received by this message. We don't care about the size of the headers for
     * the purpose of flow control
     * @return long Number of credits to be sent. Greater than 0 if credits needs to be sent, 0 otherwise
     */
    private long adjustCredit(Address src, int length) {
        if(src == null) {
            if(log.isErrorEnabled()) log.error("src is null");
            return 0;
        }

        if(length == 0)
            return 0; // no effect

        long remaining_cred=decrementCredit(received, src, length);
        long credit_response=max_credits - remaining_cred;
        if(credit_response >= min_credits) {
            received.put(src, max_credits_constant);
            return credit_response; // this will trigger sending of new credits as we have received more than min_credits bytes from src
        }
        return 0;
    }

    /**
     *
     * @param sender The sender who requests credits
     * @param left_credits Number of bytes that the sender has left to send messages to us
     */
    private void handleCreditRequest(Address sender, Long left_credits) {
        if(sender == null) return;

        lock.lock();
        long credit_response=0;
        try {
            Long old_credit=received.get(sender);
            if(old_credit != null) {
                credit_response=max_credits - old_credit.longValue();
            }

            if(credit_response > 0) {
                if(log.isTraceEnabled())
                    log.trace("received credit request from " + sender + ": sending " + credit_response + " credits");
                received.put(sender, max_credits_constant);
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
                    received.put(sender, max_credits_constant);
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
        Number number;
        if(credit < Integer.MAX_VALUE)
            number=new Integer((int)credit);
        else
            number=new Long(credit);
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


    private void handleViewChange(Vector mbrs) {
        Address addr;
        if(mbrs == null) return;
        if(log.isTraceEnabled()) log.trace("new membership: " + mbrs);

        lock.lock();
        try {
            // add members not in membership to received and sent hashmap (with full credits)
            for(int i=0; i < mbrs.size(); i++) {
                addr=(Address)mbrs.elementAt(i);
                if(!received.containsKey(addr))
                    received.put(addr, max_credits_constant);
                if(!sent.containsKey(addr))
                    sent.put(addr, max_credits_constant);
            }
            // remove members that left
            for(Iterator it=received.keySet().iterator(); it.hasNext();) {
                addr=(Address)it.next();
                if(!mbrs.contains(addr))
                    it.remove();
            }

            // remove members that left
            for(Iterator it=sent.keySet().iterator(); it.hasNext();) {
                addr=(Address)it.next();
                if(!mbrs.contains(addr))
                    it.remove(); // modified the underlying map
            }

            // remove all creditors which are not in the new view
            for(Address creditor: creditors) {
                if(!mbrs.contains(creditor))
                    creditors.remove(creditor);
            }

            if(log.isTraceEnabled()) log.trace("creditors are " + creditors);
            if(insufficient_credit && creditors.isEmpty()) {
                lowest_credit=computeLowestCredit(sent);
                insufficient_credit=false;
                mutex.signalAll();
            }
        }
        finally {
            lock.unlock();
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

        public FcHeader() {

        }

        public FcHeader(byte type) {
            this.type=type;
        }

        public long size() {
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

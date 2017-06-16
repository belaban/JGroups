package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.Credit;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender.
 * 
 * @author Bela Ban
 */
@MBean(description="Simple flow control protocol based on a credit system")
public abstract class FlowControl extends Protocol {

    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    /**
     * Max number of bytes to send per receiver until an ack must be received before continuing sending
     */
    @Property(description="Max number of bytes to send per receiver until an ack must be received to proceed")
    protected long           max_credits=500000;

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to wait forever.
     */
    @Property(description="Max time (in ms) to block")
    protected long           max_block_time=500;

    /**
     * Defines the max number of milliseconds for a message to block before being sent, based on the length of
     * the message. The property is defined as a comma-separated list of values (separated by ':'), where the key
     * is the size in bytes and the value is the number of milliseconds to block.
     * Example: max_block_times="50:1,500:3,1500:5,10000:10,100000:100". This means that messages up to 50 bytes wait
     * 1 ms max until they get sent, messages up to 500 bytes 3 ms, and so on.
     * If a message's length (size of the payload in bytes) is for example 15'000 bytes,
     * FlowControl blocks it for a max of 100 ms.
     */
    protected Map<Long,Long> max_block_times;


    /**
     * If we're down to (min_threshold * max_credits) bytes for P, we send more credits to P. Example: if
     * max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits to P once we've got only
     * 250'000 credits left for P (we've received 750'000 bytes from P).
     */
    @Property(description="The threshold (as a percentage of max_credits) at which a receiver sends more credits to " +
            "a sender. Example: if max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits " +
            "to P once we've got only 250'000 credits left for P (we've received 750'000 bytes from P)")
    protected double         min_threshold=0.40;

    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    @Property(description="Computed as max_credits x min_theshold unless explicitly set")
    protected long           min_credits;
    

    
    
    /* ---------------------------------------------   JMX      -------------------------------------------------- */
    protected int  num_credit_requests_received, num_credit_requests_sent;
    protected int  num_credit_responses_sent, num_credit_responses_received;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
   

    /**
     * Keeps track of credits per member at the receiver. For each message, the credits for the sender are decremented
     * by the size of the received message. When the credits fall below the threshold, we refill and send a REPLENISH
     * message to the sender.
     */
    protected final Map<Address,Credit> received=Util.createConcurrentMap();

    protected Address                   local_addr;


    /** Whether FlowControl is still running, this is set to false when the protocol terminates (on stop()) */
    protected volatile boolean          running=true;

    protected int                       frag_size; // remember frag_size from the fragmentation protocol





    public void resetStats() {
        super.resetStats();
        num_credit_responses_sent=num_credit_responses_received=num_credit_requests_received=num_credit_requests_sent=0;
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
            max_block_times=new TreeMap<>();
        for(String tmp: vals) {
            int index=tmp.indexOf(':');
            if(index == -1)
                throw new IllegalArgumentException("element '" + tmp + "'  is missing a ':' separator");
            Long key=Long.parseLong(tmp.substring(0, index).trim());
            Long val=Long.parseLong(tmp.substring(index +1).trim());

            // sanity checks:
            if(key < 0 || val < 0)
                throw new IllegalArgumentException("keys and values must be >= 0");

            if(prev_key != null && key <= prev_key)
                throw new IllegalArgumentException("keys are not sorted: " + vals);
            prev_key=key;

            if(prev_val != null && val <= prev_val)
                throw new IllegalArgumentException("values are not sorted: " + vals);
            prev_val=val;
            max_block_times.put(key, val);
        }
        log.debug("max_block_times: %s", max_block_times);
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

    public abstract int getNumberOfBlockings();

    public abstract double getAverageTimeBlocked();

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
    
    public abstract String printSenderCredits();

    @ManagedOperation(description="Print receiver credits")
    public String printReceiverCredits() {
        return printMap(received);
    }


    public String printCredits() {
        StringBuilder sb=new StringBuilder();
        sb.append("receivers:\n").append(printMap(received));
        return sb.toString();
    }


    protected long getMaxBlockTime(long length) {
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
     * Whether the protocol handles message with dest == null || dest.isMulticastAddress()
     * @return
     */
    protected abstract boolean handleMulticastMessage();

    protected abstract void    handleCredit(Address sender, long increase);

    protected abstract Header  getReplenishHeader();
    protected abstract Header  getCreditRequestHeader();



    /**
     * Allows to unblock all blocked senders from an external program, e.g. JMX
     */
    @ManagedOperation(description="Unblocks all senders")
    public void unblock() {
        ;
    }

    public void init() throws Exception {
        boolean min_credits_set = min_credits != 0;
        if(!min_credits_set)
            min_credits=(long)(max_credits * min_threshold);
    }

    public void start() throws Exception {
        super.start();
        boolean is_udp_transport=getTransport().isMulticastCapable();
        if(is_udp_transport && frag_size <= 0)
            log.warn("No fragmentation protocol was found. When flow control is used, we recommend " +
                       "a fragmentation protocol, due to http://jira.jboss.com/jira/browse/JGRP-590");
        if(frag_size > 0 && frag_size >= min_credits) {
            log.warn("The fragmentation size of the fragmentation protocol is %d, which is greater than min_credits (%d). " +
                       "This can lead to blockings (https://issues.jboss.org/browse/JGRP-1659)", frag_size, min_credits);
        }
        running=true;
    }

    public void stop() {
        super.stop();
        running=false;
    }


    @SuppressWarnings("unchecked")
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.CONFIG:
                handleConfigEvent(evt.getArg());
                break;
            
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }


    public Object down(Message msg) {
        if(msg.isFlagSet(Message.Flag.NO_FC))
            return down_prot.down(msg);

        Address dest=msg.getDest();
        boolean multicast=dest == null;
        boolean handle_multicasts=handleMulticastMessage();
        boolean process=(handle_multicasts && multicast) || (!handle_multicasts && !multicast);
        if(!process)
            return down_prot.down(msg);

        int length=msg.getLength();
        if(length == 0)
            return down_prot.down(msg);

        Object retval=handleDownMessage(msg);

        // if the message is DONT_LOOPBACK, we will not receive it, therefore the credit
        // check needs to be done now
        if(msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK)) {
            long new_credits=adjustCredit(received, local_addr, length);
            if(new_credits > 0)
                sendCredit(local_addr, new_credits);
        }
        return retval;
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        if(msg.isFlagSet(Message.Flag.NO_FC))
            return up_prot.up(msg);

        Address dest=msg.getDest();
        boolean multicast=dest == null;
        boolean handle_multicasts=handleMulticastMessage();
        FcHeader hdr=msg.getHeader(this.id);
        boolean process=(handle_multicasts && multicast) || (!handle_multicasts && !multicast) || hdr != null;
        if(!process)
            return up_prot.up(msg);

        if(hdr != null) {
            handleUpEvent(msg, hdr);
            return null; // don't pass message up
        }

        try {
            return up_prot.up(msg);
        }
        finally {
            int length=msg.getLength();
            if(length > 0) {
                Address sender=msg.getSrc();
                long new_credits=adjustCredit(received, sender, length);
                if(new_credits > 0)
                    sendCredit(sender, new_credits);
            }
        }
    }

    protected void handleUpEvent(final Message msg, FcHeader hdr) {
        switch(hdr.type) {
            case FcHeader.REPLENISH:
                num_credit_responses_received++;
                handleCredit(msg.getSrc(), bufferToLong(msg.getRawBuffer(), msg.getOffset()));
                break;
            case FcHeader.CREDIT_REQUEST:
                num_credit_requests_received++;
                Address sender=msg.getSrc();
                Long requested_credits=bufferToLong(msg.getRawBuffer(), msg.getOffset());
                if(requested_credits != null)
                    handleCreditRequest(received, sender,requested_credits);
                break;
            default:
                log.error(Util.getMessage("HeaderTypeNotKnown"), local_addr, hdr.type);
                break;
        }
    }


    public void up(MessageBatch batch) {
        int length=0;
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_FC))
                continue;

            Address dest=msg.getDest();
            boolean multicast=dest == null;
            boolean handle_multicasts=handleMulticastMessage();
            FcHeader hdr=msg.getHeader(this.id);
            boolean process=(handle_multicasts && multicast) || (!handle_multicasts && !multicast) || hdr != null;
            if(!process)
                continue;

            if(hdr != null) {
                batch.remove(msg); // remove the message with a flow control header so it won't get passed up
                handleUpEvent(msg,hdr);
                continue;
            }
            length+=msg.getLength();
        }

        if(!batch.isEmpty()) {
            try {
                up_prot.up(batch);
            }
            finally {
                if(length > 0) {
                    Address sender=batch.sender();
                    long new_credits=adjustCredit(received, sender, length);
                    if(new_credits > 0)
                        sendCredit(sender, new_credits);
                }
            }
        }
    }


    protected void handleConfigEvent(Map<String,Object> info) {
        if(info != null) {
            Integer tmp=(Integer)info.get("frag_size");
            if(tmp != null)
                this.frag_size=tmp;
        }
    }

    
    protected abstract Object handleDownMessage(final Message msg);



    /**
     * Check whether sender has enough credits left. If not, send it some more
     * @param map The hashmap to use
     * @param sender The address of the sender
     * @param length The number of bytes received by this message. We don't care about the size of the headers for
     * the purpose of flow control
     * @return long Number of credits to be sent. Greater than 0 if credits needs to be sent, 0 otherwise
     */
    protected long adjustCredit(Map<Address,Credit> map, Address sender, int length) {
        Credit cred;
        if(sender == null || length == 0 || (cred=map.get(sender)) == null)
            return 0;
        if(log.isTraceEnabled())
            log.trace("%s used %d credits, %d remaining", sender, length, cred.get() - length);
        return cred.decrementAndGet(length, min_credits, max_credits);
    }

    /**
     * @param map The map to modify
     * @param sender The sender who requests credits
     * @param requested_credits Number of bytes that the sender has left to send messages to us
     */
    protected void handleCreditRequest(Map<Address,Credit> map, Address sender, long requested_credits) {
        if(requested_credits > 0 && sender != null) {
            Credit cred=map.get(sender);
            if(cred == null)
                return;
            if(log.isTraceEnabled())
                log.trace("received credit request from %s: sending %d credits", sender, requested_credits);
            cred.increment(requested_credits, max_credits);
            sendCredit(sender, requested_credits);
        }
    }


    protected void sendCredit(Address dest, long credits) {
        if(log.isTraceEnabled())
            log.trace("sending %d credits to %s", credits, dest);
        Message msg=new Message(dest, longToBuffer(credits))
          .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id,getReplenishHeader());
        down_prot.down(msg);
        num_credit_responses_sent++;
    }

    /**
     * We cannot send this request as OOB message, as the credit request needs to queue up behind the regular messages;
     * if a receiver cannot process the regular messages, that is a sign that the sender should be throttled !
     * @param dest The member to which we send the credit request
     * @param credits_needed The number of bytes (of credits) left for dest
     */
    protected void sendCreditRequest(final Address dest, long credits_needed) {
        if(log.isTraceEnabled())
            log.trace("sending request for %d credits to %s", credits_needed, dest);
        Message msg=new Message(dest, longToBuffer(credits_needed))
          .setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id, getCreditRequestHeader());
        down_prot.down(msg);
        num_credit_requests_sent++;
    }


    protected void handleViewChange(List<Address> mbrs) {
        if(mbrs == null) return;
        if(log.isTraceEnabled()) log.trace("new membership: %s", mbrs);

        // add members not in membership to received and sent hashmap (with full credits)
        mbrs.stream().filter(addr -> !received.containsKey(addr)).forEach(addr -> received.put(addr, new Credit(max_credits)));

        // remove members that left
        received.keySet().retainAll(mbrs);
    }



    protected static String printMap(Map<Address,? extends Credit> m) {
        return m.entrySet().stream().collect(StringBuilder::new,
                                             (sb,entry) -> sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n"),
                                             (l,r) -> {}).toString();
    }

    protected static byte[] longToBuffer(long num) {
        byte[] buf=new byte[Global.LONG_SIZE];
        Bits.writeLong(num, buf, 0);
        return buf;
    }

    protected static long bufferToLong(byte[] buf, int offset) {
        return Bits.readLong(buf, offset);
    }





}

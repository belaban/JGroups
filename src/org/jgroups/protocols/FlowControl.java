package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Average;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;


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

    protected final static FcHeader REPLENISH_HDR=new FcHeader(FcHeader.REPLENISH);
    protected final static FcHeader CREDIT_REQUEST_HDR=new FcHeader(FcHeader.CREDIT_REQUEST);  

    
    /* -----------------------------------------    Properties     -------------------------------------------------- */
    
    /**
     * Max number of bytes to send per receiver until an ack must be received before continuing sending
     */
    @Property(description="Max number of bytes to send per receiver until an ack must be received to proceed")
    protected long max_credits=500000;

    /**
     * Max time (in milliseconds) to block. If credit hasn't been received after max_block_time, we send
     * a REPLENISHMENT request to the members from which we expect credits. A value <= 0 means to wait forever.
     */
    @Property(description="Max time (in ms) to block")
    protected long max_block_time=5000;

    /**
     * Defines the max number of milliseconds for a message to block before being sent, based on the length of
     * the message. The property is defined as a comma-separated list of values (separated by ':'), where the key
     * is the size in bytes and the value is the number of milliseconds to block.
     * Example: max_block_times="50:1,500:3,1500:5,10000:10,100000:100". This means that messages up to 50 bytes wait
     * 1 ms max until they get sent, messages up to 500 bytes 3 ms, and so on.
     * If a message's length (size of the payload in bytes) is for example 15'000 bytes,
     * FlowControl blocks it for a max of 100 ms.
     */
    protected Map<Long,Long> max_block_times=null;


    /**
     * If we're down to (min_threshold * max_credits) bytes for P, we send more credits to P. Example: if
     * max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits to P once we've got only
     * 250'000 credits left for P (we've received 750'000 bytes from P).
     */
    @Property(description="The threshold (as a percentage of max_credits) at which a receiver sends more credits to " +
            "a sender. Example: if max_credits is 1'000'000, and min_threshold 0.25, then we send ca. 250'000 credits " +
            "to P once we've got only 250'000 credits left for P (we've received 750'000 bytes from P)")
    protected double min_threshold=0.40;

    /**
     * Computed as <tt>max_credits</tt> times <tt>min_theshold</tt>. If explicitly set, this will
     * override the above computation
     */
    @Property(description="Computed as max_credits x min_theshold unless explicitly set")
    protected long min_credits=0;
    
    /**
     * Whether an up thread that comes back down should be allowed to bypass blocking if all credits are exhausted.
     * Avoids JGRP-465. Set to false by default in 2.5 because we have OOB messages for credit replenishments -
     * this flag should not be set to true if the concurrent stack is used
     */
    @Property(description="Does not block a down message if it is a result of handling an up message in the" +
            "same thread. Fixes JGRP-928",deprecatedMessage="not used any longer")
    protected boolean ignore_synchronous_response=false;
    
    
    
    
    /* ---------------------------------------------   JMX      ------------------------------------------------------ */
    protected int  num_credit_requests_received=0, num_credit_requests_sent=0;
    protected int  num_credit_responses_sent=0, num_credit_responses_received=0;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
   

    /**
     * Keeps track of credits per member at the receiver. For each message, the credits for the sender are decremented
     * by the size of the received message. When the credits fall below the threshold, we refill and send a REPLENISH
     * message to the sender.
     */
    protected final Map<Address,Credit> received=Util.createConcurrentMap();

    protected Address local_addr;


    /** Whether FlowControl is still running, this is set to false when the protocol terminates (on stop()) */
    protected volatile boolean running=true;

    
    protected int frag_size;

   



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

    public Map<String, Object> dumpStats() {
        Map<String, Object> retval=super.dumpStats();      
        retval.put("receivers", printMap(received));
        return retval;
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

    protected abstract void handleCredit(Address sender, long increase);


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
            log.warn("The fragmentation size of the fragmentation protocol is " + frag_size +
                       ", which is greater than min_credits (" + min_credits +  "). This can lead to blockings " +
                       "(https://issues.jboss.org/browse/JGRP-1659)");
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
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_FC))
                    break;

                Address dest=msg.getDest();
                boolean multicast=dest == null;
                boolean handle_multicasts=handleMulticastMessage();
                boolean process=(handle_multicasts && multicast) || (!handle_multicasts && !multicast);
                if(!process)
                    break;

                int length=msg.getLength();
                if(length == 0)
                    break;

                Object retval=handleDownMessage(evt, msg, dest, length);

                // if the message is DONT_LOOPBACK, we will not receive it, therefore the credit
                // check needs to be done now
                if(msg.isTransientFlagSet(Message.TransientFlag.DONT_LOOPBACK)) {
                    long new_credits=adjustCredit(received, local_addr, length);
                    if(new_credits > 0)
                        sendCredit(local_addr, new_credits);
                }
                return retval;

            case Event.CONFIG:
                handleConfigEvent((Map<String,Object>)evt.getArg()); 
                break;
            
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }


    @SuppressWarnings("unchecked")
    public Object up(Event evt) {
        switch(evt.getType()) {

            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_FC))
                    break;

                Address dest=msg.getDest();
                boolean multicast=dest == null;
                boolean handle_multicasts=handleMulticastMessage();
                FcHeader hdr=(FcHeader)msg.getHeader(this.id);
                boolean process=(handle_multicasts && multicast) || (!handle_multicasts && !multicast) || hdr != null;
                if(!process)
                    break;
                
                if(hdr != null) {
                    handleUpEvent(msg, hdr);
                    return null; // don't pass message up
                }

                Address sender=msg.getSrc();
                long new_credits=adjustCredit(received, sender, msg.getLength());
                
                try {
                    return up_prot.up(evt);
                }
                finally {
                    if(new_credits > 0)
                        sendCredit(sender, new_credits);
                }

            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return up_prot.up(evt);
    }


    protected void handleUpEvent(final Message msg, FcHeader hdr) {
        switch(hdr.type) {
            case FcHeader.REPLENISH:
                num_credit_responses_received++;
                handleCredit(msg.getSrc(), (Long)msg.getObject());
                break;
            case FcHeader.CREDIT_REQUEST:
                num_credit_requests_received++;
                Address sender=msg.getSrc();
                Long requested_credits=(Long)msg.getObject();
                if(requested_credits != null)
                    handleCreditRequest(received, sender,requested_credits);
                break;
            default:
                log.error("header type " + hdr.type + " not known");
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
            FcHeader hdr=(FcHeader)msg.getHeader(this.id);
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


    protected void handleConfigEvent(Map<String,Object> info) {
        if(info != null) {
            Integer tmp=(Integer)info.get("frag_size");
            if(tmp != null)
                this.frag_size=tmp;
        }
    }

    
    protected abstract Object handleDownMessage(final Event evt, final Message msg, Address dest, int length);



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
            log.trace(sender + " used " + length + " credits, " + (cred.get() - length) + " remaining");
        return cred.decrementAndGet(length);
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
                log.trace("received credit request from " + sender + ": sending " + requested_credits + " credits");
            cred.increment(requested_credits);
            sendCredit(sender, requested_credits);
        }
    }


    protected void sendCredit(Address dest, long credits) {
        if(log.isTraceEnabled())
            if(log.isTraceEnabled()) log.trace("sending " + credits + " credits to " + dest);
        Message msg=new Message(dest, credits).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id,REPLENISH_HDR);
        down_prot.down(new Event(Event.MSG, msg));
        num_credit_responses_sent++;
    }

    /**
     * We cannot send this request as OOB message, as the credit request needs to queue up behind the regular messages;
     * if a receiver cannot process the regular messages, that is a sign that the sender should be throttled !
     * @param dest The member to which we send the credit request
     * @param credits_needed The number of bytes (of credits) left for dest
     */
    protected void sendCreditRequest(final Address dest, Long credits_needed) {
        if(log.isTraceEnabled())
            log.trace("sending request for " + credits_needed + " credits to " + dest);
        Message msg=new Message(dest, credits_needed).setFlag(Message.Flag.OOB, Message.Flag.INTERNAL, Message.Flag.DONT_BUNDLE)
          .putHeader(this.id, CREDIT_REQUEST_HDR);
        down_prot.down(new Event(Event.MSG, msg));
        num_credit_requests_sent++;
    }


    protected void handleViewChange(List<Address> mbrs) {
        if(mbrs == null) return;
        if(log.isTraceEnabled()) log.trace("new membership: " + mbrs);

        // add members not in membership to received and sent hashmap (with full credits)
        for(Address addr: mbrs) {
            if(!received.containsKey(addr))
                received.put(addr, new Credit(max_credits, null));
        }
        // remove members that left
        for(Iterator<Address> it=received.keySet().iterator(); it.hasNext();) {
            Address addr=it.next();
            if(!mbrs.contains(addr))
                it.remove();
        }
    }



    protected static String printMap(Map<Address,Credit> m) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Credit> entry: m.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }



    protected class Credit {
        protected long          credits_left;
        protected int           num_blockings;
        protected long          last_credit_request; // ns
        protected final Average avg_blockings;

        
        protected Credit(long credits, Average avg_blockings) {
            this.credits_left=credits;
            this.avg_blockings=avg_blockings;
        }

        public void reset() {num_blockings=0; if(avg_blockings != null) avg_blockings.clear();}

        protected synchronized boolean decrementIfEnoughCredits(long credits, long timeout) {
            if(decrement(credits))
                return true;

            if(timeout <= 0)
                return false;

            long start=avg_blockings != null? System.nanoTime() : 0;
            try {
                this.wait(timeout);
            }
            catch(InterruptedException e) {
            }
            finally {
                num_blockings++;
                if(avg_blockings != null)
                    avg_blockings.add(System.nanoTime() - start);
            }

            return decrement(credits);
        }

        
        protected boolean decrement(long credits) {
            if(credits <= credits_left) {
                credits_left-=credits;
                return true;
            }
            return false;
        }


        protected synchronized long decrementAndGet(long credits) {
            credits_left=Math.max(0, credits_left - credits);
            if(credits_left <= min_credits) {
                long credit_response=Math.min(max_credits, max_credits - credits_left);
                credits_left=max_credits;
                return credit_response;
            }
            return 0;
        }


        protected synchronized void increment(long credits) {
            credits_left=Math.min(max_credits, credits_left + credits);
            notifyAll();
        }

        protected synchronized boolean needToSendCreditRequest() {
            long current_time=System.nanoTime();
            // will most likely send a request the first time (last_credit_request is 0), unless nanoTime() is negative
            if(current_time - last_credit_request >= TimeUnit.NANOSECONDS.convert(max_block_time, TimeUnit.MILLISECONDS)) {
                last_credit_request=current_time;
                return true;
            }
            return false;
        }

        protected int getNumBlockings() {return num_blockings;}

        protected synchronized long get() {return credits_left;}

        protected synchronized void set(long new_credits) {
            credits_left=Math.min(max_credits, new_credits);
            notifyAll();
        }

        public String toString() {
            return String.valueOf(credits_left);
        }

    }


}

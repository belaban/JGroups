package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.Average;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


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
 * <li>Receivers don't send the full credits (max_credits), but rather the actual number of bytes received
 * <ol/>
 * @author Bela Ban
 */
@MBean(description="Simple flow control protocol based on a credit system")
public class UFC extends FlowControl {
    protected final static FcHeader UFC_REPLENISH_HDR = new FcHeader(FcHeader.REPLENISH);
    protected final static FcHeader UFC_CREDIT_REQUEST_HDR = new FcHeader(FcHeader.CREDIT_REQUEST);

    /**
     * Map<Address,Long>: keys are members, values are credits left. For each send,
     * the number of credits is decremented by the message size
     */
    protected final Map<Address,Credit> sent=Util.createConcurrentMap();

    protected final Average             avg_block_time=new Average(); // in ns



    @ManagedOperation(description="Print sender credits")
    public String printSenderCredits() {
        return printMap(sent);
    }

    
    @ManagedOperation(description="Print credits")
    public String printCredits() {
        StringBuilder sb=new StringBuilder(super.printCredits());
        sb.append("\nsenders:\n").append(printMap(sent));
        return sb.toString();
    }

    public Map<String, Object> dumpStats() {
        Map<String, Object> retval=super.dumpStats();
        retval.put("senders", printMap(sent));
        return retval;
    }

    protected boolean handleMulticastMessage()          {return false;}
    @Override protected Header getReplenishHeader()     {return UFC_REPLENISH_HDR;}
    @Override protected Header getCreditRequestHeader() {return UFC_CREDIT_REQUEST_HDR;}



    public void unblock() {
        super.unblock();
    }

    @ManagedAttribute(description="Number of times flow control blocks sender")
    public int getNumberOfBlockings() {
        int retval=0;
        for(Credit cred: sent.values())
            retval+=cred.getNumBlockings();
        return retval;
    }

    @ManagedAttribute(description="Average time blocked (in ms) in flow control when trying to send a message")
    public double getAverageTimeBlocked() {
        return avg_block_time.getAverage() / 1000000.0;
    }

    public void init() throws Exception {
        super.init();
        TP transport=getTransport();
        if(transport instanceof BasicTCP)
            log.info(this.getClass().getSimpleName() + " is not needed (and can be removed) as we're running on a TCP transport");
    }

    public void stop() {
        super.stop();
        for(Credit cred: sent.values())
            cred.set(max_credits);
    }

    public void resetStats() {
        super.resetStats();
        avg_block_time.clear();
        for(Credit cred: sent.values())
            cred.reset();

    }

    protected Object handleDownMessage(final Event evt, final Message msg, Address dest, int length) {
        if(dest == null) { // 2nd line of defense, not really needed
            log.error("%s doesn't handle multicast messages; passing message down", getClass().getSimpleName());
            return down_prot.down(evt);
        }

        Credit cred=sent.get(dest);
        if(cred == null)
            return down_prot.down(evt);

        long block_time=max_block_times != null? getMaxBlockTime(length) : max_block_time;
        
        while(running && sent.containsKey(dest)) {
            boolean rc=cred.decrementIfEnoughCredits(length, block_time);
            if(rc || !running || max_block_times != null)
                break;

            if(cred.needToSendCreditRequest())
                sendCreditRequest(dest, Math.max(0, max_credits - cred.get()));
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(evt);
    }




    protected void handleViewChange(List<Address> mbrs) {
        super.handleViewChange(mbrs);
        if(mbrs == null) return;

        // add members not in membership to received and sent hashmap (with full credits)
        for(Address addr: mbrs) {
            if(!sent.containsKey(addr))
                sent.put(addr, new Credit(max_credits, avg_block_time));
        }

        // remove members that left
        for(Iterator<Address> it=sent.keySet().iterator(); it.hasNext();) {
            Address addr=it.next();
            if(!mbrs.contains(addr))
                it.remove(); // modified the underlying map
        }
    }


    protected void handleCredit(Address sender, long increase) {
        Credit cred;
        if(sender == null || (cred=sent.get(sender)) == null || increase <= 0)
            return;

        if(log.isTraceEnabled()) {
            long new_credit=Math.min(max_credits, cred.get() + increase);
            log.trace("received %d credits from %s, old credits: %s, new credits: %d", increase, sender, cred, new_credit);
        }
        cred.increment(increase);
    }
    

}

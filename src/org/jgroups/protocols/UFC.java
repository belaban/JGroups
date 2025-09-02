package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.Credit;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * Simple flow control protocol based on a credit system. Each sender has a number of credits (bytes
 * to send). When the credits have been exhausted, the sender blocks. Each receiver also keeps track of
 * how many credits it has received from a sender. When credits for a sender fall below a threshold,
 * the receiver sends more credits to the sender. Works for both unicast and multicast messages.
 * <p>
 * Note that this protocol must be located towards the top of the stack, or all down_threads from JChannel to this
 * protocol must be set to false ! This is in order to block JChannel.send()/JChannel.down().
 * <p>This is the second simplified implementation of the same model. The algorithm is sketched out in
 * doc/FlowControl.txt
 * <p>
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
    protected final Map<Address,? extends Credit> sent=Util.createConcurrentMap();



    @ManagedOperation(description="Print sender credits")
    public String printSenderCredits() {
        return printMap(sent);
    }

    
    @ManagedOperation(description="Print credits")
    public String printCredits() {
        return String.format("%s\nsenders:\n%s", super.printCredits(), printMap(sent));
    }


    protected boolean          handleMulticastMessage() {return false;}
    @Override protected Header getReplenishHeader()     {return UFC_REPLENISH_HDR;}
    @Override protected Header getCreditRequestHeader() {return UFC_CREDIT_REQUEST_HDR;}



    public void unblock() {
        super.unblock();
        sent.values().forEach(cred -> cred.increment(max_credits, max_credits));
    }

    public long getSenderCreditsFor(Address mbr) {
        Credit credits=sent.get(mbr);
        return credits == null? 0 : credits.get();
    }

    @ManagedAttribute(description="Number of times flow control blocks sender",type=AttributeType.SCALAR)
    public int getNumberOfBlockings() {
        int retval=0;
        for(Credit cred: sent.values())
            retval+=cred.getNumBlockings();
        return retval;
    }

    @ManagedAttribute(description="Average time blocked (in ms) in flow control when trying to send a message",
      type=AttributeType.TIME)
    public double getAverageTimeBlocked() {
        return sent.values().stream().mapToDouble(c -> c.getAverageBlockTime() / 1_000_000).average().orElse(0.0);
    }


    public void stop() {
        super.stop();
        unblock();
        sent.values().forEach(Credit::reset);
    }

    public void resetStats() {
        super.resetStats();
        sent.values().forEach(Credit::resetStats);
    }

    @Override
    protected Object handleDownMessage(final Message msg, int length) {
        Address dest=msg.getDest();
        if(dest == null) { // 2nd line of defense, not really needed
            log.error("%s doesn't handle multicast messages; passing message down", getClass().getSimpleName());
            return down_prot.down(msg);
        }

        Credit cred=sent.get(dest);
        if(cred == null)
            return down_prot.down(msg);

        while(running && sent.containsKey(dest)) {
            boolean rc=cred.decrementIfEnoughCredits(msg, length, max_block_time);
            if(rc || !running)
                break;

            if(cred.needToSendCreditRequest(max_block_time))
                sendCreditRequest(dest, Math.max(0, max_credits - cred.get()));
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(msg);
    }


    protected void handleViewChange(List<Address> mbrs) {
        super.handleViewChange(mbrs);
        if(mbrs == null) return;

        // add members not in membership to received and sent hashmap (with full credits)
        mbrs.stream().filter(addr -> !sent.containsKey(addr)).forEach(addr -> sent.put(addr, createCredit((int)max_credits)));

        // remove members that left
        Iterator<? extends Map.Entry<Address,? extends Credit>> it=sent.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<Address,? extends Credit> entry=it.next();
            Address addr=entry.getKey();
            if(!mbrs.contains(addr)) {
                Credit cred=entry.getValue();
                cred.reset();
                it.remove();
            }
        }
        // sent.keySet().retainAll(mbrs);
    }


    protected void handleCredit(Address sender, long increase) {
        Credit cred;
        if(sender == null || (cred=sent.get(sender)) == null || increase <= 0)
            return;

        if(log.isTraceEnabled()) {
            long new_credit=Math.min(max_credits, cred.get() + increase);
            log.trace("received %d credits from %s, old credits: %s, new credits: %d", increase, sender, cred, new_credit);
        }
        cred.increment(increase, max_credits);
    }

    protected <T extends Credit> T createCredit(int initial_credits) {
        return (T)new Credit(initial_credits);
    }
    

}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;
import java.util.Vector;


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
    
    /**
     * Map<Address,Long>: keys are members, values are credits left. For each send,
     * the number of credits is decremented by the message size
     */
    protected final Map<Address,Credit> sent=Util.createConcurrentMap();



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


    protected boolean handleMulticastMessage() {
        return false;
    }



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

    @ManagedAttribute(description="Total time (ms) spent in flow control block")
    public long getTotalTimeBlocked() {
        long retval=0;
        for(Credit cred: sent.values())
            retval+=cred.getTotalBlockingTime();
        return retval;
    }

    public void stop() {
        super.stop();
        for(Credit cred: sent.values())
            cred.set(max_credits);
    }




    protected Object handleDownMessage(final Event evt, final Message msg, Address dest, int length) {
        if(dest == null || dest.isMulticastAddress()) { // 2nd line of defense, not really needed
            log.error(getClass().getSimpleName() + " doesn't handle multicast messages; passing message down");
            return down_prot.down(evt);
        }

        Credit cred=sent.get(dest);
        if(cred == null) {
            log.warn("destination " + dest + " not found; passing message down");
            return down_prot.down(evt);
        }

        long block_time=max_block_times != null? getMaxBlockTime(length) : max_block_time;
        
        while(running) {
            boolean rc=cred.decrementIfEnoughCredits(length, block_time);
            if(rc || !running || max_block_times != null)
                break;

            if(cred.needToSendCreditRequest())
                sendCreditRequest(dest, Math.max(0, max_credits - cred.get()));
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(evt);
    }


    protected void handleViewChange(Vector<Address> mbrs) {
        super.handleViewChange(mbrs);
        if(mbrs == null) return;

        // add members not in membership to received and sent hashmap (with full credits)
        for(Address addr: mbrs) {
            if(!sent.containsKey(addr))
                sent.put(addr, new Credit(max_credits));
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

        long new_credit=Math.min(max_credits, cred.get() + increase);
        if(log.isTraceEnabled()) {
            StringBuilder sb=new StringBuilder();
            sb.append("received " + increase + " credits from ").append(sender).append(", old credits: ").append(cred)
                    .append(", new credits: ").append(new_credit);
            log.trace(sb);
        }
        cred.increment(increase);
    }
    

}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.CreditMap;
import org.jgroups.util.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


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
public class MFC extends FlowControl {
    protected final static FcHeader MFC_REPLENISH_HDR      = new FcHeader(FcHeader.REPLENISH);
    protected final static FcHeader MFC_CREDIT_REQUEST_HDR = new FcHeader(FcHeader.CREDIT_REQUEST);
    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    

    /** Maintains credits per member */
    protected CreditMap credits;

    
    /** Last time a credit request was sent. Used to prevent credit request storms */
    protected long last_credit_request; // ns

   

    /** Allows to unblock a blocked sender from an external program, e.g. JMX */
    @ManagedOperation(description="Unblock a sender")
    public void unblock() {
        credits.replenishAll();
    }

    @ManagedOperation(description="Print credits")
    public String printCredits() {
        return super.printCredits() + "\nsenders min credits: " + credits.computeLowestCreditWithAccumulated();
    }

    @ManagedOperation(description="Print sender credits")
    public String printSenderCredits() {
        return credits.toString();
    }

    @ManagedAttribute(description="Number of times flow control blocks sender")
    public int getNumberOfBlockings() {
        return credits.getNumBlockings();
    }

    @ManagedAttribute(description="Average time blocked (in ms) in flow control when trying to send a message")
    public double getAverageTimeBlocked() {
        return credits.getAverageBlockTime();
    }

    protected boolean          handleMulticastMessage() {return true;}
    @Override protected Header getReplenishHeader()     {return MFC_REPLENISH_HDR;}
    @Override protected Header getCreditRequestHeader() {return MFC_CREDIT_REQUEST_HDR;}

   
    public void init() throws Exception {
        super.init();
        credits=createCreditMap(max_credits);
    }

    public void stop() {
        super.stop();
        credits.clear();
    }

    public void resetStats() {
        super.resetStats();
        credits.reset();
    }

    protected CreditMap createCreditMap(long max_creds) {
        return new CreditMap(max_creds);
    }

    @Override
    protected Object handleDownMessage(final Message msg) {
        Address dest=msg.getDest();
        if(dest != null) // 2nd line of defense, not really needed
            return down_prot.down(msg);

        int length=msg.getLength();
        long block_time=max_block_times != null? getMaxBlockTime(length) : max_block_time;
        while(running) {
            boolean rc=credits.decrement(msg, length, block_time);
            if(rc || max_block_times != null || !running)
                break;

            if(needToSendCreditRequest()) {
                List<Tuple<Address,Long>> targets=credits.getMembersWithCreditsLessThan(min_credits);
                for(Tuple<Address,Long> tuple: targets)
                    sendCreditRequest(tuple.getVal1(), Math.min(max_credits, max_credits - tuple.getVal2()));
            }
        }
        
        // send message - either after regular processing, or after blocking (when enough credits are available again)
        return down_prot.down(msg);
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
  


    protected void handleCredit(Address sender, long increase) {
        credits.replenish(sender, increase);
        if(log.isTraceEnabled())
            log.trace("received %d credits from %s, new credits for %s: %d, min_credits=%d",
                      increase, sender, sender, credits.get(sender), credits.getMinCredits());
    }


    protected void handleViewChange(List<Address> mbrs) {
        super.handleViewChange(mbrs);

        Set<Address> keys=new HashSet<>(credits.keys());
        keys.stream().filter(key -> !mbrs.contains(key)).forEach(key -> credits.remove(key));
        mbrs.forEach(key -> credits.putIfAbsent(key));
    }




}

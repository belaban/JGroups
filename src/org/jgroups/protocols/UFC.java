package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;


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
 * @version $Id: UFC.java,v 1.2 2010/09/07 10:38:21 belaban Exp $
 */
@MBean(description="Simple flow control protocol based on a credit system")
public class UFC extends FlowControl {

    

    protected boolean handleMulticastMessage() {
        return false;
    }


    protected Credit createCredit(long credits) {
        return new UfcCredit(credits);
    }

    public void unblock() {
        super.unblock();
    }

    public double getAverageTimeBlocked() {
        int    blockings=0;
        long   total_time_blocked=0;

        for(Credit cred: sent.values()) {
            blockings+=((UfcCredit)cred).getNumBlockings();
            total_time_blocked=((UfcCredit)cred).getTotalBlockingTime();
        }

        return blockings > 0? total_time_blocked / (double)blockings : 0.0; // prevent div-by-zero
    }


    public int getNumberOfBlockings() {
        int retval=0;
        for(Credit cred: sent.values())
            retval+=((UfcCredit)cred).getNumBlockings();
        return retval;
    }

    public long getTotalTimeBlocked() {
        long retval=0;
        for(Credit cred: sent.values())
            retval+=((UfcCredit)cred).getTotalBlockingTime();
        return retval;
    }

    public void stop() {
        super.stop();
        for(final Credit cred: sent.values()) {
            synchronized(cred) {
                cred.set(max_credits);
                cred.notifyAll();
            }
        }
    }




    protected Object handleDownMessage(final Event evt, final Message msg, int length) {
        Address dest=msg.getDest();
        if(dest == null || dest.isMulticastAddress()) {
            log.error(getClass().getSimpleName() + " doesn't handle multicast messages; passing message down");
            return down_prot.down(evt);
        }

        if(ignore_synchronous_response && ignore_thread.get()) { // JGRP-465
            if(log.isTraceEnabled())
                log.trace("bypassing blocking to avoid deadlocking " + Thread.currentThread());
            return down_prot.down(evt);
        }

        UfcCredit cred=(UfcCredit)sent.get(dest);
        if(cred == null) {
            log.error("destination " + dest + " not found; passing message down");
            return down_prot.down(evt);
        }

        long block_time=max_block_times != null? getMaxBlockTime(length) : max_block_time;
        
        while(running) {
            if(cred.decrementIfEnoughCredits(length, 0)) // timeout == 0: don't block
                break;

            if(log.isTraceEnabled())
                log.trace("blocking for credits (for " + block_time + " ms)");
            boolean rc=cred.decrementIfEnoughCredits(length, block_time);
            if(rc && log.isTraceEnabled())
                log.trace("unblocking (received credits)");
            
            if(rc || !running || max_block_times != null)
                break;

            if(cred.sendCreditRequest(System.currentTimeMillis()))
                sendCreditRequest(dest, cred.get());
        }

        // send message - either after regular processing, or after blocking (when enough credits available again)
        return down_prot.down(evt);
    }
    


    protected void handleCredit(Address sender, Number increase) {
        if(sender == null) return;
        StringBuilder sb=null;

        Credit cred=sent.get(sender);
        if(cred == null)
            return;
        long new_credit=Math.min(max_credits, cred.get() + increase.longValue());

        if(log.isTraceEnabled()) {
            sb=new StringBuilder();
            sb.append("received " + increase + " credits from ").append(sender).append(", old credits: ").append(cred)
                    .append(", new credits: ").append(new_credit);
            log.trace(sb);
        }

        cred.increment(increase.longValue());
    }
    

    protected class UfcCredit extends Credit {
        int num_blockings=0;
        long total_blocking_time=0;
        long last_credit_request=0;

        protected UfcCredit(long credits) {
            super(credits);
        }

        protected synchronized boolean decrementIfEnoughCredits(long credits, long timeout) {
            if(credits <= credits_left) {
                credits_left-=credits;
                return true;
            }

            if(timeout <= 0)
                return false;

            long start=System.currentTimeMillis();
            try {
                this.wait(timeout);
            }
            catch(InterruptedException e) {
            }
            finally {
                total_blocking_time+=System.currentTimeMillis() - start;
                num_blockings++;
            }

            if(credits <= credits_left) {
                credits_left-=credits;
                return true;
            }
            return false;
        }

        protected synchronized long increment(long credits) {
            long retval=super.increment(credits);
            notifyAll();
            return retval;
        }

        protected synchronized boolean sendCreditRequest(long current_time) {
            if(current_time - last_credit_request >= max_block_time) {
                // we have to set this var now, because we release the lock below (for sending a credit request), so
                // all blocked threads would send a credit request, leading to a credit request storm
                last_credit_request=System.currentTimeMillis();
                return true;
            }
            return false;
        }

        protected int getNumBlockings() {return num_blockings;}

        protected long getTotalBlockingTime() {return total_blocking_time;}
    }


}

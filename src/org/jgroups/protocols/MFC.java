package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;

import java.util.*;
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
 * <li>Receivers don't send the full credits (max_credits), but rather the actual number of bytes received
 * <ol/>
 * @author Bela Ban
 * @version $Id: MFC.java,v 1.3 2010/09/07 10:38:32 belaban Exp $
 */
@MBean(description="Simple flow control protocol based on a credit system")
public class MFC extends FlowControl {

    
    
    /* --------------------------------------------- Fields ------------------------------------------------------ */
    
    
  
    /**
     * the lowest credits of any destination (sent_msgs)
     */
    @GuardedBy("lock")
    private long lowest_credit=max_credits;

    /** Lock protecting sent credits table and some other vars (creditors for example) */
    private final Lock lock=new ReentrantLock();


    /** List of members from whom we expect credits */
    @GuardedBy("lock")
    protected final Set<Address> creditors=new HashSet<Address>(11);


    /** Mutex to block on down() */
    private final Condition credits_available=lock.newCondition();
   

    /**
     * Allows to unblock a blocked sender from an external program, e.g. JMX
     */
    @ManagedOperation(description="Unblock a sender")
    public void unblock() {
        lock.lock();
        try {
            if(log.isTraceEnabled())
                log.trace("unblocking the sender and replenishing all members");

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
        super.init();
        lowest_credit=max_credits;
    }

    public void start() throws Exception {
        super.start();
        lowest_credit=max_credits;
    }

    public void stop() {
        super.stop();
        lock.lock();
        try {
            running=false;
            ignore_thread.set(false);
            credits_available.signalAll(); // notify all threads waiting on the mutex that we are done
        }
        finally {
            lock.unlock();
        }
    }

    protected boolean handleMulticastMessage() {
        return true;
    }

    protected Credit createCredit(long credits) {
        return new MfcCredit(credits);
    }




    protected Object handleDownMessage(final Event evt, final Message msg, int length) {
        Address dest=msg.getDest();
        if(dest != null && !dest.isMulticastAddress()) {
            log.error(getClass().getSimpleName() + " doesn't handle unicast messages; passing message down");
            return down_prot.down(evt);
        }

        lock.lock();
        try {
            if(length > lowest_credit) { // then block and loop asking for credits until enough credits are available
                if(ignore_synchronous_response && ignore_thread.get()) { // JGRP-465
                    if(log.isTraceEnabled())
                        log.trace("bypassing blocking to avoid deadlocking " + Thread.currentThread());
                }
                else {
                    determineCreditors(length);
                    num_blockings++; // we count overall blockings, not blockings for *all* threads
                    if(log.isTraceEnabled())
                        log.trace("Starting blocking. lowest_credit=" + lowest_credit + "; msg length =" + length);

                    long block_time=max_block_times != null? getMaxBlockTime(length) : max_block_time;
                    while(length > lowest_credit && running) {
                        try {
                            long start=System.currentTimeMillis();
                            boolean rc=credits_available.await(block_time, TimeUnit.MILLISECONDS);
                            total_time_blocking+=System.currentTimeMillis() - start;
                            if(length <= lowest_credit || rc || !running)
                                break;

                            // if we use max_block_times, then we do *not* send credit requests, even if we run
                            // into timeouts: in this case, it is up to the receivers to send new credits
                            if(!rc && max_block_times != null)
                                break;

                            long curr_time=System.currentTimeMillis();
                            long wait_time=curr_time - last_credit_request;
                            if(wait_time >= max_block_time) {

                                // we have to set this var now, because we release the lock below (for sending a
                                // credit request), so all blocked threads would send a credit request, leading to
                                // a credit request storm
                                last_credit_request=curr_time;

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
                }
            }

            long tmp=decrementCredit(sent, length);
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
     * Adds members which have not enough credits to the creditors list. Called with lock held
     * @param length
     */
    protected void determineCreditors(int length) {
        for(Map.Entry<Address,Credit> entry: sent.entrySet()) {
            if(entry.getValue().get() <= length)
                creditors.add(entry.getKey());
        }
    }


  

    /**
     * Decrements credits from a single member, or all members in sent_msgs, depending on whether it is a multicast
     * or unicast message. No need to acquire mutex (must already be held when this method is called)
     * @param dest
     * @param credits
     * @return The lowest number of credits left, or -1 if a unicast member was not found
     */
    protected long decrementCredit(Map<Address,Credit> map, long credits) {
        if(map.isEmpty())
            return -1;
        long lowest=max_credits;
        for(Credit cred: map.values())
            lowest=Math.min(cred.decrement(credits), lowest);
        return lowest;
    }

    protected void handleCredit(Address sender, Number increase) {
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
                sb.append("received " + increase + " credits from ").append(sender).append(", old credits: ").append(cred)
                        .append(", new credits: ").append(new_credit).append(".\nCreditors before are: ").append(creditors);
                log.trace(sb);
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


    protected void handleViewChange(Vector<Address> mbrs) {
        super.handleViewChange(mbrs);

        lock.lock();
        try {
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

    protected class MfcCredit extends Credit {

        protected MfcCredit(long credits) {
            super(credits);
        }
    }


}

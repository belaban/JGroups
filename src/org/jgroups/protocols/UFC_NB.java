package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.Credit;
import org.jgroups.util.NonBlockingCredit;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Non-blocking alternative to {@link UFC}.<br/>
 * JIRA: https://issues.jboss.org/browse/JGRP-2172
 * @author Bela Ban
 * @since  4.0.4
 */
@MBean(description="Simple non-blocking flow control protocol based on a credit system")
public class UFC_NB extends UFC {
    @Property(description="Max number of bytes of all queued messages for a given destination. If a given destination " +
      "has no credits left and the message cannot be added to the queue because it is full, then the sender thread " +
      "will be blocked until there is again space available in the queue, or the protocol is stopped.",type=AttributeType.BYTES)
    protected int                     max_queue_size=10_000_000;
    protected final Consumer<Message> send_function=msg -> down_prot.down(msg);
    protected Future<?>               credit_send_task;


    public int      getMaxQueueSize()      {return max_queue_size;}
    public UFC_NB   setMaxQueueSize(int s) {this.max_queue_size=s; return this;}

    @ManagedAttribute(description="The number of messages currently queued due to insufficient credit",type=AttributeType.SCALAR)
    public int getNumberOfQueuedMessages() {
        return sent.values().stream().map(c -> ((NonBlockingCredit)c).getQueuedMessages()).reduce(0, Integer::sum);
    }

    @ManagedAttribute(description="The total size of all currently queued messages for all destinations",type=AttributeType.BYTES)
    public int getQueuedSize() {
        return sent.values().stream().map(c -> ((NonBlockingCredit)c).getQueuedMessageSize()).reduce(0, Integer::sum);
    }

    @ManagedAttribute(description="The number of times messages have been queued due to insufficient credits",type=AttributeType.SCALAR)
    public int getNumberOfQueuings() {
        return sent.values().stream().map(c -> ((NonBlockingCredit)c).getEnqueuedMessages()).reduce(0, Integer::sum);
    }

    public boolean isQueuingTo(Address dest) {
        NonBlockingCredit cred=(NonBlockingCredit)sent.get(dest);
        return cred != null && cred.isQueuing();
    }

    public int getQueuedMessagesTo(Address dest) {
        NonBlockingCredit cred=(NonBlockingCredit)sent.get(dest);
        return cred != null? cred.getQueuedMessages() : 0;
    }

    public void start() throws Exception {
        super.start();
        if(max_block_time > 0) {
            credit_send_task=getTransport().getTimer()
              .scheduleWithFixedDelay(this::sendCreditRequestsIfNeeded, max_block_time, max_block_time, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        super.stop();
        if(credit_send_task != null)
            credit_send_task.cancel(true);
    }

    @Override protected Object handleDownMessage(Message msg) {
        Address dest=msg.getDest();
        if(dest == null) { // 2nd line of defense, not really needed
            log.error("%s doesn't handle multicast messages; passing message down", getClass().getSimpleName());
            return down_prot.down(msg);
        }

        Credit cred=sent.get(dest);
        if(cred == null)
            return down_prot.down(msg);

        int length=msg.getLength();
        if(running) {
            if(cred.decrementIfEnoughCredits(msg, length, 0)) // timeout is ignored
                return down_prot.down(msg);
            if(cred.needToSendCreditRequest(max_block_time))
                sendCreditRequest(dest, Math.max(0, max_credits - cred.get()));
            return null; // msg was queued
        }
        return down_prot.down(msg);
    }

    protected <T extends Credit> T createCredit(int initial_credits) {
        return (T)new NonBlockingCredit(initial_credits, max_queue_size, new ReentrantLock(true), send_function);
    }

    /**
     * Checks the sent table: if some credits are in queueing mode and credits left are less than min_credits:
     * send a credit request
     */
    protected void sendCreditRequestsIfNeeded() {
        sent.forEach((dest, c) -> {
            NonBlockingCredit cred=(NonBlockingCredit)c;
            if(cred.get() < min_credits && cred.isQueuing() && cred.needToSendCreditRequest(max_block_time)) {
                sendCreditRequest(dest, Math.max(0, max_credits - cred.get()));
            }
        });
    }

}

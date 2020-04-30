package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.CreditMap;
import org.jgroups.util.NonBlockingCreditMap;
import org.jgroups.util.Tuple;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;


/**
 * Non-blocking alternative to {@link MFC}.<br/>
 * JIRA: JIRA: https://issues.jboss.org/browse/JGRP-2172
 * @author Bela Ban
 * @since 4.0.4
 */
@MBean(description="Simple non-blocking flow control protocol based on a credit system")
public class MFC_NB extends MFC {
    @Property(description="Max number of bytes of all queued messages for a given destination. If a given destination " +
      "has no credits left and the message cannot be added to the queue because it is full, then the sender thread " +
      "will be blocked until there is again space available in the queue, or the protocol is stopped.",type=AttributeType.BYTES)
    protected int                     max_queue_size=10_000_000;
    protected final Consumer<Message> send_function=msg -> down_prot.down(msg);
    protected Future<?>               credit_send_task;


    public int      getMaxQueueSize()      {return max_queue_size;}
    public MFC_NB   setMaxQueueSize(int s) {this.max_queue_size=s; return this;}

    @ManagedAttribute(description="Whether or not messages are currently being queued due to insufficient credits")
    public boolean isQueuing() {
        return ((NonBlockingCreditMap)credits).isQueuing();
    }

    @ManagedAttribute(description="The number of messages currently queued due to insufficient credit"
      ,type=AttributeType.SCALAR)
    public int getNumberOfQueuedMessages() {
        return ((NonBlockingCreditMap)credits).getQueuedMessages();
    }

    @ManagedAttribute(description="The total size of all currently queued messages",type=AttributeType.BYTES)
    public int getQueuedSize() {
        return ((NonBlockingCreditMap)credits).getQueuedMessageSize();
    }

    @ManagedAttribute(description="The number of times messages have been queued due to insufficient credits"
      ,type=AttributeType.SCALAR)
    public int getNumberOfQueuings() {
        return ((NonBlockingCreditMap)credits).getEnqueuedMessages();
    }



    protected CreditMap createCreditMap(long max_creds) {
        return new NonBlockingCreditMap(max_creds, max_queue_size, new ReentrantLock(true), send_function);
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

    @Override
    protected Object handleDownMessage(final Message msg) {
        Address dest=msg.getDest();
        if(dest != null) // 2nd line of defense, not really needed
            return down_prot.down(msg);

        int length=msg.getLength();
        if(running) {
            if(credits.decrement(msg, length, 0)) // timeout is ignored
                return down_prot.down(msg);

            // message was queued
            if(needToSendCreditRequest()) {
                List<Tuple<Address,Long>> targets=credits.getMembersWithCreditsLessThan(min_credits);
                for(Tuple<Address,Long> tuple: targets)
                    sendCreditRequest(tuple.getVal1(), Math.min(max_credits, max_credits - tuple.getVal2()));
            }
            return null;
        }
        return down_prot.down(msg);
    }

    /**
     * Checks the sent table: if some credits are in queueing mode and credits left are less than min_credits:
     * send a credit request
     */
    protected void sendCreditRequestsIfNeeded() {
        if(credits.getMinCredits() < min_credits && needToSendCreditRequest()) {
            List<Tuple<Address,Long>> targets=credits.getMembersWithCreditsLessThan(min_credits);
            for(Tuple<Address,Long> tuple: targets)
                sendCreditRequest(tuple.getVal1(), Math.min(max_credits, max_credits - tuple.getVal2()));
        }
    }


}

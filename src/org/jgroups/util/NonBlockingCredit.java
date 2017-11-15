package org.jgroups.util;

import org.jgroups.BaseMessage;
import org.jgroups.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Non-blocking credit for a unicast destination.<br/>
 * Instead of blocking when insufficient credits are available for sending a message, this class <em>queues</em> the
 * message and sends it at a later time when enough credits have been received to send it.<br/>
 * JIRA: https://issues.jboss.org/browse/JGRP-2172
 * @author Bela Ban
 * @since  4.0.4
 */
public class NonBlockingCredit extends Credit {
    protected final SizeBoundedQueue<Message> msg_queue;
    protected boolean                         queuing; // messages will be queued when true
    protected final Consumer<Message>         send_function;
    protected static final Consumer<Message>  NO_OP_SEND_FUNCTION=msg -> {};
    protected final LongAdder                 num_queued=new LongAdder();


    public NonBlockingCredit(long credits, int max_size, Lock lock) {
        this(credits, max_size, lock, NO_OP_SEND_FUNCTION);
    }

    public NonBlockingCredit(long credits, int max_size, Lock lock, final Consumer<Message> send_function) {
        super(credits, lock);
        this.msg_queue=new SizeBoundedQueue<>(max_size, lock);
        this.send_function=send_function;
    }

    public boolean isQueuing()            {return queuing;}
    public int     getQueuedMessages()    {return msg_queue.getElements();}
    public int     getQueuedMessageSize() {return msg_queue.size();}
    public int     getEnqueuedMessages()  {return num_queued.intValue();}

    public void reset() {
        super.reset();
        num_queued.reset();
    }

    /**
     * Decrements the sender's credits by the size of the message.
     * @param msg The message
     * @param credits The number of bytes to decrement the credits. Is {@link BaseMessage#getLength()}.
     * @param timeout Ignored
     * @return True if the message was sent, false if it was queued
     */
    public boolean decrementIfEnoughCredits(final Message msg, int credits, long timeout) {
        lock.lock();
        try {
            if(queuing)
                return addToQueue(msg, credits);
            if(decrement(credits))
                return true; // enough credits, message will be sent
            queuing=true;    // not enough credits, start queuing
            return addToQueue(msg, credits);
        }
        finally {
            lock.unlock();
        }
    }

    public void increment(long credits, long max_credits) {
        List<Message> drain_list;
        lock.lock();
        try {
            super.increment(credits, max_credits);
            if(!queuing || msg_queue.isEmpty())
                return;
            int drained=msg_queue.drainTo(drain_list=new ArrayList<>(msg_queue.getElements()), (int)this.credits_left);
            if(drained > 0)
                credits_left=Math.min(max_credits, credits_left - drained);
            if(msg_queue.isEmpty())
                queuing=false;
        }
        finally {
            lock.unlock();
        }

        // finally send drained messages:
        if(!drain_list.isEmpty())
            drain_list.forEach(send_function);
    }

    public String toString() {
        return String.format("%s bytes left (queuing: %b, msg-queue size: %d, bytes: %s, enqueued: %d)",
                             super.toString(), isQueuing(), getQueuedMessages(), Util.printBytes(getQueuedMessageSize()), num_queued.intValue());
    }

    protected boolean addToQueue(Message msg, int length) {
        try {
            msg_queue.add(msg, length);
            num_queued.increment();
        }
        catch(InterruptedException e) {
        }
        return false;
    }

}

package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * @author Bela Ban
 * @since  4.0.4
 */
public class NonBlockingCreditMap extends CreditMap {
    protected final SizeBoundedQueue<Message> msg_queue;
    protected boolean                         queuing; // messages will be queued when true
    protected final Consumer<Message>         send_function;
    protected static final Consumer<Message>  NO_OP_SEND_FUNCTION=msg -> {};
    protected final LongAdder                 num_queued=new LongAdder();


    public NonBlockingCreditMap(long max_credits, int max_size, Lock lock) {
        this(max_credits, max_size, lock, NO_OP_SEND_FUNCTION);
    }

    public NonBlockingCreditMap(long max_credits, int max_size, Lock lock, final Consumer<Message> send_function) {
        super(max_credits, lock);
        this.msg_queue=new SizeBoundedQueue<>(max_size, lock);
        this.send_function=send_function;
    }


    public boolean isQueuing()            {return queuing;}
    public int     getQueuedMessages()    {return msg_queue.getElements();}
    public int     getQueuedMessageSize() {return msg_queue.size();}
    public int     getEnqueuedMessages()  {return num_queued.intValue();}

    public void resetStats() {
        super.resetStats();
        num_queued.reset();
    }



    @Override
    public boolean decrement(Message msg, int credits, long timeout) {
        lock.lock();
        try {
            if(done)
                return false;
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


    @Override
    public void replenish(Address sender, long new_credits) {
        if(sender == null)
            return;

        List<Message> drain_list;
        lock.lock();
        try {
            super.replenish(sender, new_credits);
            if(!queuing || msg_queue.isEmpty())
                return;
            int drained=msg_queue.drainTo(drain_list=new ArrayList<>(msg_queue.getElements()), (int)this.min_credits);
            if(drained > 0)
                decrement(drained);
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

    @Override public Long remove(Address key) {
        lock.lock();
        try {
            Long retval=super.remove(key);
            msg_queue.clear(false);
            return retval;
        }
        finally {
            lock.unlock();
        }
    }

    @Override public void clear() {
        lock.lock();
        try {
            super.clear();
            queuing=false;
            msg_queue.clear(true);
        }
        finally {
            lock.unlock();
        }
    }


    @Override public CreditMap reset() {
        lock.lock();
        try {
            super.reset();
            queuing=false;
            msg_queue.clear(true);
            return this;
        }
        finally {
            lock.unlock();
        }
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

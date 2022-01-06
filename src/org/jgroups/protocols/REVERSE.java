package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Predicate;

/**
 * Reverses the next N messages that are received. E.g. for N=4, {1,2,3,4,5} will be sent up as {4,3,2,1,5}. Blocks
 * until N messages have been received. Used for testing
 * @author Bela Ban
 * @since  4.1.5
 */
@MBean(description="Queues the next N messages that are received and passes them up in reverse order")
public class REVERSE extends Protocol {

    @ManagedAttribute(description="Number of messages to reverse.",writable=true)
    protected volatile int         num_msgs_to_reverse;
    protected Predicate<Message>   filter; // if set and false -> mesage will be sent up the stack

    protected final Deque<Message> queue=new ConcurrentLinkedDeque<>();

    public int     numMessagesToReverse()       {return num_msgs_to_reverse;}
    public REVERSE numMessagesToReverse(int n)  {num_msgs_to_reverse=n; return this;}
    public Predicate<Message> filter()          {return filter;}
    public REVERSE filter(Predicate<Message> f) {this.filter=f; return this;}
    @ManagedAttribute(description="Number of queued messages")
    public int     queuedMessages()             {return queue.size();}

    public void stop() {
        flush();
        super.stop();
    }

    public Object up(Message msg) {
        if(num_msgs_to_reverse == 0 || (filter != null && !filter.test(msg)))
            return up_prot.up(msg);
        if(queue.add(msg) && queue.size() >= num_msgs_to_reverse)
            flush();
        return null;
    }

    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(num_msgs_to_reverse == 0 || filter != null && !filter.test(msg))
                continue;
            if(queue.add(msg) && queue.size() >= num_msgs_to_reverse)
                flush();
            it.remove();
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    @ManagedOperation(description="Sends all queued messages and disables queueing for subsequent messages")
    public REVERSE flush() {
        Message msg;
        while((msg=queue.pollLast()) != null) {
            up_prot.up(msg);
        }
        num_msgs_to_reverse=0;
        return this;
    }
}

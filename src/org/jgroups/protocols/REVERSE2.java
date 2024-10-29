package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Predicate;

/**
 * @author Bela Ban
 * @since x.y
 */
@MBean(description="Reverts messages based on a filter and delivers them when told")
public class REVERSE2 extends Protocol {
    protected volatile Predicate<Message> filter; // if set and true: queue messages
    protected final Deque<Message>        queue=new ConcurrentLinkedDeque<>();

    public Predicate<Message> filter()                     {return filter;}
    public REVERSE2           filter(Predicate<Message> f) {this.filter=f; return this;}
    public int                size()                       {return queue.size();}

    /** Delivers queued messages */
    public int deliver() {
        Message msg; int count=0;
        while((msg=queue.pollLast()) != null) {
            up_prot.up(msg);
            count++;
        }
        return count;
    }

    @Override
    public Object up(Message msg) {
        if(filter != null && filter.test(msg)) {
            queue.add(msg);
            return null;
        }
        return up_prot.up(msg);
    }

    @Override
    public void up(MessageBatch batch) {
        if(filter == null) {
            up_prot.up(batch);
            return;
        }
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(filter.test(msg)) {
                it.remove();
                queue.add(msg);
            }
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    @Override
    public String toString() {
        return String.format("%d msgs", queue.size());
    }
}

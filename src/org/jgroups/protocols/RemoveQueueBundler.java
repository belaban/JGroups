package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Runner;

import java.util.List;

/**
 * Bundler implementation which sends message batches (or single messages) as soon as the remove queue is full
 * (or max_bundler_size would be exceeded).<p>
 * Messages are removed from the main queue  and processed as follows (assuming they all fit into the remove queue):<p>
 * A B B C C A causes the following sends: {@literal {AA} -> {CC} -> {BB}}
 * <p>
 * Note that <em>null</em> is also a valid destination (send-to-all).<p>
 * Contrary to {@link TransferQueueBundler}, this bundler uses a {@link RingBuffer} rather than an ArrayBlockingQueue
 * and the size of the remove queue is fixed. TransferQueueBundler increases the size of the remove queue
 * dynamically, which leads to higher latency if the remove queue grows too much.
 * <p>
 * JIRA: https://issues.redhat.com/browse/JGRP-2171
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class RemoveQueueBundler extends BaseBundler {
    protected RingBuffer<Message>   rb;
    protected Runner                runner;
    protected Message[]             remove_queue;
    protected final AverageMinMax   avg_batch_size=new AverageMinMax();
    protected static final String   THREAD_NAME="rq-bundler";

    @Property(name="remove_queue_size",description="The capacity of the remove queue",writable=false)
    protected int                   queue_size=1024;

    @ManagedAttribute(description="Average batch length")
    public String avgBatchSize() {return avg_batch_size.toString();}

    @ManagedAttribute(description="Current number of messages (to be sent) in the ring buffer")
    public int ringBufferSize() {return rb.size();}


    public void resetStats() {
        avg_batch_size.clear();
    }

    public void init(TP transport) {
        super.init(transport);
        rb=new RingBuffer<>(Message.class, capacity);
        remove_queue=new Message[queue_size];
        runner=new Runner(transport.getThreadFactory(), THREAD_NAME, this::run, null);
    }

    public synchronized void start() {
        super.start();
        runner.start();
    }

    public synchronized void stop() {
        runner.stop();
        super.stop();
    }

    public void renameThread() {
        transport.getThreadFactory().renameThread(THREAD_NAME, runner.getThread());
    }

    public void send(Message msg) throws Exception {
        rb.put(msg);
    }

    public void run() {
        try {
            int drained=rb.drainToBlocking(remove_queue);
            if(drained == 1) {
                output.position(0);
                sendSingleMessage(remove_queue[0]);
                return;
            }

            for(int i=0; i < drained; i++) {
                Message msg=remove_queue[i];
                int size=msg.size();
                if(count + size >= max_size)
                    sendBundledMessages();
                addMessage(msg, msg.size());
            }
            sendBundledMessages();
        }
        catch(Throwable t) {
        }
    }

    public int getQueueSize() {
        return rb.size();
    }

    public int size() {
        return rb.size();
    }

    protected void sendMessageList(Address dest, Address src, List<Message> list) {
        super.sendMessageList(dest, src, list);
        avg_batch_size.add(list.size());
    }


}

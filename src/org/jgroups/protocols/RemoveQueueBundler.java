package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Runner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bundler implementation which sends message batches (or single messages) as soon as the remove queue is full
 * (or max_bundler_size would be exceeded).<br/>
 * Messages are removed from the main queue  and processed as follows (assuming they all fit into the remove queue):<br/>
 * A B B C C A causes the following sends: {AA} -> {CC} -> {BB}<br/>
 * Note that <em>null</em> is also a valid destination (send-to-all).<br/>
 * Contrary to {@link TransferQueueBundler}, this bundler uses a {@link RingBuffer} rather than an ArrayBlockingQueue
 * and the size of the remove queue is fixed. TransferQueueBundler increases the size of the remove queue
 * dynamically, which leads to higher latency if the remove queue grows too much.
 * <br/>
 * JIRA: https://issues.jboss.org/browse/JGRP-2171
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class RemoveQueueBundler extends BaseBundler {
    protected RingBuffer<Message>   rb;
    protected Runner                runner;
    protected Message[]             remove_queue;
    protected final AverageMinMax   avg_batch_size=new AverageMinMax();
    protected int                   queue_size=1024;

    @ManagedAttribute(description="Remove queue size")
    public int rqbRemoveQueueSize() {return remove_queue.length;}

    @ManagedAttribute(description="Sets the size of the remove queue; creates a new remove queue")
    public void rqbRemoveQueueSize(int size) {
        if(size == queue_size) return;
        queue_size=size;
        remove_queue=new Message[queue_size];
    }

    @ManagedAttribute(description="Average batch length")
    public String rqbAvgBatchSize() {return avg_batch_size.toString();}

    @ManagedAttribute(description="Current number of messages (to be sent) in the ring buffer")
    public int rqbRingBufferSize() {return rb.size();}

    public Map<String,Object> getStats() {
        Map<String,Object> map=new HashMap<>();
        map.put("avg-batch-size",    avg_batch_size.toString());
        map.put("ring-buffer-size",  rb.size());
        map.put("remove-queue-size", queue_size);
        return map;
    }

    public void resetStats() {
        avg_batch_size.clear();
    }

    public void init(TP transport) {
        super.init(transport);
        rb=new RingBuffer(Message.class, transport.getBundlerCapacity());
        remove_queue=new Message[queue_size];
        runner=new Runner(new DefaultThreadFactory("aqb", true, true), "runner", this::run, null);
    }

    public synchronized void start() {
        super.start();
        runner.start();
    }

    public synchronized void stop() {
        runner.stop();
        super.stop();
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
                if(count + size >= transport.getMaxBundleSize())
                    sendBundledMessages();
                addMessage(msg, msg.size());
            }
            sendBundledMessages();
        }
        catch(Throwable t) {
        }
    }

    public int size() {
        return rb.size();
    }

    protected void sendMessageList(Address dest, Address src, List<Message> list) {
        super.sendMessageList(dest, src, list);
        avg_batch_size.add(list.size());
    }


}

package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Runner;

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
 * JIRA: https://issues.redhat.com/browse/JGRP-2171
 * @author Bela Ban
 * @since  4.0.4
 */
@Experimental
public class RemoveQueueBundler extends BaseBundler {
    protected RingBuffer<Message>   rb;
    protected Runner                runner;
    protected Message[]             remove_queue;
    protected static final String   THREAD_NAME="rq-bundler";

    @Property(name="remove_queue_size",description="The capacity of the remove queue",writable=false)
    protected int                   queue_size=1024;

    @ManagedAttribute(description="Current number of messages (to be sent) in the ring buffer")
    public int ringBufferSize() {return rb.size();}

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
                sendSingle(remove_queue[0].dest(), remove_queue[0], this.output);
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

}

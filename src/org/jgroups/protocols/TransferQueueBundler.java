package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.util.ConcurrentBlockingRingBuffer;
import org.jgroups.util.ConcurrentLinkedBlockingQueue;
import org.jgroups.util.FastArray;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.redhat.com/browse/JGRP-1540
 */
public class TransferQueueBundler extends BaseBundler implements Runnable {
    protected BlockingQueue<Message> queue;
    protected List<Message>          remove_queue;
    protected Thread                 bundler_thread;
    protected volatile boolean       running=true;
    protected static final String    THREAD_NAME="TQ-Bundler";


    public TransferQueueBundler() {
    }

    @ManagedAttribute(description="Size of the queue")
    public int                   getQueueSize()        {return queue.size();}

    @ManagedAttribute(description="Size of the remove-queue")
    public int                   removeQueueSize()     {return remove_queue.size();}

    @ManagedAttribute(description="Capacity of the remove-queue")
    public int                   removeQueueCapacity() {return ((FastArray<Message>)remove_queue).capacity();}

    @Override
    public void init(TP transport) {
        super.init(transport);
        if(transport instanceof TCP tcp) {
            tcp.useLockToSend(false); // https://issues.redhat.com/browse/JGRP-2901
            int size=tcp.getBufferedOutputStreamSize();
            if(size < max_size) { // https://issues.redhat.com/browse/JGRP-2903
                int new_size=max_size + Integer.BYTES;
                log.warn("buffered_output_stream_size adjusted from %,d -> %,d", size, new_size);
                tcp.setBufferedOutputStreamSize(new_size);
            }
        }
    }

    public synchronized void start() {
        if(running)
            stop();
        // queue blocks on consumer when empty; producers simply drop the message when full
        if(use_ringbuffer)
            queue=new ConcurrentBlockingRingBuffer<>(capacity, true, false);
        else
            queue=new ConcurrentLinkedBlockingQueue<>(capacity, true, false);
        if(remove_queue_capacity == 0)
            remove_queue_capacity=Math.max(capacity/4, 1024);
        remove_queue=new FastArray<>(remove_queue_capacity);
        bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
        running=true;
        bundler_thread.start();
    }

    public synchronized void stop() {
        running=false;
        Thread tmp=bundler_thread;
        bundler_thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {
                    tmp.join(500);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        drain();
    }

    public void renameThread() {
        transport.getThreadFactory().renameThread(THREAD_NAME, bundler_thread);
    }

    @ManagedAttribute(description="The number of unsent messages in the bundler")
    public int size() {
        return super.size() + removeQueueSize() + getQueueSize();
    }

    public void send(Message msg) throws Exception {
        if(!running)
            return;
        if(!queue.offer(msg))
            num_drops_on_full_queue.increment();
    }

    public void run() {
        while(running) {
            Message msg=null;
            try {
                if((msg=queue.take()) == null)
                    continue;
                addAndSendIfSizeExceeded(msg);
                while(true) {
                    remove_queue.clear();
                    int num_msgs=queue.drainTo(remove_queue, remove_queue_capacity);
                    if(num_msgs <= 0)
                        break;
                    avg_remove_queue_size.add(num_msgs);
                    remove_queue.forEach(this::addAndSendIfSizeExceeded); // ArrayList.forEach() avoids array bounds check
                }
                if(count > 0) {
                    if(transport.statsEnabled())
                        avg_fill_count.add(count);
                    sendBundledMessages();
                    num_sends_because_no_msgs.increment();
                }
            }
            catch(InterruptedException iex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void addAndSendIfSizeExceeded(Message msg) {
        int size=msg.size();
        if(count + size > max_size) {
            if(transport.statsEnabled())
                avg_fill_count.add(count);
            sendBundledMessages();
            num_sends_because_full_queue.increment();
        }
        addMessage(msg, size);
    }

    /** Takes all messages from the queue, adds them to the hashmap and then sends all bundled messages */
    protected void drain() {
        Message msg;
        if(queue != null) {
            while((msg=queue.poll()) != null)
                addAndSendIfSizeExceeded(msg);
        }
        if(!msgs.isEmpty())
            sendBundledMessages();
    }


}

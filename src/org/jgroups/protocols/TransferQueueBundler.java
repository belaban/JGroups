package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.FastArray;
import org.jgroups.util.Util;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.redhat.com/browse/JGRP-1540
 */
public class TransferQueueBundler extends BaseBundler implements Runnable {
    protected BlockingQueue<Message> queue;
    protected final List<Message>    remove_queue=new FastArray<Message>(Math.max(capacity/4, 1024))
                                                         .increment(128);
    protected volatile     Thread    bundler_thread;

    @Property(description="When the queue is full, senders will drop a message rather than wait until space " +
      "is available (https://issues.redhat.com/browse/JGRP-2765)")
    protected boolean                drop_when_full=true;

    protected volatile boolean       running=true;
    @ManagedAttribute(description="Number of times a message was sent because the queue was full", type=SCALAR)
    protected final LongAdder        num_sends_because_full_queue=new LongAdder();
    @ManagedAttribute(description="Number of times a message was sent because there was no message available in the queue",
      type=SCALAR)
    protected final LongAdder        num_sends_because_no_msgs=new LongAdder();

    @ManagedAttribute(description="Number of dropped messages (when drop_when_full is true)",type=SCALAR)
    protected final LongAdder        num_drops_on_full_queue=new LongAdder();

    @ManagedAttribute(description="Average fill size of the queue (in bytes)")
    protected final AverageMinMax    avg_fill_count=new AverageMinMax(512); // avg number of bytes when a batch is sent
    protected static final String    THREAD_NAME="TQ-Bundler";

    public TransferQueueBundler() {
    }

    @ManagedAttribute(description="Size of the queue")
    public int                   getQueueSize()          {return queue.size();}

    @ManagedAttribute(description="Size of the remove-queue")
    public int                   removeQueueSize()       {return remove_queue.size();}

    @ManagedAttribute(description="Capacity of the remove-queue")
    public int                   removeQueueCapacity()   {return ((FastArray<Message>)remove_queue).capacity();}

    public boolean               dropWhenFull()          {return drop_when_full;}
    public <T extends Bundler> T dropWhenFull(boolean d) {this.drop_when_full=d; return (T)this;}


    @Override
    public void resetStats() {
        super.resetStats();
        Stream.of(num_sends_because_full_queue,num_sends_because_no_msgs,num_drops_on_full_queue)
          .forEach(LongAdder::reset);
        avg_fill_count.clear();
    }

    public void init(TP tp) {
        super.init(tp);
    }

    public synchronized void start() {
        if(running)
            stop();
        // todo: replace with LinkedBlockingQueue and measure impact (if any) on perf
        queue=new ArrayBlockingQueue<>(Util.assertPositive(capacity, "bundler capacity cannot be " + capacity));
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

    public int size() {
        return super.size() + removeQueueSize() + getQueueSize();
    }

    public void send(Message msg) throws Exception {
        if(!running)
            return;
        if(drop_when_full || msg.isFlagSet(Message.TransientFlag.DONT_BLOCK)) {
            if(!queue.offer(msg))
                num_drops_on_full_queue.increment();
        }
        else
            queue.put(msg);
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
                    int num_msgs=queue.drainTo(remove_queue);
                    if(num_msgs <= 0)
                        break;
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
            catch(Throwable t) {
                log.trace("%s: failed sending message: %s", transport.addr(), t);
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

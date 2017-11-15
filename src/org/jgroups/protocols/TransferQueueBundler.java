package org.jgroups.protocols;


import org.jgroups.Message;
import org.jgroups.util.AverageMinMax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
 */
public class TransferQueueBundler extends BaseBundler implements Runnable {
    protected BlockingQueue<Message> queue;
    protected List<Message>          remove_queue;
    protected volatile     Thread    bundler_thread;
    protected volatile boolean       running=true;
    protected int                    num_sends_because_full_queue;
    protected int                    num_sends_because_no_msgs;
    protected final AverageMinMax    fill_count=new AverageMinMax(); // avg number of bytes when a batch is sent
    protected static final String    THREAD_NAME="TQ-Bundler";

    public TransferQueueBundler() {
        this.remove_queue=new ArrayList<>(16);
    }

    protected TransferQueueBundler(BlockingQueue<Message> queue) {
        this.queue=queue;
        this.remove_queue=new ArrayList<>(16);
    }

    public TransferQueueBundler(int capacity) {
        this(new ArrayBlockingQueue<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread               getThread()               {return bundler_thread;}
    public int                  getBufferSize()           {return queue.size();}
    public int                  removeQueueSize()         {return remove_queue.size();}
    public TransferQueueBundler removeQueueSize(int size) {this.remove_queue=new ArrayList<>(size); return this;}

    @Override
    public Map<String,Object> getStats() {
        Map<String,Object> retval=super.getStats();
        if(retval == null)
            retval=new HashMap<>(3);
        retval.put("sends_because_full", num_sends_because_full_queue);
        retval.put("sends_because_no_msgs", num_sends_because_no_msgs);
        retval.put("avg_fill_count", fill_count);
        return retval;
    }

    @Override
    public void resetStats() {
        num_sends_because_full_queue=num_sends_because_no_msgs=0;
        fill_count.clear();
    }

    public void init(TP tp) {
        super.init(tp);
        if(queue == null)
            queue=new ArrayBlockingQueue<>(assertPositive(tp.getBundlerCapacity(), "bundler capacity cannot be " + tp.getBundlerCapacity()));
    }

    public synchronized void start() {
        if(running)
            stop();
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
                try {tmp.join(500);} catch(InterruptedException e) {}
            }
        }
        drain();
    }


    public int size() {
        return super.size() + removeQueueSize() + getBufferSize();
    }

    public void send(Message msg) throws Exception {
        if(running)
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
                    for(int i=0; i < remove_queue.size(); i++) {
                        msg=remove_queue.get(i);
                        addAndSendIfSizeExceeded(msg);
                    }
                }
                if(count > 0) {
                    num_sends_because_no_msgs++;
                    fill_count.add(count);
                    _sendBundledMessages();
                }
            }
            catch(Throwable t) {
            }
        }
    }


    protected void addAndSendIfSizeExceeded(Message msg) {
        int size=msg.size();
        if(count + size >= transport.getMaxBundleSize()) {
            num_sends_because_full_queue++;
            fill_count.add(count);
            _sendBundledMessages();
        }
        _addMessage(msg, size);
    }


    /** Takes all messages from the queue, adds them to the hashmap and then sends all bundled messages */
    protected void drain() {
        Message msg;
        while((msg=queue.poll()) != null)
            addAndSendIfSizeExceeded(msg);
        _sendBundledMessages();
    }


    // This should not affect perf, as the lock is uncontended most of the time
    protected void _sendBundledMessages() {
        lock.lock();
        try {
            sendBundledMessages();
        }
        finally {
            lock.unlock();
        }
    }

    protected void _addMessage(Message msg, int size) {
        lock.lock();
        try {
            addMessage(msg, size);
        }
        finally {
            lock.unlock();
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}

package org.jgroups.protocols;

/**
 * @author Bela Ban
 * @since x.y
 */

import org.jgroups.Message;

import java.util.ArrayList;
import java.util.List;
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
    protected static final String    THREAD_NAME="TransferQueueBundler";

    public TransferQueueBundler() {
        this.remove_queue=new ArrayList<>(16);
    }

    protected TransferQueueBundler(BlockingQueue<Message> queue) {
        this.queue=queue;
        this.remove_queue=new ArrayList<>(16);
    }

    public TransferQueueBundler(int capacity) {
        this(new ArrayBlockingQueue<Message>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread               getThread()               {return bundler_thread;}
    public int                  getBufferSize()           {return queue.size();}
    public int                  removeQueueSize()         {return remove_queue.size();}
    public TransferQueueBundler removeQueueSize(int size) {this.remove_queue=new ArrayList<>(size); return this;}

    public void init(TP transport) {
        super.init(transport);
        if(queue == null)
            queue=new ArrayBlockingQueue<>(assertPositive(transport.getBundlerCapacity(), "bundler capacity cannot be " + transport.getBundlerCapacity()));
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
        queue.clear();
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
                long size=msg.size();
                if(count + size >= transport.getMaxBundleSize())
                    sendBundledMessages();
                addMessage(msg, size);
                while(true) {
                    remove_queue.clear();
                    int num_msgs=queue.drainTo(remove_queue);
                    if(num_msgs <= 0)
                        break;
                    for(int i=0; i < remove_queue.size(); i++) {
                        msg=remove_queue.get(i);
                        size=msg.size();
                        if(count + size >= transport.getMaxBundleSize())
                            sendBundledMessages();
                        addMessage(msg, size);
                    }
                }
                if(count > 0)
                    sendBundledMessages();
            }
            catch(Throwable t) {
            }
        }
    }


    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}

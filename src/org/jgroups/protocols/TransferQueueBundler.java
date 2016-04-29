package org.jgroups.protocols;

/**
 * @author Bela Ban
 * @since x.y
 */

import org.jgroups.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
 */
public class TransferQueueBundler extends BaseBundler implements Runnable {
    protected BlockingQueue<Message> queue;
    protected volatile     Thread    bundler_thread;
    protected static final String    THREAD_NAME="TransferQueueBundler";

    public TransferQueueBundler() {
    }

    protected TransferQueueBundler(BlockingQueue<Message> queue) {
        this.queue = queue;
    }

    protected TransferQueueBundler(int capacity) {
        this(new ArrayBlockingQueue<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread getThread()     {return bundler_thread;}
    public int    getBufferSize() {return queue.size();}

    public void init(TP transport) {
        super.init(transport);
        queue=new ArrayBlockingQueue<>(assertPositive(transport.getBundlerCapacity(), "bundler capacity cannot be " + transport.getBundlerCapacity()));
    }

    public synchronized void start() {
        if(bundler_thread != null)
            stop();
        bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
        bundler_thread.start();
    }

    public synchronized void stop() {
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

    public void send(Message msg) throws Exception {
        if(bundler_thread != null)
            queue.put(msg);
    }

    public void run() {
        while(Thread.currentThread() == bundler_thread) {
            Message msg=null;
            try {
                if(count == 0) {
                    msg=queue.take();
                    if(msg == null)
                        continue;
                    long size=msg.size();
                    if(count + size >= transport.getMaxBundleSize())
                        sendBundledMessages();
                    addMessage(msg, size);
                }
                while(null != (msg=queue.poll())) {
                    long size=msg.size();
                    if(count + size >= transport.getMaxBundleSize())
                        sendBundledMessages();
                    addMessage(msg, size);
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

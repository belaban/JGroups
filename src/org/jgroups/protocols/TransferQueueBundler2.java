package org.jgroups.protocols;


import org.jgroups.*;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540<br/>
 * The difference to {@link TransferQueueBundler} is that a size is maintained <pre>per destination</pre> and we
 * maintain byte arrays of max_bundle_size per destination into which we marshall a message directly when it is sent.
 */
public class TransferQueueBundler2 implements Bundler, Runnable {

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type=AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued until they are sent")
    protected int                    max_size=64000;

    @Property(description="The max number of elements in a bundler if the bundler supports size limitations",
      type=AttributeType.SCALAR)
    protected int                    capacity=16384;

    @Property(description="Time (microseconds) to wait on poll() from the down_queue. A value of <= 0 doesn't wait",
      type=AttributeType.TIME, unit=TimeUnit.MICROSECONDS)
    protected long                   poll_timeout=50;


    protected TP                     transport;
    protected Log                    log;
    protected BlockingQueue<Message> queue;
    protected List<Message>          remove_queue;
    protected volatile     Thread    bundler_thread;
    protected volatile boolean       running=true;
    @ManagedAttribute(description="Number of times a message was sent because the queue was full",
          type=AttributeType.SCALAR)
    protected long                   num_sends_because_full_queue;
    @ManagedAttribute(description="Number of times a message was sent because there was no message available",
      type=AttributeType.SCALAR)
    protected long                   num_sends_because_no_msgs;

    @ManagedAttribute(description="Average fill size of the queue (in bytes)")
    protected final AverageMinMax    avg_fill_count=new AverageMinMax(); // avg number of bytes when a batch is sent
    protected static final String    THREAD_NAME="TQ-Bundler2";


    protected final Map<Address,Buffer> messages=new ConcurrentHashMap<>();
    protected static final NullAddress  NIL=new NullAddress();


    public TransferQueueBundler2() {
        this.remove_queue=new ArrayList<>(16);
    }

    protected TransferQueueBundler2(BlockingQueue<Message> queue) {
        this.queue=queue;
        this.remove_queue=new ArrayList<>(16);
    }

    public TransferQueueBundler2(int capacity) {
        this(new ArrayBlockingQueue<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread                getThread()               {return bundler_thread;}

    public int                   getCapacity()       {return capacity;}
    public Bundler               setCapacity(int c)  {this.capacity=c; return this;}
    public int                   getMaxSize()        {return max_size;}
    public Bundler               setMaxSize(int s)   {max_size=s; return this;}

    @ManagedAttribute(description="Size of the queue")
    public int                   getQueueSize()            {return queue.size();}

    @ManagedAttribute(description="Size of the remove-queue")
    public int                   removeQueueSize()         {return remove_queue.size();}
    public TransferQueueBundler2 removeQueueSize(int size) {this.remove_queue=new ArrayList<>(size); return this;}

    @ManagedOperation(description="dumps info about buffers")
    public String dump() {
        return messages.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey() instanceof NullAddress? "null" : e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    @Override public void init(TP transport) {
        this.transport=transport;
        log=transport.getLog();
        messages.putIfAbsent(NIL, new Buffer(max_size + MSG_OVERHEAD));
    }

    @Override
    public void resetStats() {
        num_sends_because_full_queue=0;
        num_sends_because_no_msgs=0;
        avg_fill_count.clear();
    }

    public void viewChange(View view) {
        messages.keySet().retainAll(view.getMembers());
        messages.putIfAbsent(NIL, new Buffer(max_size + MSG_OVERHEAD));
    }

    public synchronized void start() {
        if(running)
            stop();
        queue=new ArrayBlockingQueue<>(assertPositive(capacity, "bundler capacity cannot be " + capacity));
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
        return removeQueueSize() + getQueueSize();
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
                for(;;) {
                    remove_queue.clear();
                    int num_msgs=queue.drainTo(remove_queue);
                    if(num_msgs > 0) {
                        for(Message m : remove_queue)
                            addAndSendIfSizeExceeded(m);
                    }
                    else {
                        msg=queue.poll(poll_timeout, TimeUnit.MICROSECONDS);
                        if(msg == null)
                            break;
                        addAndSendIfSizeExceeded(msg);
                    }
                }
                if(hasMessages()) {
                    num_sends_because_no_msgs++;
                    sendBundledMessages();
                }
            }
            catch(Throwable t) {
            }
        }
    }

    protected boolean hasMessages() {
        return messages.values().stream().anyMatch(b -> !b.isEmpty());
    }


    protected void addAndSendIfSizeExceeded(Message msg) {
        int size=msg.size();
        Address dest=msg.getDest() == null? NIL : msg.getDest();
        Buffer buf=messages.computeIfAbsent(dest, d -> new Buffer(size + MSG_OVERHEAD));
        if(buf.out.position() + size > max_size) {
            num_sends_because_full_queue++;
            avg_fill_count.add(buf.out.position());
            try {
                buf.send(msg.dest(), transport); // resets the buffer
            }
            catch(Exception ex) {
                log.error("%s: failed sending message: %s", transport.getAddress(), ex);
            }
        }
        try {
            buf.addMessage(msg, transport);
        }
        catch(Exception ex) {
            log.error("%s: failed serializing message to buffer: %s", transport.getAddress(), ex);
            buf.reset();
        }
    }

    protected void sendBundledMessages() {
        for(Map.Entry<Address,Buffer> entry: messages.entrySet()) {
            Buffer buf=entry.getValue();
            if(buf.isEmpty())
                continue;
            Address dest=entry.getKey();
            try {
                avg_fill_count.add(buf.out.position());
                buf.send(dest instanceof NullAddress || dest == null? null : dest, transport);
            }
            catch(Exception ex) {
                log.trace("%s: failed sending message: %s", transport.getAddress(), ex);
            }
        }
    }


    /** Takes all messages from the queue, adds them to the hashmap and then sends all bundled messages */
    protected void drain() {
        Message msg;
        if(queue != null) {
            while((msg=queue.poll()) != null)
                addAndSendIfSizeExceeded(msg);
        }
        sendBundledMessages();
    }



    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }


    private static class Buffer {
        private final ByteArrayDataOutputStream out;
        private int                             length_index; // to remember where to write the length field in the stream
        private int                             count;        // number of messages

        private Buffer(int length) {
            out=new ByteArrayDataOutputStream(length);
        }

        private Buffer reset() {
            out.position(0);
            length_index=count=0;
            return this;
        }

        private int size() {return out.position();}
        private boolean isEmpty() {return size() == 0;}

        private Buffer correctLength() {
            int old_pos=out.position();
            out.position(length_index).writeInt(count);
            out.position(old_pos);
            return this;
        }


        private Buffer addMessage(Message msg, TP transport) throws IOException {
            short transport_id=transport.getId();
            if(count == 0) { // write the headers - only once
                Util.writeMessageListHeader(msg.dest(), transport.getAddress(), transport.getClusterNameAscii().chars(),
                                            1, out, msg.getDest() == null);
                length_index=out.position() - Global.INT_SIZE;
            }
            out.writeShort(msg.getType());
            msg.writeToNoAddrs(msg.src(), out, transport_id); // exclude the transport header
            count++;
            return this;
        }

        private Buffer send(Address dest, TP transport) throws Exception {
            try {
                if(count > 0) {
                    if(count > 1) // 1 is the default when writing the header
                        correctLength();
                    transport.doSend(out.buffer(), 0, out.position(), dest);
                }
                return this;
            }
            finally {
                reset();
            }
        }

        public String toString() {
            return String.format("%d msg(s) %d bytes [cpacity=%d bytes]", count, out.position(), out.capacity());
        }
    }
}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Queues messages per destination ('null' is a special destination), sending when the last sender thread to the same
 * destination returns or max_size has been reached.
 * <br/>
 * See https://issues.redhat.com/browse/JGRP-2639 for details.
 * @author Bela Ban
 * @since  5.2.7
 */
public class PerDestinationBundler implements Bundler {


    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type= AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued (per destination) until they are sent")
    protected int                           max_size=64000;

    @Property(description="The maximum number of queued messages per destination. When the queue is full, a new " +
      "batch will be sent")
    protected int                           max_queue_size=128;

    @ManagedAttribute(description="Total number of messages sent (single and batches)",type=AttributeType.SCALAR)
    protected final LongAdder               total_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of single messages sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_single_msgs_sent=new LongAdder();


    @ManagedAttribute(description="Number of batches sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_batches_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because the queue was full",type=AttributeType.SCALAR)
    protected final LongAdder               num_sends_due_to_max_size=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because the max number of messages has been " +
      "reached (max_queue_size)", type=AttributeType.SCALAR)
    protected final LongAdder               num_sends_due_to_full_queue=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because the last sender thread returned",type=AttributeType.SCALAR)
    protected final LongAdder               num_sends_due_to_last_thread=new LongAdder();

    protected TP                            transport;
    protected Log                           log;
    protected Address                       local_addr;
    protected final Map<Address,SendBuffer> dests=Util.createConcurrentMap();
    protected static final Address          NULL=new NullAddress();


    public int     size() {
        return dests.values().stream().map(SendBuffer::size).reduce(0, Integer::sum);
    }
    
    public int     getQueueSize()         {return -1;}
    public int     getMaxSize()           {return max_size;}
    public Bundler setMaxSize(int s)      {this.max_size=s; return this;}
    public int     getMaxQueueSize()      {return max_queue_size;}
    public Bundler setMaxQueueSize(int s) {this.max_queue_size=s; return this;}

    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        long num_batches=num_batches_sent.sum(), total_msgs=total_msgs_sent.sum(), single_msgs=num_single_msgs_sent.sum();
        if(num_batches == 0 || total_msgs == 0) return 0.0;
        long batched_msgs=total_msgs - single_msgs;
        return batched_msgs / (double)num_batches;
    }

    @Override public void resetStats() {
        Stream.of(total_msgs_sent, num_batches_sent, num_sends_due_to_max_size,
                  num_sends_due_to_full_queue, num_sends_due_to_last_thread).forEach(LongAdder::reset);
    }

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
        this.log=transport.getLog();
    }

    public void start() {
        local_addr=Objects.requireNonNull(transport.getAddress());
    }

    public void stop() {

    }

    public void send(Message msg) throws Exception {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
        Address dest=msg.dest() == null ? NULL : msg.dest();
        SendBuffer buf=dests.computeIfAbsent(dest, k -> new SendBuffer());
        buf.addMessage(dest, msg);
    }

    public void viewChange(View view) {
        List<Address> mbrs=view.getMembers();
        if(mbrs == null) return;

        mbrs.stream().filter(dest -> !dests.containsKey(dest))
          .forEach(dest -> dests.putIfAbsent(dest, new SendBuffer()));

        // remove left members
        dests.keySet().stream().filter(dest -> !mbrs.contains(dest) && !(dest == NULL))
          .forEach(dests::remove);
    }


    protected class SendBuffer {
        private final Message[]                 msgs;
        private int                             index;
        private long                            total_bytes;
        private final AtomicInteger             thread_count=new AtomicInteger();
        private final ByteArrayDataOutputStream output;
        private final Lock                      lock=new ReentrantLock(false);


        protected SendBuffer() {
            this.msgs=new Message[max_queue_size];
            this.index=0;
            output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
        }

        protected void addMessage(Address dest, Message msg) {
            int msg_bytes=msg.getLength();
            thread_count.incrementAndGet();

            lock.lock();
            try {
                if(total_bytes + msg_bytes >= max_size) {
                    num_sends_due_to_max_size.increment();
                    sendBatch(dest); // will not throw an exception, just log a warning
                }

                msgs[index++]=msg;
                total_bytes+=msg_bytes;
                if(index == msgs.length) {
                    num_sends_due_to_full_queue.increment();
                    sendBatch(dest);
                }

                if(thread_count.decrementAndGet() == 0) {
                    num_sends_due_to_last_thread.increment();
                    sendBatch(dest); // will not throw an exception, just log a warning
                }
            }
            finally {
                lock.unlock();
            }
        }

        protected void sendBatch(Address destination) {
            if(index == 0)
                return;

            Address dest=destination == NULL? null : destination;
            if(index == 1) { // send a single message
                sendSingleMessage(dest, msgs[0]);
                num_single_msgs_sent.increment();
            }
            else {
                sendMessageList(dest, local_addr, msgs, index);
                num_batches_sent.increment();
            }
            total_msgs_sent.add(index);
            total_bytes=0;
            index=0;
        }

        protected void sendSingleMessage(final Address dest, final Message msg) {
            try {
                output.position(0);
                Util.writeMessage(msg, output, dest == null);
                transport.doSend(output.buffer(), 0, output.position(), dest);
                if(transport.statsEnabled())
                    transport.getMessageStats().incrNumSingleMsgsSent(1);
            }
            catch(Throwable e) {
                log.error("%s: failed sending message: %s", local_addr, e);
            }
        }

        protected void sendMessageList(final Address dest, final Address src, final Message[] list, int length) {
            try {
                output.position(0);
                Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, 0,
                                      length, output, dest == null, transport.getId());
                transport.doSend(output.buffer(), 0, output.position(), dest);
                if(transport.statsEnabled())
                    transport.getMessageStats().incrNumBatchesSent(1);
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
        }

        protected int size() {
            lock.lock();
            try {
                return index;
            }
            finally {
                lock.unlock();
            }
        }

    }


}

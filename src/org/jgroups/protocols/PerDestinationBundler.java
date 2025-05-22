package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
import static org.jgroups.conf.AttributeType.SCALAR;
import static org.jgroups.protocols.TP.MSG_OVERHEAD;
import static org.jgroups.util.MessageBatch.Mode.OOB;
import static org.jgroups.util.MessageBatch.Mode.REG;

/**
 * Queues messages per destination ('null' is a special destination), sending when the last sender thread to the same
 * destination returns or max_size has been reached. This uses 1 thread per destination, so it won't scale to many
 * cluster members (unless virtual threads are used).
 * <br/>
 * See https://issues.redhat.com/browse/JGRP-2639 for details.
 * @author Bela Ban
 * @since  5.2.7
 */
@Experimental
public class PerDestinationBundler implements Bundler {

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type= AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued (per destination) until they are sent")
    protected int                           max_size=64000;

    @Property(description="When the queue is full, senders will drop a message rather than wait until space " +
      "is available (https://issues.redhat.com/browse/JGRP-2765)")
    protected boolean                       drop_when_full=true;

    @ManagedAttribute(description="Total number of messages sent (single and batches)",type=AttributeType.SCALAR)
    protected final LongAdder               total_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of single messages sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_single_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent",type=AttributeType.SCALAR)
    protected final LongAdder               num_batches_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because no more messages were available",type=AttributeType.SCALAR)
    protected final LongAdder               num_send_due_to_no_msgs=new LongAdder();

    @ManagedAttribute(description="Number of batches sent because the queue was full",type=AttributeType.SCALAR)
    protected final LongAdder               num_sends_due_to_max_size=new LongAdder();

    @ManagedAttribute(description="Number of dropped messages (when drop_when_full is true)",type=SCALAR)
    protected final LongAdder               num_drops_on_full_queue=new LongAdder();

    @ManagedAttribute(description="Average time to send messages (transport part)")
    protected final AverageMinMax           avg_send_time=new AverageMinMax(1024).unit(TimeUnit.NANOSECONDS);

    protected TP                            transport;
    protected MsgStats                      msg_stats;
    protected MessageProcessingPolicy       msg_processing_policy;
    protected Log                           log;
    protected Address                       local_addr;
    protected final Map<Address,SendBuffer> dests=Util.createConcurrentMap();
    protected static final Address          NULL=new NullAddress();
    protected static final String           THREAD_NAME="pd-bundler";

    public int     size() {
        return dests.values().stream().map(SendBuffer::size).reduce(0, Integer::sum);
    }
    public int     getQueueSize()         {return -1;}
    public int     getMaxSize()           {return max_size;}
    public Bundler setMaxSize(int s)      {this.max_size=s; return this;}

    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        long num_batches=num_batches_sent.sum(), total_msgs=total_msgs_sent.sum(), single_msgs=num_single_msgs_sent.sum();
        if(num_batches == 0 || total_msgs == 0) return 0.0;
        long batched_msgs=total_msgs - single_msgs;
        return batched_msgs / (double)num_batches;
    }

    @ManagedOperation(description="Shows all destinations")
    public String dests() {
        return dests.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    @Override public void resetStats() {
        Stream.of(total_msgs_sent, num_batches_sent, num_single_msgs_sent, num_sends_due_to_max_size, num_drops_on_full_queue)
          .forEach(LongAdder::reset);
        avg_send_time.clear();
    }

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
        msg_processing_policy=transport.msgProcessingPolicy();
        msg_stats=transport.getMessageStats();
        this.log=transport.getLog();
    }

    public void start() {
        local_addr=Objects.requireNonNull(transport.getAddress());
        dests.values().forEach(SendBuffer::start);
    }

    public void stop() {
        dests.values().forEach(SendBuffer::stop);
    }

    public void send(Message msg) throws Exception {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
        Address dest=msg.dest() == null ? NULL : msg.dest();
        SendBuffer buf=dests.get(dest);
        if(buf == null)
            buf=dests.computeIfAbsent(dest, k -> new SendBuffer(dest).start());
        buf.send(msg);
    }

    public void viewChange(View view) {
        List<Address> mbrs=view.getMembers();
        if(mbrs == null) return;

        mbrs.stream().filter(dest -> !dests.containsKey(dest))
          .forEach(dest -> dests.putIfAbsent(dest, new SendBuffer(dest).start()));

        // remove left members
        dests.keySet().stream().filter(dest -> !mbrs.contains(dest) && !(dest == NULL))
          .forEach(dests::remove);
    }


    protected class SendBuffer implements Runnable {
        private final Address                   dest;
        protected final FastArray<Message>      msgs=new FastArray<>(128);
        private final Lock                      lock=new ReentrantLock(false);
        private final BlockingQueue<Message>    queue=new ArrayBlockingQueue<>(8192);
        private final List<Message>             remove_queue=new ArrayList<>(1024);
        private final ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
        private volatile Thread                 bundler_thread;
        private volatile boolean                running=true;
        private long                            count;


        protected SendBuffer(Address dest) {
            this.dest=dest;
        }

        public SendBuffer start() {
            if(running)
                stop();
            bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
            running=true;
            bundler_thread.start();
            return this;
        }

        public void stop() {
            running=false;
            Thread tmp=bundler_thread;
            if(tmp != null)
                tmp.interrupt();
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
                        sendBundledMessages();
                        num_send_due_to_no_msgs.increment();
                    }
                }
                catch(Throwable t) {
                }
            }
        }

        protected void addAndSendIfSizeExceeded(Message msg) {
            int size=msg.size(); // getLength() might return 0 when no [ayload is present: don't use!
            if(count + size >= max_size) {
                sendBundledMessages();
                num_sends_due_to_max_size.increment();
            }
            addMessage(msg, size);
        }

        protected void addMessage(Message msg, int size) {
            msgs.add(msg);
            count+=size;
        }

        protected void send(Message msg) throws Exception {
            if(!running)
                return;
            if(drop_when_full || msg.isFlagSet(Message.TransientFlag.DONT_BLOCK)) {
                if(!queue.offer(msg))
                    num_drops_on_full_queue.increment();
            }
            else
                queue.put(msg);
        }

        protected void sendBundledMessages() {
            if(msgs.isEmpty()) // should never happen!
                return;
            Address dst=dest == NULL? null : dest;
            sendMessages(dst, local_addr, msgs);
            msgs.clear(false);
            count=0;
        }

        protected void sendMessages(final Address dest, final Address src, final FastArray<Message> list) {
            long start=transport.statsEnabled()? System.nanoTime() : 0;
            try {
                int size=list.size();
                if(size == 0)
                    return;
                if(size == 1)
                    sendSingle(dest, list.get(0), this.output);
                else
                    sendMultiple(dest, src, list, this.output);
                if(start > 0)
                    avg_send_time.add(System.nanoTime()-start);
                total_msgs_sent.add(size);
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
        }

        protected void sendSingle(Address dst, Message msg, ByteArrayDataOutputStream out) {
            if(dst == null) { // multicast
                sendSingleMessage(msg.dest(), msg, out);
                loopbackUnlessDontLoopbackIsSet(msg);
            }
            else {            // unicast
                boolean send_to_self=Objects.equals(transport.getAddress(), dst)
                  || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
                if(send_to_self)
                    loopbackUnlessDontLoopbackIsSet(msg);
                else
                    sendSingleMessage(msg.dest(), msg, out);
            }
        }

        protected void sendMultiple(Address dst, Address sender, FastArray<Message> list, ByteArrayDataOutputStream out) {
            if(dst == null) { // multicast
                sendMessageList(dst, sender, list, out);
                loopback(dst, transport.getAddress(), list);
            }
            else {            // unicast
                boolean loopback=Objects.equals(transport.getAddress(), dst)
                  || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
                if(loopback)
                    loopback(dst, transport.getAddress(), list);
                else
                    sendMessageList(dst, sender, list, out);
            }
        }

        protected void sendSingleMessage(final Address dest, final Message msg, ByteArrayDataOutputStream out) {
            try {
                out.position(0);
                Util.writeMessage(msg, out, dest == null);
                transport.doSend(out.buffer(), 0, out.position(), dest);
                transport.getMessageStats().incrNumSingleMsgsSent();
                num_single_msgs_sent.increment();
            }
            catch(Throwable e) {
                log.error("%s: failed sending message to %s: %s", local_addr, dest, e);
            }
        }

        protected void sendMessageList(Address dest, Address src, FastArray<Message> list, ByteArrayDataOutputStream out) {
            out.position(0);
            try {
                Util.writeMessageList(dest, src, transport.cluster_name.chars(), list,
                                      out, dest == null);
                transport.doSend(out.buffer(), 0, out.position(), dest);
                transport.getMessageStats().incrNumBatchesSent();
                num_batches_sent.increment();
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
        }

        protected void loopback(Address dest, Address sender, FastArray<Message> list) {
            MessageBatch reg=null, oob=null;
            for(Message msg: list) {
                if(msg.isFlagSet(DONT_LOOPBACK))
                    continue;
                if(msg.isFlagSet(Message.Flag.OOB)) {
                    // we cannot reuse message batches (like in ReliableMulticast.removeAndDeliver()), because batches are
                    // submitted to a thread pool and new calls of this method might change them while they're being passed up
                    if(oob == null)
                        oob=new MessageBatch(dest, sender, transport.getClusterNameAscii(), dest == null, OOB, list.size());
                    oob.add(msg);
                }
                else {
                    if(reg == null)
                        reg=new MessageBatch(dest, sender, transport.getClusterNameAscii(), dest == null, REG, list.size());
                    reg.add(msg);
                }
            }
            if(reg != null) {
                msg_stats.received(reg);
                msg_processing_policy.loopback(reg, false);
            }
            if(oob != null) {
                msg_stats.received(oob);
                msg_processing_policy.loopback(oob, true);
            }
        }

        protected void loopbackUnlessDontLoopbackIsSet(Message msg) {
            if(msg.isFlagSet(DONT_LOOPBACK))
                return;
            msg_stats.received(msg);
            msg_processing_policy.loopback(msg, msg.isFlagSet(Message.Flag.OOB));
        }

        public String toString() {
            return String.format("%d msgs", size());
        }

        protected int size() {
            lock.lock();
            try {
                return msgs.size();
            }
            finally {
                lock.unlock();
            }
        }

    }


}

package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.logging.Log;
import org.jgroups.stack.MessageProcessingPolicy;
import org.jgroups.util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
import static org.jgroups.conf.AttributeType.SCALAR;
import static org.jgroups.protocols.TP.MSG_OVERHEAD;
import static org.jgroups.util.MessageBatch.Mode.OOB;
import static org.jgroups.util.MessageBatch.Mode.REG;

/**
 * Implements storing of messages in a hashmap and sending of single messages and message batches. Most bundler
 * implementations will want to extend this class
 * @author Bela Ban
 * @since  4.0
 */
public abstract class BaseBundler implements Bundler {
    /** Keys are destinations, values are lists of Messages */
    protected final Map<Address,List<Message>>      msgs=new HashMap<>(24);
    protected final Function<Address,List<Message>> FUNC=k -> new FastArray<Message>(32).increment(64);
    protected TP                                    transport;
    protected MessageProcessingPolicy               msg_processing_policy;
    protected final ReentrantLock                   lock=new ReentrantLock();
    protected @GuardedBy("lock") long               count;    // current number of bytes accumulated
    protected ByteArrayDataOutputStream             output;
    protected MsgStats                              msg_stats;
    protected Log                                   log;
    protected SuppressLog<Address>                  suppress_log;
    protected static final String                   FMT="%s: failed sending message to %s: %s";

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type=AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued until they are sent")
    protected int                                   max_size=64000;

    @Property(description="The max number of messages in a bundler if the bundler supports size limitations",
      type=SCALAR)
    protected int                                   capacity=16384;

    @Property(description="Capacity of the remove queue. If > 0, the capacity is fixed, otherwise it will be " +
      "computed dynamically as a function of capacity")
    protected int                                   remove_queue_capacity;

    @Property(description="True: the queues (e.g. TransferQueueBundler, PerDestinationBundler) use " +
      "ConcurrentBlockingRingBuffer. False: they use ConcurrentLinkedBlockingQueue. Bundlers may ignore this attribute")
    protected boolean                               use_ringbuffer=true;

    @Property(description="True: use a single thread for all destinationns. False: use a thread per destination. " +
      "This is currently only used by PerDestinationBundler, but was hoisted into the base class so the same " +
      "configuration could be used when changing bundlers")
    protected boolean                               use_single_sender_thread=true;

    @Property(description="When true, when there's no space to queue a message, senders will drop the message rather" +
      "than wait until space is available (https://issues.redhat.com/browse/JGRP-2765)",deprecatedMessage="ignored")
    @Deprecated(since="5.5.0",forRemoval=true)
    protected boolean                               drop_when_full=true;

    @Property(description="Number of milliseconds duplicate warnings (e.g. connection/sending failed) should be " +
      "suppressed. 0 disables the suppress log", type=AttributeType.TIME)
    protected long                                  suppress_log_timeout;

    @Property(description="Delay (in ms) after which queued messages to non-members are removed",unit=MILLISECONDS,type=AttributeType.TIME)
    protected long                                  remove_delay=5000;

    @ManagedAttribute(description="Average fill size of the queue (in bytes) when messages are sent")
    protected final AverageMinMax                   avg_fill_count=new AverageMinMax(512);

    @ManagedAttribute(description="Average number of messages drained into the remove queue")
    protected final AverageMinMax                   avg_remove_queue_size=new AverageMinMax(512);

    @ManagedAttribute(description="Time (us) to send the bundled messages")
    protected final AverageMinMax                   avg_send_time=new AverageMinMax(1024).unit(NANOSECONDS);

    @ManagedAttribute(description="Total number of messages sent (single and batches)",type=AttributeType.SCALAR)
    protected final LongAdder                       total_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of single messages sent",type=AttributeType.SCALAR)
    protected final LongAdder                       num_single_msgs_sent=new LongAdder();

    @ManagedAttribute(description="Number of batches sent",type=AttributeType.SCALAR)
    protected final LongAdder                       num_batches_sent=new LongAdder();

    @ManagedAttribute(description="Number of times a message was sent because the queue was full", type=SCALAR)
    protected final LongAdder                       num_sends_because_full_queue=new LongAdder();
    @ManagedAttribute(description="Number of times a message was sent because there were no more messages " +
      "available in the queue", type=SCALAR)
    protected final LongAdder                       num_sends_because_no_msgs=new LongAdder();

    @ManagedAttribute(description="Number of dropped messages (when drop_when_full is true)",type=SCALAR)
    protected final LongAdder                       num_drops_on_full_queue=new LongAdder();

    @ManagedAttribute(description="The current members")
    protected volatile List<Address>                members;

    @ManagedOperation(description="Prints the capacity of the buffers")
    public String printBuffers() {
        return msgs.entrySet().stream()
          .map(e -> String.format("%s: %d", e.getKey(), ((FastArray<Message>)e.getValue()).capacity()))
          .collect(Collectors.joining("\n"));
    }

    public int                   getCapacity()                    {return capacity;}
    public Bundler               setCapacity(int c)               {this.capacity=c; return this;}
    public int                   removeQueueCapacity()            {return remove_queue_capacity;}
    public Bundler               removeQueueCapacity(int c)       {this.remove_queue_capacity=c; return this;}
    public int                   getMaxSize()                     {return max_size;}
    public Bundler               setMaxSize(int s)                {max_size=s; return this;}
    public boolean               dropWhenFull()                   {return true;}
    public Bundler               dropWhenFull(boolean ignored)    {return this;}
    public boolean               useSingleSenderThread()          {return use_single_sender_thread;}
    public Bundler               useSingleSenderThread(boolean u) {this.use_single_sender_thread=u; return this;}
    public boolean               useRingBuffer()                  {return use_ringbuffer;}
    public Bundler               useRingBuffer(boolean u)         {this.use_ringbuffer=u; return this;}
    public long                  suppressLogTimeout()             {return suppress_log_timeout;}
    public void                  suppressLogTimeout(long s)       {this.suppress_log_timeout=s;}
    public long                  removeDelay()                    {return remove_delay;}
    public Bundler               removeDelay(long remove_delay)   {this.remove_delay=remove_delay; return this;}

    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        long num_batches=num_batches_sent.sum(), total_msgs=total_msgs_sent.sum(), single_msgs=num_single_msgs_sent.sum();
        if(num_batches == 0 || total_msgs == 0) return 0.0;
        long batched_msgs=total_msgs - single_msgs;
        return batched_msgs / (double)num_batches;
    }

    public void init(TP transport) {
        this.transport=transport;
        msg_processing_policy=transport.msgProcessingPolicy();
        msg_stats=transport.getMessageStats();
        log=transport.getLog();
        suppress_log=new SuppressLog<>(log);
        output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
    }

    public void resetStats() {
        Stream.of(total_msgs_sent,num_batches_sent, num_single_msgs_sent,num_sends_because_full_queue,
                  num_sends_because_no_msgs,num_drops_on_full_queue)
          .forEach(LongAdder::reset);
        avg_send_time.clear(); avg_fill_count.clear(); avg_remove_queue_size.clear();
    }

    public void start() {}
    public void stop()  {}
    public void send(Message msg) throws Exception {}

    public void viewChange(View view) {
        // code removed (https://issues.redhat.com/browse/JGRP-2324)
        this.members=view.getMembers();
    }

    /** Returns the total number of messages in the hashmap */
    @ManagedAttribute(description="The number of unsent messages in the bundler")
    public int size() {
        lock.lock();
        try {
            return msgs.values().stream().map(List::size).reduce(0, Integer::sum);
        }
        finally {
            lock.unlock();
        }
    }

    @ManagedAttribute(description="Size of the queue (if available")
    public int getQueueSize() {
        return -1;
    }

    /**
     * Sends all messages in the map. Messages for the same destination are bundled into a message list.
     * The map will be cleared when done.
     */
    @GuardedBy("lock") protected void sendBundledMessages() {
        boolean stats_enabled=transport.statsEnabled();
        long start=stats_enabled? System.nanoTime() : 0;
        for(Map.Entry<Address,List<Message>> entry: msgs.entrySet()) {
            List<Message> list=entry.getValue();
            if(list.isEmpty())
                continue;
            Address dst=entry.getKey();
            output.position(0);
            try {
                if(list.size() == 1)
                    sendSingle(dst, list.get(0), output);
                else
                    sendMultiple(dst, list.get(0).src(), list, output);
            }
            catch(Exception ex) {
                if(suppress_log_timeout <= 0)
                    log.trace(FMT, transport.getAddress(), dst, ex.getMessage());
                else
                    suppress_log.warn(dst, suppress_log_timeout, FMT, transport.getAddress(), dst, ex.getMessage());
            }
            total_msgs_sent.add(list.size());
            list.clear();
        }
        count=0;
        if(stats_enabled) {
            long time=System.nanoTime() - start;
            avg_send_time.add(time);
        }
    }

    protected void sendSingle(Address dst, Message msg, ByteArrayDataOutputStream out) throws Exception {
        if(dst == null) { // multicast
            sendSingleMessage(dst, msg, out);
            loopbackUnlessDontLoopbackIsSet(msg);
        }
        else {            // unicast
            boolean send_to_self=Objects.equals(transport.getAddress(), dst)
              || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
            if(send_to_self)
                loopbackUnlessDontLoopbackIsSet(msg);
            else
                sendSingleMessage(dst, msg, out);
        }
    }

    protected void sendMultiple(Address dst, Address sender, List<Message> list, ByteArrayDataOutputStream out) throws Exception {
        if(dst == null) { // multicast
            sendMessageList(dst, sender, list, out);
            loopback(dst, transport.getAddress(), list, list.size());
        }
        else {            // unicast
            boolean loopback=Objects.equals(transport.getAddress(), dst)
              || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
            if(loopback)
                loopback(dst, transport.getAddress(), list, list.size());
            else
                sendMessageList(dst, sender, list, out);
        }
    }

    protected void sendMultiple(Address dst, Address sender, Message[] list, int len, ByteArrayDataOutputStream out) {
        if(dst == null) { // multicast
            sendMessageListArray(dst, sender, list, len, out);
            loopback(dst, transport.getAddress(), list, len);
        }
        else {            // unicast
            boolean send_to_self=Objects.equals(transport.getAddress(), dst)
              || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
            if(send_to_self)
                loopback(dst, transport.getAddress(), list, len);
            else
                sendMessageListArray(dst, sender, list, len, out);
        }
    }

    protected void loopbackUnlessDontLoopbackIsSet(Message msg) {
        if(msg.isFlagSet(DONT_LOOPBACK))
            return;
        msg_stats.received(msg);
        msg_processing_policy.loopback(msg, msg.isFlagSet(Message.Flag.OOB));
    }

    protected void loopback(Address dest, Address sender, Iterable<Message> list, int size) {
        MessageBatch reg=null, oob=null;
        for(Message msg: list) {
            if(msg.isFlagSet(DONT_LOOPBACK))
                continue;
            if(msg.isFlagSet(Message.Flag.OOB)) {
                // we cannot reuse message batches (like in ReliableMulticast.removeAndDeliver()), because batches are
                // submitted to a thread pool and new calls of this method might change them while they're being passed up
                if(oob == null)
                    oob=new MessageBatch(dest, sender, transport.getClusterNameAscii(), dest == null, OOB, size);
                oob.add(msg);
            }
            else {
                if(reg == null)
                    reg=new MessageBatch(dest, sender, transport.getClusterNameAscii(), dest == null, REG, size);
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

    protected void loopback(Address dest, Address sender, Message[] list, int len) {
        FastArray<Message> fa=new FastArray<>(list, len);
        loopback(dest, sender, fa, fa.size());
    }

    protected void sendSingleMessage(final Address dest, final Message msg, ByteArrayDataOutputStream out) throws Exception {
        Util.writeMessage(msg, out, dest == null);
        transport.doSend(out.buffer(), 0, out.position(), dest);
        transport.getMessageStats().incrNumSingleMsgsSent();
        num_single_msgs_sent.increment();
    }

    protected void sendMessageList(Address dest, Address src, List<Message> list, ByteArrayDataOutputStream out) throws Exception {
        Util.writeMessageList(dest, src, transport.cluster_name.val(), list, out, dest == null);
        transport.doSend(out.buffer(), 0, out.position(), dest);
        transport.getMessageStats().incrNumBatchesSent();
        num_batches_sent.increment();
    }

    protected void sendMessageListArray(final Address dest, final Address src, Message[] list, int len, ByteArrayDataOutputStream out) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.val(), list, 0, len, out, dest == null);
            transport.doSend(out.buffer(), 0, out.position(), dest);
            transport.getMessageStats().incrNumBatchesSent();
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
        }
    }

    @GuardedBy("lock") protected void addMessage(Message msg, int size) {
        Address dest=msg.getDest();
        List<Message> tmp=msgs.computeIfAbsent(dest, FUNC);
        tmp.add(msg);
        count+=size;
    }
}
package org.jgroups.protocols;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
import static org.jgroups.util.MessageBatch.Mode.OOB;
import static org.jgroups.util.MessageBatch.Mode.REG;

/**
 * Bundler which uses {@link RingBuffer} to store messages. The difference to {@link TransferQueueBundler} is that
 * RingBuffer uses a wait strategy (to for example spinning) before blocking. Also, the hashmap of the superclass is not
 * used, but the array of the RingBuffer is used directly to bundle and send messages, minimizing memory allocation.
 */
public class RingBufferBundler extends BaseBundler {
    protected RingBuffer<Message>         rb;
    protected Runner                      bundler_thread;

    @Property(description="Number of spins before a real lock is acquired")
    protected int                         num_spins=40; // number of times we call Thread.yield before acquiring the lock (0 disables)
    protected static final String         THREAD_NAME="RingBufferBundler";
    protected BiConsumer<Integer,Integer> wait_strategy=SPIN_PARK;
    protected final Runnable              run_function=this::readMessages;

    protected static final BiConsumer<Integer,Integer> SPIN=(it,spins) -> {;};
    protected static final BiConsumer<Integer,Integer> YIELD=(it,spins) -> Thread.yield();
    protected static final BiConsumer<Integer,Integer> PARK=(it,spins) -> LockSupport.parkNanos(1);
    protected static final BiConsumer<Integer,Integer> SPIN_PARK=(it, spins) -> {
        if(it < spins/10)
            return; // spin for the first 10% of all iterations, then switch to park()
        LockSupport.parkNanos(1);
    };
    protected static final BiConsumer<Integer,Integer> SPIN_YIELD=(it, spins) -> {
        if(it < spins/10)
            return;           // spin for the first 10% of the total number of iterations
        Thread.yield(); //, then switch to yield()
    };


    public RingBufferBundler() {
    }

    protected RingBufferBundler(RingBuffer<Message> rb) {
        this.rb=rb;
        this.capacity=rb.capacity();
    }

    public RingBufferBundler(int capacity) {
        this(new RingBuffer<>(Message.class, assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public RingBuffer<Message> buf()                     {return rb;}
    public int                 size()                    {return rb.size();}
    public int                 getQueueSize()            {return rb.size();}
    public int                 numSpins()                {return num_spins;}
    public RingBufferBundler   numSpins(int n)           {num_spins=n; return this;}

    @Property(description="The wait strategy: spin, yield, park, spin-park, spin-yield",writable=false)
    public String              waitStrategy()            {return print(wait_strategy);}
    @Property
    public RingBufferBundler   waitStrategy(String st)   {wait_strategy=createWaitStrategy(st, YIELD); return this;}


    public void init(TP transport) {
        super.init(transport);
        if(rb == null) {
            rb=new RingBuffer<>(Message.class, assertPositive(capacity, "bundler capacity cannot be " + capacity));
            this.capacity=rb.capacity();
        }
        bundler_thread=new Runner(transport.getThreadFactory(), THREAD_NAME, run_function, () -> rb.clear());
    }

    public void start() {
        bundler_thread.start();
    }

    public void stop() {
        bundler_thread.stop();
    }

    public void renameThread() {
        transport.getThreadFactory().renameThread(THREAD_NAME, bundler_thread.getThread());
    }

    public void send(Message msg) throws Exception {
        rb.put(msg);
    }

    /** Read and send messages in range [read-index .. read-index+available_msgs-1] */
    public void sendBundledMessages(final Message[] buf, final int read_index, final int available_msgs) {
        byte[]    cluster_name=transport.cluster_name.chars();
        int       start=read_index;
        final int end=index(start + available_msgs-1); // index of the last message to be read

        for(;;) {
            Message msg=buf[start];
            if(msg == null) {
                if(start == end)
                    break;
                start=advance(start);
                continue;
            }
            Address dest=msg.getDest();
            if(dest == null) { // multicast
                List<Message> list=new ArrayList<>();
                _send(dest, msg, cluster_name, buf, start, end, list);
                loopback(dest, transport.getAddress(), list, list.size());
            }
            else {            // unicast
                boolean send_to_self=Objects.equals(transport.getAddress(), dest)
                  || dest instanceof PhysicalAddress && dest.equals(transport.localPhysicalAddress());
                if(send_to_self)
                    _loopback(dest, transport.getAddress(), buf, start, end);
                else
                    _send(dest, msg, cluster_name, buf, start, end, null);
            }
            if(start == end)
                break;
            start=advance(start);
        }
    }

    protected void _loopback(Address dest, Address sender, Message[] buf, int start_index, final int end_index) {
        MessageBatch reg=null, oob=null;
        AsciiString cluster_name=transport.getClusterNameAscii();
        for(;;) {
            Message msg=buf[start_index];
            if(msg != null && Objects.equals(dest, msg.getDest())) {
                if(msg.isFlagSet(DONT_LOOPBACK))
                    continue;
                if(msg.isFlagSet(Message.Flag.OOB)) {
                    // we cannot reuse message batches (like in ReliableMulticast.removeAndDeliver()), because batches are
                    // submitted to a thread pool and new calls of this method might change them while they're being passed up
                    if(oob == null)
                        oob=new MessageBatch(dest, sender, cluster_name, dest == null, OOB, 128);
                    oob.add(msg);
                }
                else {
                    if(reg == null)
                        reg=new MessageBatch(dest, sender, cluster_name, dest == null, REG, 128);
                    reg.add(msg);
                }
                buf[start_index]=null;
            }
            if(start_index == end_index)
                break;
            start_index=advance(start_index);
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

    protected void _send(Address dest, Message msg, byte[] cluster_name, Message[] buf, int start, int end,
                         List<Message> list) {
        try {
            if(list != null)
                list.add(msg);
            output.position(0);
            Util.writeMessageListHeader(dest, msg.getSrc(), cluster_name, 1, output, dest == null);

            // remember the position at which the number of messages (an int) was written, so we can later set the
            // correct value (when we know the correct number of messages)
            int size_pos=output.position() - Global.INT_SIZE;
            int num_msgs=marshalMessagesToSameDestination(dest, buf, start, end, max_size, list);
            if(num_msgs > 1) {
                int current_pos=output.position();
                output.position(size_pos);
                output.writeInt(num_msgs);
                output.position(current_pos);
            }
            transport.doSend(output.buffer(), 0, output.position(), dest);
            transport.getMessageStats().incrNumBatchesSent(num_msgs);
        }
        catch(Exception ex) {
            log.trace("failed to send message(s) to %s: %s", dest == null? "group" : dest, ex.getMessage());
        }
    }


    // Iterate through the following messages and find messages to the same destination (dest) and write them to output
    protected int marshalMessagesToSameDestination(Address dest, Message[] buf, int start_index, final int end_index,
                                                   int max_bundle_size, List<Message> list) throws Exception {
        int num_msgs=0, bytes=0;
        for(;;) {
            Message msg=buf[start_index];
            if(msg != null && Objects.equals(dest, msg.getDest())) {
                if(list != null)
                    list.add(msg);
                int size=msg.sizeNoAddrs(msg.getSrc()) + Global.SHORT_SIZE;
                if(bytes + size > max_bundle_size)
                    break;
                bytes+=size;
                num_msgs++;
                buf[start_index]=null;
                output.writeShort(msg.getType());
                msg.writeToNoAddrs(msg.getSrc(), output);
            }
            if(start_index == end_index)
                break;
            start_index=advance(start_index);
        }
        return num_msgs;
    }

    protected void readMessages() {
        try {
            int available_msgs=rb.waitForMessages(num_spins, wait_strategy);
            int read_index=rb.readIndexLockless();
            Message[] buf=rb.buf();
            sendBundledMessages(buf, read_index, available_msgs);
            rb.publishReadIndex(available_msgs);
        }
        catch(Throwable t) {
            ;
        }
    }


    protected final int advance(int index) {return index+1 == capacity? 0 : index+1;}
    protected final int index(int idx)     {return idx & (capacity-1);}    // fast equivalent to %


    protected static String print(BiConsumer<Integer,Integer> wait_strategy) {
        if(wait_strategy      == null)            return null;
        if(wait_strategy      == SPIN)            return "spin";
        else if(wait_strategy == YIELD)           return "yield";
        else if(wait_strategy == PARK)            return "park";
        else if(wait_strategy == SPIN_PARK)       return "spin-park";
        else if(wait_strategy == SPIN_YIELD)      return "spin-yield";
        else return wait_strategy.getClass().getSimpleName();
    }

    protected BiConsumer<Integer,Integer> createWaitStrategy(String st, BiConsumer<Integer,Integer> default_wait_strategy) {
        if(st == null) return default_wait_strategy;
        switch(st) {
            case "spin":            return wait_strategy=SPIN;
            case "yield":           return wait_strategy=YIELD;
            case "park":            return wait_strategy=PARK;
            case "spin_park":
            case "spin-park":       return wait_strategy=SPIN_PARK;
            case "spin_yield":
            case "spin-yield":      return wait_strategy=SPIN_YIELD;
            default:
                try {
                    Class<BiConsumer<Integer,Integer>> clazz=(Class<BiConsumer<Integer,Integer>>)Util.loadClass(st, this.getClass());
                    return clazz.getDeclaredConstructor().newInstance();
                }
                catch(Throwable t) {
                    log.error("failed creating wait_strategy " + st, t);
                    return default_wait_strategy;
                }
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}

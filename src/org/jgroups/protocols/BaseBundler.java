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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
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

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type=AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued until they are sent")
    protected int                                   max_size=64000;

    @Property(description="The max number of elements in a bundler if the bundler supports size limitations",
      type=AttributeType.SCALAR)
    protected int                                   capacity=16384;

    @ManagedAttribute(description="Time (us) to send the bundled messages")
    protected final AverageMinMax                   avg_send_time=new AverageMinMax().unit(NANOSECONDS);

    @ManagedOperation(description="Prints the capacity of the buffers")
    public String printBuffers() {
        return msgs.entrySet().stream()
          .map(e -> String.format("%s: %d", e.getKey(), ((FastArray<Message>)e.getValue()).capacity()))
          .collect(Collectors.joining("\n"));
    }

    public int     getCapacity()               {return capacity;}
    public Bundler setCapacity(int c)          {this.capacity=c; return this;}
    public int     getMaxSize()                {return max_size;}
    public Bundler setMaxSize(int s)           {max_size=s; return this;}

    public void init(TP transport) {
        this.transport=transport;
        msg_processing_policy=transport.getMsgProcessingPolicy();
        msg_stats=transport.getMessageStats();
        log=transport.getLog();
        output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
    }

    public void resetStats() {
        avg_send_time.clear();
    }

    public void start() {}
    public void stop()  {}
    public void send(Message msg) throws Exception {}

    public void viewChange(View view) {
        // code removed (https://issues.redhat.com/browse/JGRP-2324)
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
            if(list.size() == 1)
                sendSingle(dst, list.get(0), output);
            else
                sendMultiple(dst, list.get(0).src(), list, output);
            list.clear();
        }
        count=0;
        if(stats_enabled) {
            long time=System.nanoTime() - start;
            avg_send_time.add(time);
        }
    }

    protected void sendSingle(Address dst, Message msg, ByteArrayDataOutputStream out) {
        if(dst == null) { // multicast
            sendSingleMessage(msg, out);
            loopbackUnlessDontLoopbackIsSet(msg);
        }
        else {            // unicast
            boolean send_to_self=Objects.equals(transport.getAddress(), dst)
              || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
            if(send_to_self)
                loopbackUnlessDontLoopbackIsSet(msg);
            else
                sendSingleMessage(msg, out);
        }
    }

    protected void sendMultiple(Address dst, Address sender, List<Message> list, ByteArrayDataOutputStream out) {
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
            sendMessageList(dst, sender, list, len, out);
            loopback(dst, transport.getAddress(), list, len);
        }
        else {            // unicast
            boolean send_to_self=Objects.equals(transport.getAddress(), dst)
              || dst instanceof PhysicalAddress && dst.equals(transport.localPhysicalAddress());
            if(send_to_self)
                loopback(dst, transport.getAddress(), list, len);
            else
                sendMessageList(dst, sender, list, len, out);
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

    protected void sendSingleMessage(final Message msg, ByteArrayDataOutputStream out) {
        Address dest=msg.getDest();
        try {
            Util.writeMessage(msg, out, dest == null);
            transport.doSend(out.buffer(), 0, out.position(), dest);
            transport.getMessageStats().incrNumSingleMsgsSent();
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("SendFailure"),
                      transport.getAddress(), (dest == null? "cluster" : dest), msg.size(), e.toString(), msg.printHeaders());
        }
    }

    protected void sendMessageList(Address dest, Address src, List<Message> list, ByteArrayDataOutputStream out) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, out, dest == null);
            transport.doSend(out.buffer(), 0, out.position(), dest);
            transport.getMessageStats().incrNumBatchesSent();
        }
        catch(Throwable e) {
            log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
        }
    }

    protected void sendMessageList(final Address dest, final Address src, Message[] list, int len, ByteArrayDataOutputStream out) {
        try {
            Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, 0, len, out, dest == null);
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
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
import org.jgroups.util.FastArray;
import org.jgroups.util.Profiler;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Queues messages per destination ('null' is a special destination), sending when the last sender thread to the same
 * destination returns or max_size has been reached. This uses 1 thread per destination, so it won't scale to many
 * cluster members (unless virtual threads are used).
 * <p>
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

    protected TP                            transport;
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

    @Override public void resetStats() {
        Stream.of(total_msgs_sent, num_batches_sent, num_single_msgs_sent, num_sends_due_to_max_size)
          .forEach(LongAdder::reset);
        p_send_msg_list.reset();
        p_send_single_msg.reset();
        p_send_msgs.reset();
    }

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
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
        SendBuffer buf=dests.computeIfAbsent(dest, k -> new SendBuffer(dest).start());
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


    @ManagedAttribute final Profiler p_send_msg_list=new Profiler(),
      p_send_single_msg=new Profiler(),
      p_send_msgs=new Profiler();


    protected class SendBuffer implements Runnable {
        private final Address                   dest;
        protected final FastArray<Message>      msgs=new FastArray<Message>(16).increment(10);
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
            int size=msg.size();
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

        protected void sendBundledMessages() {
            sendBatch(dest, msgs);
            msgs.clear(false);
            count=0;
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

        public void renameThread() {
            transport.getThreadFactory().renameThread(THREAD_NAME, bundler_thread);
        }

        protected void send(Message msg) throws Exception {
            queue.put(msg);
        }


        protected void sendBatch(Address destination, FastArray<Message> list) {
            if(list.isEmpty()) // should never happen!
                return;
            Address dst=destination == NULL? null : destination;
            sendMessages(dst, local_addr, list);
        }

        protected void sendSingleMessage(final Address dest, final Message msg) {
            p_send_single_msg.start();
            try {
                output.position(0);
                Util.writeMessage(msg, output, dest == null);
                transport.doSend(output.buffer(), 0, output.position(), dest);
                transport.getMessageStats().incrNumSingleMsgsSent();
                num_single_msgs_sent.increment();
            }
            catch(Throwable e) {
                log.error("%s: failed sending message to %s: %s", local_addr, dest, e);
            }
            finally {
                p_send_single_msg.stop();
            }
        }


        protected void sendMessageList(final Address dest, final Address src, final FastArray<Message> list) {
            p_send_msg_list.start();
            output.position(0);
            try {
                Util.writeMessageList(dest, src, transport.cluster_name.chars(), list,
                                      output, dest == null);
                transport.doSend(output.buffer(), 0, output.position(), dest);
                transport.getMessageStats().incrNumBatchesSent();
                num_batches_sent.increment();
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
            finally {
                p_send_msg_list.stop();
            }
        }

        protected void sendMessages(final Address dest, final Address src, final FastArray<Message> list) {
            p_send_msgs.start();
            try {
                int size=list.size();
                if(size == 0)
                    return;
                if(size == 1)
                    sendSingleMessage(dest, list.get(0));
                else
                    sendMessageList(dest, src, list);
                total_msgs_sent.add(size);
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
            finally {
                p_send_msgs.stop();
            }
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

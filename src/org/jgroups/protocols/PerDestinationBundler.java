package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.FastArray;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Queues messages per destination ('null' is a special destination). Uses 1 thread per destination to process
 * queued messages, so it won't scale to many cluster members (unless virtual threads are used).
 * <br/>
 * See https://issues.redhat.com/browse/JGRP-2639 for details.
 * @author Bela Ban
 * @since  5.2.7
 */
public class PerDestinationBundler extends BaseBundler {
    protected Address                       local_addr;
    protected final Map<Address,SendBuffer> dests=Util.createConcurrentMap();
    protected static final Address          NULL=new NullAddress();
    protected static final String           THREAD_NAME="pd-bundler";

    // todo: add SuppressLog to each Sender and make 'log' redirect to it

    public int getQueueSize() {return -1;}

    public int size() {
        return dests.values().stream().map(SendBuffer::size).reduce(0, Integer::sum);
    }

    @ManagedOperation
    public String dump() {
        return dests.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue().dump()))
          .collect(Collectors.joining("\n"));
    }

    @ManagedOperation(description="Shows all destinations")
    public String dests() {
        return dests.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
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
            buf=dests.computeIfAbsent(dest, k -> new SendBuffer(msg.dest()).start());
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
        protected final FastArray<Message>      msgs=new FastArray<Message>(32).increment(64);
        private final Lock                      lock=new ReentrantLock(false);
        private BlockingQueue<Message>          queue;
        private FastArray<Message>              remove_queue;
        private final ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
        private Thread                          bundler_thread;
        private boolean                         running=true;
        private long                            count;


        public String dump() {
            return String.format("msgs cap: %,d, remove-q cap: %,d", msgs.capacity(), remove_queue.capacity());
        }

        protected SendBuffer(Address dest) {
            this.dest=dest;
        }

        public SendBuffer start() {
            if(running)
                stop();
            queue=new ArrayBlockingQueue<>(capacity);
            if(remove_queue_capacity == 0)
                remove_queue_capacity=Math.max(capacity/8, 1024);
            remove_queue=new FastArray<>(remove_queue_capacity);
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
            int rq_cap=remove_queue.capacity();
            while(running) {
                Message msg=null;
                try {
                    if((msg=queue.take()) == null)
                        continue;
                    addAndSendIfSizeExceeded(msg);
                    while(true) {
                        remove_queue.clear(false);
                        int num_msgs=queue.drainTo(remove_queue, rq_cap);
                        if(num_msgs <= 0)
                            break;
                        avg_remove_queue_size.add(num_msgs);
                        remove_queue.forEach(this::addAndSendIfSizeExceeded); // forEach() avoids array bounds check
                    }
                    if(count > 0) {
                        if(transport.statsEnabled())
                            avg_fill_count.add(count);
                        sendBundledMessages();
                        num_sends_because_no_msgs.increment();
                    }
                }
                catch(Throwable t) {
                }
            }
        }

        protected void addAndSendIfSizeExceeded(Message msg) {
            int size=msg.size(); // getLength() might return 0 when no payload is present: don't use!
            if(count + size >= max_size) {
                if(transport.statsEnabled())
                    avg_fill_count.add(count);
                sendBundledMessages();
                num_sends_because_full_queue.increment();
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
            sendMessages(dest, local_addr, msgs);
            msgs.clear(false);
            count=0;
        }

        protected void sendMessages(final Address dest, final Address src, final List<Message> list) {
            long start=transport.statsEnabled()? System.nanoTime() : 0;
            try {
                output.position(0);
                int size=list.size();
                // list.size() is guaranteed to be > 0 when this method is called
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

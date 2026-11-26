package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.util.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
public class PerDestinationBundler extends BaseBundler implements Runnable {
    protected Address                       local_addr;
    protected final Map<Address,SendBuffer> dests=Util.createConcurrentMap();
    protected static final Address          NULL=new NullAddress();
    protected Runner                        single_thread_runner;
    protected static final String           THREAD_NAME="pd-bundler";
    protected final Semaphore               msgs_available=new Semaphore(0);

    public boolean isRunning() {
        return single_thread_runner != null && single_thread_runner.isRunning();
    }

    @ManagedAttribute(description="Total number of messages in all queues")
    public int messagesAvailable() {return msgs_available.availablePermits();}

    @ManagedAttribute(description="Size of the queue (if available")
    public int getQueueSize() {return -1;}

    @ManagedAttribute(description="The number of unsent messages in the bundler",gauge=true)
    public int size() {
        return dests.values().stream().map(SendBuffer::size).reduce(0, Integer::sum);
    }

    @ManagedOperation(description="Dumps all sendbuffers")
    public String dump() {
        return dests.entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue().dump()))
          .collect(Collectors.joining("\n"));
    }

    @ManagedOperation(description="Shows the active threads")
    public String active() {
        return dests.entrySet().stream()
          .map(e -> String.format("dest: %s alive=%b", e.getKey(), e.getValue().isThreadAlive()))
          .collect(Collectors.joining("\n"));
    }

    @ManagedOperation(description="Shows all destinations")
    public String dests() {
        return dests.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    @Override
    public void init(TP transport) {
        super.init(transport);
        if(transport instanceof TCP tcp) {
            int size=tcp.getBufferedOutputStreamSize();
            if(size < max_size) { // https://issues.redhat.com/browse/JGRP-2903
                int new_size=max_size + Integer.BYTES;
                log.warn("buffered_output_stream_size adjusted from %,d -> %,d", size, new_size);
                tcp.setBufferedOutputStreamSize(new_size);
            }
        }
    }

    public void start() {
        super.start();
        local_addr=Objects.requireNonNull(transport.getAddress());
        if(transport instanceof TCP tcp)
            tcp.useLockToSend(!use_single_sender_thread); // https://issues.redhat.com/browse/JGRP-2901
        if(use_single_sender_thread)
            startSingleThreadRunner();
    }

    public void stop() {
        super.stop();
        dests.values().forEach(SendBuffer::stop);
        dests.clear();
        stopSingleThreadRunner();
    }

    public void startSingleThreadRunner() {
        if(single_thread_runner == null)
            single_thread_runner=new Runner(transport.getThreadFactory(), THREAD_NAME, this, null).joinTimeout(0);
        single_thread_runner.start();
    }

    public void stopSingleThreadRunner() {
        Util.close(single_thread_runner);
    }

    public void send(Message msg) throws Exception {
        if(single_thread_runner != null && !single_thread_runner.isRunning())
            return;
        doSend(msg);
    }

    public void doSend(Message msg) throws Exception {
        if(msg.getSrc() == null)
            msg.setSrc(local_addr);
        Address dest=msg.dest() == null ? NULL : msg.dest();
        SendBuffer buf=dests.get(dest);
        if(buf == null) {
            buf=dests.computeIfAbsent(dest, k -> new SendBuffer(msg.dest()));
            // start() needs to be called here (*not* above): the lambda might be called multiple times, and we'd
            // have multiple zombie sender threads!
            buf.start();
        }
        boolean success=buf.send(msg);
        if(success && use_single_sender_thread)
            msgs_available.release();
    }

    /**
     * Blocks on the semaphore until messages are available, then iterates through the send buffers and sends all
     * messages.
     * When an iteration found no messages to send, the runner calls run() again, blocking on the semaphore until new
     * messages are available in any of the buffers.
     * This is the single_sender_thread (use_single_sender_thread=true)
     */
    public void run() {
        try {
            msgs_available.acquire();
            msgs_available.drainPermits();
            for(SendBuffer buf: dests.values())
                buf.removeAndSend(true);
        }
        catch(InterruptedException e) {
            throw new RuntimeException(e); // caught and swallowed by the runner
        }
    }

    public void viewChange(View view) {
        // code removed (https://issues.redhat.com/browse/JGRP-2324, https://issues.redhat.com/browse/JGRP-2960)
        // remove left members after remove_delay ms
        TimeScheduler timer=transport.getTimer();
        final List<Address> left=Util.leftMembers(this.members, view.getMembers());
        super.viewChange(view); // sets this.members
        if(left != null && !left.isEmpty()) {
            Runnable r=() -> removeLeftMembers(left);
            timer.schedule(r, remove_delay, TimeUnit.MILLISECONDS);
        }
    }

    protected void removeLeftMembers(final List<Address> left_mbrs) {
        for(Address left: left_mbrs) {
            SendBuffer send_buf=dests.remove(left);
            if(send_buf != null)
                send_buf.stop();
        }
    }


    protected class SendBuffer implements Runnable {
        private final Address                   dest;
        private final FastArray<Message>        msgs=new FastArray<Message>(32).increment(64);
        private final Lock                      lock=new ReentrantLock(false);
        private final BlockingQueue<Message>    queue;
        private final FastArray<Message>        remove_queue;
        private final ByteBufferOutputStream    output=new ByteBufferOutputStream(max_size + MSG_OVERHEAD, false,
                                                                                  transport.useDirectMemory());
        private Runner                          sendbuf_runner;
        private long                            count;


        public String dump() {
            return String.format("msgs cap: %,d, remove-q cap: %,d", msgs.capacity(), remove_queue.capacity());
        }

        public SendBuffer(Address dest) {
            this.dest=dest;
            boolean block_on_empty=!use_single_sender_thread;
            if(use_ringbuffer)
                queue=new ConcurrentBlockingRingBuffer<>(capacity, block_on_empty, false);
            else
                queue=new ConcurrentLinkedBlockingQueue<>(capacity, block_on_empty, false);
            if(remove_queue_capacity == 0)
                remove_queue_capacity=Math.max(capacity/8, 1024);
            remove_queue=new FastArray<>(remove_queue_capacity);
        }

        public boolean isThreadAlive() {return sendbuf_runner != null && sendbuf_runner.getThread().isAlive();}

        public SendBuffer start() {
            if(!use_single_sender_thread) {
                lock.lock(); // https://issues.redhat.com/browse/JGRP-2915
                try {
                    if(sendbuf_runner == null)
                        sendbuf_runner=new Runner(transport.getThreadFactory(), THREAD_NAME, this, null).setJoinTimeout(0);
                    sendbuf_runner.start();
                }
                finally {
                    lock.unlock();
                }
            }
            return this;
        }

        public void stop() {
            Util.close(sendbuf_runner);
        }

        public void run() {
            try {
                Message msg=queue.take();
                if(msg == null)
                    return;
                addAndSendIfSizeExceeded(msg);
                removeAndSend(false); // loops until no more msgs available or size exceeded
            }
            catch(Throwable t) {
            }
        }

        protected void removeAndSend(boolean execute_only_once) {
            while(true) {
                remove_queue.clear(false);
                int num_msgs=queue.drainTo(remove_queue, remove_queue_capacity);
                if(num_msgs <= 0)
                    break;
                avg_remove_queue_size.add(num_msgs);
                remove_queue.forEach(this::addAndSendIfSizeExceeded); // forEach() avoids array bounds check
                if(execute_only_once)
                    break;
            }
            if(count > 0) {
                if(transport.statsEnabled())
                    avg_fill_count.add(count);
                sendBundledMessages();
                num_sends_because_no_msgs.increment();
            }
        }

        protected void addAndSendIfSizeExceeded(Message msg) {
            int size=msg.size(); // length() might return 0 when no payload is present: don't use!
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

        protected boolean send(Message msg) throws Exception {
            if(sendbuf_runner != null && !sendbuf_runner.isRunning())
                return false;
            if(queue.offer(msg))
                return true;
            num_drops_on_full_queue.increment();
            return false;
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
                output.reset();
                int size=list.size();
                // list.size() is guaranteed to be > 0 when this method is called
                if(size == 1)
                    sendSingle(dest, list.get(0), this.output);
                else
                    sendMultiple(dest, src, list, this.output);
                if(transport.statsEnabled())
                    avg_send_time.add(System.nanoTime()-start);
                total_msgs_sent.add(size);
            }
            catch(Throwable ex) {
                if(suppress_log_timeout <= 0)
                    log.trace(FMT, transport.getAddress(), dest, ex.getMessage());
                else
                    suppress_log.warn(dest, suppress_log_timeout, FMT, transport.getAddress(), dest, ex.getMessage());
            }
        }

        public String toString() {
            return String.format("%d msgs", queue.size());
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

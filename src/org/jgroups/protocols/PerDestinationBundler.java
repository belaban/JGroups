package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Queues messages per destination ('null' is a special destination). Uses 1 thread per destination to process
 * queued messages, so it won't scale to many cluster members (unless virtual threads are used).
 * <p>
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
    protected final Condition               not_empty=lock.newCondition();

    @ManagedAttribute(description="Total number of messages in all queues")
    protected final AtomicInteger           msgs_available=new AtomicInteger();

    @Property(description="True: use a single thread for all destinationns. False: use a thread per destination")
    protected boolean                       use_single_sender_thread;

    public boolean isRunning() {
        return single_thread_runner != null && single_thread_runner.isRunning();
    }

    public boolean useSingleSenderThread() {
        return use_single_sender_thread;
    }

    public PerDestinationBundler useSingleSenderThread(boolean use_single_thread) {
        this.use_single_sender_thread=use_single_thread;
        return this;
    }

    public int getQueueSize() {return -1;}

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

    public void init(TP transport) {
        this.transport=Objects.requireNonNull(transport);
        msg_processing_policy=transport.msgProcessingPolicy();
        msg_stats=transport.getMessageStats();
        this.log=transport.getLog();
    }

    public void start() {
        super.start();
        local_addr=Objects.requireNonNull(transport.getAddress());
        if(use_single_sender_thread) {
            if(single_thread_runner == null)
                single_thread_runner=new Runner(transport.getThreadFactory(), THREAD_NAME, this, null).joinTimeout(0);
            single_thread_runner.start();
        }
    }

    public void stop() {
        super.stop();
        dests.values().forEach(SendBuffer::stop);
        dests.clear();
        if(single_thread_runner != null) {
            lock.lock();
            try {
                msgs_available.set(1); // ???
                not_empty.signal();
            }
            finally {
                lock.unlock();
            }
            Util.close(single_thread_runner);
        }
    }

    public void send(Message msg) throws Exception {
        if(single_thread_runner != null && !single_thread_runner.isRunning())
            return;
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
        if(success && use_single_sender_thread) {
            int old_val=msgs_available.getAndIncrement();
            if(old_val == 0)
                signalNotEmpty();
        }
    }

    /**
     * Iterates through the send buffers and sends when messages are available. When an iteration found no messages to
     * send, the thread blocks on a condition that is signalled as soon as messages are available in any of the buffers.
     * This is the single_sender_thread (use_single_sender_thread=true)
     */
    public void run() {
        int removed_msgs=0;
        for(SendBuffer buf: dests.values()) {
            int removed=buf.removeAndSend(true);
            removed_msgs+=removed;
        }
        // continue looping until no messages were removed in an iteration
        if(removed_msgs > 0) {
            msgs_available.addAndGet(-removed_msgs);
            return; // Runner will run another iteration
        }
        if(msgs_available.get() == 0)
            waitUntilMessagesAreAvailable();
    }

    public void viewChange(View view) {
        List<Address> mbrs=view.getMembers();

        // add new members
        mbrs.stream().filter(dest -> !dests.containsKey(dest))
          .forEach(dest -> {
              SendBuffer buf=dests.get(dest);
              if(buf == null) {
                  buf=dests.computeIfAbsent(dest, k -> new SendBuffer(dest));
                  // start() needs to be called here (*not* above): the lambda might be called multiple times, and we'd
                  // have multiple zombie sender threads!
                  buf.start();
              }
          });

        // remove left members
        dests.entrySet().stream()
          .filter(e -> e.getKey() != NULL && !mbrs.contains(e.getKey()))
          .forEach(e -> {
              e.getValue().stop();
              dests.remove(e.getKey());
          });
    }

    protected void signalNotEmpty() {
        lock.lock();
        try {
            not_empty.signal();
        }
        finally {
            lock.unlock();
        }
    }

    protected void waitUntilMessagesAreAvailable() {
        lock.lock();
        try {
            while(msgs_available.get() == 0) {
                try {
                    not_empty.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            lock.unlock();
        }
    }


    protected class SendBuffer implements Runnable {
        private final Address                   dest;
        protected final FastArray<Message>      msgs=new FastArray<Message>(32).increment(64);
        private final Lock                      lock=new ReentrantLock(false);
        private BlockingQueue<Message>          queue;
        private FastArray<Message>              remove_queue;
        private final ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
        private Runner                          sendbuf_runner;
        private long                            count;


        public String dump() {
            return String.format("msgs cap: %,d, remove-q cap: %,d", msgs.capacity(), remove_queue.capacity());
        }

        public SendBuffer(Address dest) {
            this.dest=dest;
        }

        public boolean isThreadAlive() {return sendbuf_runner != null && sendbuf_runner.getThread().isAlive();}

        public SendBuffer start() {
            boolean block_on_empty=!use_single_sender_thread;
            queue=new ConcurrentLinkedBlockingQueue<>(capacity, block_on_empty, false);
            if(remove_queue_capacity == 0)
                remove_queue_capacity=Math.max(capacity/8, 1024);
            remove_queue=new FastArray<>(remove_queue_capacity);

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

        protected int removeAndSend(boolean execute_only_once) {
            int removed_msgs=0;
            while(true) {
                remove_queue.clear(false);
                int num_msgs=queue.drainTo(remove_queue, remove_queue_capacity);
                if(num_msgs <= 0)
                    break;
                removed_msgs+=num_msgs;
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
            return removed_msgs;
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

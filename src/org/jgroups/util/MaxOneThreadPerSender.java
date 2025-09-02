package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.TP;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link org.jgroups.stack.MessageProcessingPolicy} which processes <em>regular</em> messages and message batches by
 * assigning a max of 1 thread per message from the same sender. So if we have senders A, B, C and D, we'll have no more
 * than 4 threads handling regular unicasts and 4 threads handling regular multicasts.<p>
 * See <a href="https://issues.redhat.com/browse/JGRP-2143">JGRP-2143</a> for details.<p>
 * When using virtual threads, we probably don't need this anymore, and can use a virtual thread to send messages /
 * message batches directly up to UNICAST or NAKACK.
 * @author Bela Ban
 * @since  4.0
 */
public class MaxOneThreadPerSender extends SubmitToThreadPool {
    protected final MessageTable mcasts=new MessageTable();
    protected final MessageTable ucasts=new MessageTable();

    @Property(description="Max number of messages buffered for consumption of the delivery thread in " +
      "MaxOneThreadPerSender. 0 creates an unbounded buffer")
    protected int                max_buffer_size;

    @ManagedOperation(description="Dumps unicast and multicast tables")
    public String dump() {
        return String.format("\nmcasts:\n%s\nucasts:\n%s", mcasts, ucasts);
    }

    public void reset() {
        mcasts.map.values().forEach(Entry::reset);
        ucasts.map.values().forEach(Entry::reset);
    }

    public void init(TP transport) {
        super.init(transport);
    }

    public void destroy() {
        mcasts.clear();
        ucasts.clear();
    }

    public boolean loopback(Message msg, boolean oob) {
        if(oob)
            return super.loopback(msg, oob);
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        return table.process(msg, true);
    }

    public boolean loopback(MessageBatch batch, boolean oob) {
        if(oob)
            return super.loopback(batch, oob);
        MessageTable table=batch.dest() == null? mcasts : ucasts;
        return table.process(batch, true);
    }

    public boolean process(Message msg, boolean oob) {
        if(oob)
            return super.process(msg, oob);
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        return table.process(msg, false);
    }

    public boolean process(MessageBatch batch, boolean oob) {
        if(oob)
            return super.process(batch, oob);
        MessageTable table=batch.dest() == null? mcasts : ucasts;
        return table.process(batch, false);
    }

    public void viewChange(List<Address> members) {
        mcasts.viewChange(members);
        ucasts.viewChange(members);
    }


    protected class MessageTable {
        protected final ConcurrentMap<Address,Entry> map=new ConcurrentHashMap<>();

        public MessageTable() {
        }

        protected Entry get(final Address sender, boolean multicast) {
            Entry e=map.get(sender);
            if(e != null)
                return e;
            // not so elegant, but avoids lambda allocation! true?
            Entry tmp=map.putIfAbsent(sender, (e=new Entry(sender, multicast, tp.getClusterNameAscii())));
            return tmp!= null? tmp: e;
            // return map.computeIfAbsent(sender, s -> new Entry(sender, multicast, tp.getClusterNameAscii()));
        }

        protected void clear() {
            map.values().forEach(Entry::trimToInitialCapacity);
            map.clear();
        }

        protected boolean process(Message msg, boolean loopback) {
            Address dest=msg.getDest(), sender=msg.getSrc();
            return sender != null && get(sender, dest == null).process(msg, loopback);
        }

        protected boolean process(MessageBatch batch, boolean loopback) {
            Address dest=batch.dest(), sender=batch.sender();
            return get(sender, dest == null).process(batch, loopback);
        }

        protected void viewChange(List<Address> mbrs) {
            // remove all senders that are not in the new view
            map.keySet().retainAll(mbrs);
            map.values().forEach(Entry::trimToInitialCapacity);
        }

        public String toString() {
            return map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
        }
    }


    protected class Entry {
        protected final Lock               lock=new ReentrantLock();
        protected final boolean            mcast;
        protected final MessageBatch       batch;     // grabs queued msgs from msg_queue and passes them up the stack
        protected final FastArray<Message> msg_queue; // used to queue incoming (regular) messages
        protected final Address            sender;
        protected final AsciiString        cluster_name;
        protected final AtomicInteger      adders=new AtomicInteger(0);
        protected final LongAdder          submitted_batches=new LongAdder();
        protected final LongAdder          queued_msgs=new LongAdder();
        protected static final int         DEFAULT_INITIAL_CAPACITY=128;
        protected static final int         DEFAULT_INCREMENT=128;


        protected Entry(Address sender, boolean mcast, AsciiString cluster_name) {
            this.mcast=mcast;
            this.sender=sender;
            this.cluster_name=cluster_name;
            int cap=max_buffer_size > 0? max_buffer_size : DEFAULT_INITIAL_CAPACITY; // initial capacity
            batch=new MessageBatch(cap).dest(tp.getAddress()).sender(sender).clusterName(cluster_name)
              .multicast(mcast).mode(MessageBatch.Mode.REG); // only regular messages are queued
            batch.array().increment(DEFAULT_INCREMENT);
            msg_queue=max_buffer_size > 0? new FastArray<>(max_buffer_size) : new FastArray<>(DEFAULT_INITIAL_CAPACITY);
            msg_queue.increment(DEFAULT_INCREMENT);
        }

        public Entry reset() {
            Stream.of(submitted_batches,queued_msgs).forEach(LongAdder::reset);
            return this;
        }

        public Entry trimToInitialCapacity() {
            lock.lock();
            try {
                msg_queue.trimTo(max_buffer_size > 0? max_buffer_size : DEFAULT_INITIAL_CAPACITY);
                batch.array().trimTo(max_buffer_size > 0? max_buffer_size : DEFAULT_INITIAL_CAPACITY);
            }
            finally {
                lock.unlock();
            }
            return this;
        }

        protected boolean process(Message msg, boolean loopback) {
            lock.lock();
            try {
                msg_queue.add(msg, max_buffer_size == 0);
            }
            finally {
                lock.unlock();
            }
            queued_msgs.increment();
            if(adders.getAndIncrement() != 0)
                return false;
            return submit(loopback);
        }

        protected boolean process(MessageBatch batch, boolean loopback) {
            FastArray<Message> fa=batch.array();
            lock.lock();
            try {
                msg_queue.addAll(fa, max_buffer_size == 0);
            }
            finally {
                lock.unlock();
            }
            queued_msgs.add(batch.size());
            if(adders.getAndIncrement() != 0)
                return false;
            return submit(loopback);
        }

        protected boolean submit(boolean loopback) {
            submitted_batches.increment();
            BatchHandlerLoop handler=new BatchHandlerLoop(this, loopback);
            if(!tp.getThreadPool().execute(handler)) {
                adders.set(0);
                return false;
            }
            return true;
        }

        /** Called by {@link BatchHandlerLoop}. Atomically transfer messages from the entry.msg_queue to entry.batch
         * and returns true if messages were transferred.
         */
        protected boolean workAvailable() {
            lock.lock();
            try {
                batch.clear();
                int num_msgs=batch.array().transferFrom(msg_queue, true); // clears msg_queue
                return num_msgs > 0;
            }
            catch(Throwable t) {
                return false;
            }
            finally {
                lock.unlock();
            }
        }

        // unsynchronized on batch but who cares
        public String toString() {
            return String.format("msg_queue.size=%,d msg_queue.cap: %,d batch.cap=%,d queued msgs=%,d submitted batches=%,d",
                                 msg_queue.size(), msg_queue.capacity(), batch.capacity(), queued_msgs.sum(),
                                 submitted_batches.sum());
        }
    }


    protected class BatchHandlerLoop extends BatchHandler {
        protected final Entry entry;

        protected BatchHandlerLoop(Entry entry, boolean loopback) {
            super(null, loopback);
            this.entry=entry;
            this.loopback=loopback;
        }

        public void run() {
            while(entry.workAvailable() || entry.adders.decrementAndGet() != 0) {
                try {
                    MessageBatch mb=entry.batch;
                    if(mb.isEmpty())
                        continue;
                    if(!mb.multicast()) {
                        // due to an incorrect (e.g. late) view change, the cached batch's destination might be
                        // different from our local address. If this is the case, change the cached batch's dest address
                        if(tp.unicastDestMismatch(mb.dest())) {
                            Address dest=tp.addr();
                            if(dest != null)
                                mb.dest(dest);
                        }
                    }
                    tp.passBatchUp(mb, !loopback, !loopback);
                }
                catch(Throwable t) {
                    log.error("failed processing batch", t);
                }
            }
        }
    }
}

package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.protocols.TP;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;
import java.util.stream.Stream;

/**
 * {@link org.jgroups.stack.MessageProcessingPolicy} which processes <em>regular</em> messages and message batches by
 * assigning a max of 1 thread per message from the same sender. So if we have sender A, B, C and D, we'll have no more
 * than 4 threads handling regular unicasts and 4 threads handling regular multicasts.<p>
 * See <a href="https://issues.jboss.org/browse/JGRP-2143">JGRP-2143</a> for details.
 * @author Bela Ban
 * @since  4.0
 */
public class MaxOneThreadPerSender extends SubmitToThreadPool {
    protected final MessageTable mcasts=new MessageTable();
    protected final MessageTable ucasts=new MessageTable();
    protected int                max_buffer_size;
    protected boolean            resize=true;

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
        max_buffer_size=tp.getMessageProcessingMaxBufferSize();
        resize=max_buffer_size == 0;
    }

    public void destroy() {
        mcasts.clear();
        ucasts.clear();
    }

    public void loopback(Message msg, boolean oob, boolean internal) {
        if(oob || internal) {
            super.loopback(msg, oob, internal);
            return;
        }
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        table.process(msg, true);
    }

    public void process(Message msg, boolean oob, boolean internal) {
        if(oob || internal) {
            super.process(msg, oob, internal);
            return;
        }
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        table.process(msg, false);
    }

    public void process(MessageBatch batch, boolean oob, boolean internal) {
        if(oob || internal) {
            super.process(batch, oob, internal);
            return;
        }
        MessageTable table=batch.dest() == null? mcasts : ucasts;
        table.process(batch);
    }

    public void viewChange(List<Address> members) {
        mcasts.viewChange(members);
        ucasts.viewChange(members);
    }


    protected class MessageTable {
        protected final ConcurrentMap<Address,Entry> map=new ConcurrentHashMap<>();

        public MessageTable() {
        }

        protected Entry get(final Address dest, final Address sender) {
            Entry entry=map.get(sender);
            if(entry == null) {
                IntFunction<MessageBatch> creator_func=cap -> new MessageBatch(cap).dest(dest)
                  .clusterName(tp.getClusterNameAscii()). sender(sender).multicast(dest == null);
                Entry tmp=map.putIfAbsent(sender, entry=new Entry(creator_func));
                if(tmp != null)
                    entry=tmp;
            }
            return entry;
        }

        protected void clear() {map.clear();}

        protected void process(Message msg, boolean loopback) {
            Address dest=msg.getDest(), sender=msg.getSrc();
            get(dest, sender).process(msg, loopback);
        }

        protected void process(MessageBatch batch) {
            Address dest=batch.dest(), sender=batch.sender();
            get(dest, sender).process(batch);
        }

        protected void viewChange(List<Address> mbrs) {
            map.keySet().retainAll(mbrs);
        }

        public String toString() {
            return map.entrySet().stream().collect(StringBuilder::new, (sb,e) -> sb.append(e).append("\n"), (a,b) -> {}).toString();
        }
    }


    protected class Entry {
        protected final Lock                lock=new ReentrantLock();
        protected boolean                   running;       // if true, a thread is delivering a message batch to the application
        protected MessageBatch              batch;         // used to queue messages
        protected IntFunction<MessageBatch> batch_creator; // creates a batch

        protected final LongAdder           submitted_msgs=new LongAdder();
        protected final LongAdder           submitted_batches=new LongAdder();
        protected final LongAdder           queued_msgs=new LongAdder();
        protected final LongAdder           queued_batches=new LongAdder();


        protected Entry(IntFunction<MessageBatch> creator) {
            batch_creator=creator;
            batch=batch_creator.apply(max_buffer_size > 0? max_buffer_size : 16); // initial capacity
        }


        public Entry reset() {
            Stream.of(submitted_msgs, submitted_batches, queued_msgs, queued_batches).forEach(LongAdder::reset);
            return this;
        }


        protected void process(Message msg, boolean loopback) {
            if(!allowedToSubmitToThreadPool(msg))
                return;
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            submit(msg, loopback);
        }

        protected void process(MessageBatch batch) {
            if(!allowedToSubmitToThreadPool(batch))
                return;
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            submit(batch);
        }

        protected void submit(Message msg, boolean loopback) {
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            try {
                submitted_msgs.increment();
                BatchHandlerLoop handler=new BatchHandlerLoop(batch_creator.apply(16).add(msg), this, loopback);
                if(!tp.submitToThreadPool(handler, false))
                    setRunning(false);
            }
            catch(Throwable t) {
                setRunning(false);
            }
        }

        protected void submit(MessageBatch mb) {
            try {
                submitted_batches.increment();
                BatchHandlerLoop handler=new BatchHandlerLoop(batch_creator.apply(mb.size()).add(mb), this, false);
                if(!tp.submitToThreadPool(handler, false))
                    setRunning(false);
            }
            catch(Throwable t) {
                setRunning(false);
            }
        }


        /**
         * Either allows access to submit a task to the thread pool for delivery to the application, or queues the
         * message
         * @param msg the message
         * @return true if the message can be submitted to the thread pool, or false (msg was queued)
         */
        protected boolean allowedToSubmitToThreadPool(Message msg) {
            lock.lock();
            try {
                if(!running)
                    return running=true; // the caller can submit a new BatchHandlerLoop task to the thread pool
                this.batch.add(msg, resize);
                queued_msgs.increment();
                return false;
            }
            finally {
                lock.unlock();
            }
        }

        protected boolean allowedToSubmitToThreadPool(MessageBatch msg_batch) {
            lock.lock();
            try {
                if(!running)
                    return running=true; // the caller can submit a new BatchHandlerLoop task to the thread pool
                this.batch.add(msg_batch, resize);
                queued_batches.increment();
                return false;
            }
            finally {
                lock.unlock();
            }
        }

        /** Called by {@link BatchHandlerLoop}. Atomically transfer messages from the entry's batch to this batch and
         * returns true if messages were transferred. If not, sets running to false and returns false. In the latter
         * case, the handler must terminate (or else, we could have multiple handler running).
         * @param msg_batch the batch to which messages from this.batch should be transferred to.
         */
        protected boolean workAvailable(final MessageBatch msg_batch) {
            lock.lock();
            try {
                int num_msgs=msg_batch.transferFrom(this.batch, true);
                return num_msgs > 0 || (running=false);
            }
            catch(Throwable t) {
                return running=false;
            }
            finally {
                lock.unlock();
            }
        }

        protected void setRunning(boolean flag) {
            lock.lock();
            try {
                running=flag;
            }
            finally {
                lock.unlock();
            }
        }

        // unsynchronized on batch but who cares
        public String toString() {
            return String.format("batch size=%d queued msgs=%d queued batches=%d submitted msgs=%d submitted batches=%d",
                                 batch.size(), queued_msgs.sum(), queued_batches.sum(), submitted_msgs.sum(), submitted_batches.sum());
        }
    }


    protected class BatchHandlerLoop extends BatchHandler {
        protected final Entry   entry;
        protected final boolean loopback;

        public BatchHandlerLoop(MessageBatch batch, Entry entry, boolean loopback) {
            super(batch);
            this.entry=entry;
            this.loopback=loopback;
        }

        public void run() {
            do {
                try {
                    super.run();
                }
                catch(Throwable t) {
                    log.error("failed processing batch", t);
                }
            }
            while(entry.workAvailable(this.batch)); // transfers msgs from entry.batch --> this.batch
            // worker termination: workAvailable() already set running=false
        }

        @Override protected void passBatchUp() {
            tp.passBatchUp(batch, !loopback, !loopback);
        }
    }
}

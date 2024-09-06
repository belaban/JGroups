package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.TP;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * {@link org.jgroups.stack.MessageProcessingPolicy} which processes <em>regular</em> messages and message batches by
 * assigning a max of 1 thread per message from the same sender. So if we have senders A, B, C and D, we'll have no more
 * than 4 threads handling regular unicasts and 4 threads handling regular multicasts.<p>
 * See <a href="https://issues.redhat.com/browse/JGRP-2143">JGRP-2143</a> for details.<br/>
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
        return table.process(batch);
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
            // not so elegant, but avoids lambda allocation!
            Entry tmp=map.putIfAbsent(sender, (e=new Entry(sender, multicast, tp.getClusterNameAscii())));
            return tmp!= null? tmp: e;
            // return map.computeIfAbsent(sender, s -> new Entry(sender, multicast, tp.getClusterNameAscii()));
        }

        protected void clear() {map.clear();}

        protected boolean process(Message msg, boolean loopback) {
            Address dest=msg.getDest(), sender=msg.getSrc();
            return sender != null && get(sender, dest == null).process(msg, loopback);
        }

        protected boolean process(MessageBatch batch) {
            Address dest=batch.dest(), sender=batch.sender();
            return get(sender, dest == null).process(batch);
        }

        protected void viewChange(List<Address> mbrs) {
            // remove all senders that are not in the new view
            map.keySet().retainAll(mbrs);
        }

        public String toString() {
            return map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
        }
    }


    protected class Entry {
        protected final Lock         lock=new ReentrantLock();
        protected boolean            running;  // if true, a thread is delivering a message batch to the application
        protected final boolean      mcast;
        protected final MessageBatch batch;    // used to queue messages
        protected final Address      sender;
        protected final AsciiString  cluster_name;

        protected long               submitted_msgs;
        protected long               submitted_batches;
        protected long               queued_msgs;
        protected long               queued_batches;


        protected Entry(Address sender, boolean mcast, AsciiString cluster_name) {
            this.mcast=mcast;
            this.sender=sender;
            this.cluster_name=cluster_name;
            int cap=max_buffer_size > 0? max_buffer_size : 512; // initial capacity
            batch=new MessageBatch(cap).dest(tp.getAddress()).sender(sender).clusterName(cluster_name).multicast(mcast);
            batch.array().increment(512);
        }


        public Entry reset() {
            submitted_msgs=submitted_batches=queued_msgs=queued_batches=0;
            return this;
        }


        protected boolean process(Message msg, boolean loopback) {
            if(!allowedToSubmitToThreadPool(msg))
                return false;
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            return submit(msg, loopback);
        }

        protected boolean process(MessageBatch batch) {
            if(!allowedToSubmitToThreadPool(batch))
                return false;
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            return submit(batch);
        }

        protected boolean submit(Message msg, boolean loopback) {
            // running is true, we didn't queue msg and need to submit a task to the thread pool
            try {
                submitted_msgs++;
                MessageBatch mb=new MessageBatch(batch.capacity()).sender(sender).dest(mcast? null : tp.getAddress())
                  .clusterName(cluster_name).multicast(mcast).add(msg);
                BatchHandlerLoop handler=new BatchHandlerLoop(mb, this, loopback);
                if(!tp.getThreadPool().execute(handler)) {
                    setRunning(false);
                    return false;
                }
                return true;
            }
            catch(Throwable t) {
                setRunning(false);
                return false;
            }
        }

        protected boolean submit(MessageBatch batch) {
            try {
                submitted_batches++;
                BatchHandlerLoop handler=new BatchHandlerLoop(batch, this, false);
                if(!tp.getThreadPool().execute(handler)) {
                    setRunning(false);
                    return false;
                }
                return true;
            }
            catch(Throwable t) {
                setRunning(false);
                return false;
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
                this.batch.add(msg, max_buffer_size == 0);
                queued_msgs++;
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
                this.batch.add(msg_batch, max_buffer_size == 0);
                queued_batches++;
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
                                 batch.size(), queued_msgs, queued_batches, submitted_msgs, submitted_batches);
        }
    }


    protected class BatchHandlerLoop extends BatchHandler {
        protected final Entry   entry;

        protected BatchHandlerLoop(MessageBatch batch, Entry entry, boolean loopback) {
            super(batch, loopback);
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
    }
}

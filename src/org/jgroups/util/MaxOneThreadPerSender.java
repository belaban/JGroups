package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
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

    @Property(description="The initial capacity of the message batch. When exceeded, the batch's capacity " +
      "will be increased. A view change resets this value")
    protected int                batch_capacity=128;

    @Property(description="The fixed capacity of the queue used to buffer incoming messages. Messages exceeding" +
      "this capacity are discarded. The consumer blocks on an empty queue")
    protected int                queue_capacity=2048;

    public int                   batchCapacity()      {return batch_capacity;}
    public MaxOneThreadPerSender batchCapacity(int c) {this.batch_capacity=c; return this;}
    public int                   queueCapacity()      {return queue_capacity;}
    public MaxOneThreadPerSender queueCapacity(int c) {this.queue_capacity=c; return this;}

    @ManagedOperation(description="Dumps unicast and multicast tables")
    public String dump() {
        return String.format("\nmcasts:\n%s\nucasts:\n%s", mcasts, ucasts);
    }

    public void reset() {
        mcasts.map.values().forEach(Entry::reset);
        ucasts.map.values().forEach(Entry::reset);
    }

    public void destroy() {
        mcasts.clear();
        ucasts.clear();
    }

    public boolean loopback(Message msg, boolean oob) {
        if(oob)
            return super.loopback(msg, oob);
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        return table.process(msg);
    }

    public boolean loopback(MessageBatch batch, boolean oob) {
        if(oob)
            return super.loopback(batch, oob);
        MessageTable table=batch.dest() == null? mcasts : ucasts;
        return table.process(batch);
    }

    public boolean process(Message msg, boolean oob) {
        if(oob)
            return super.process(msg, oob);
        MessageTable table=msg.getDest() == null? mcasts : ucasts;
        return table.process(msg);
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

        protected Entry get(final Address sender, final Address dest) {
            Entry e=map.get(sender);
            if(e != null)
                return e;
            e=map.computeIfAbsent(sender, s -> new Entry(sender, dest, tp.getClusterNameAscii()).start());
            return e;
        }

        protected void clear() {
            map.values().forEach(Util::close);
            map.clear();
        }

        protected boolean process(Message msg) {
            Address dest=msg.getDest(), sender=msg.getSrc();
            return sender != null && get(sender, dest).process(msg);
        }

        protected boolean process(MessageBatch batch) {
            Address dest=batch.dest(), sender=batch.sender();
            return get(sender, dest).process(batch);
        }

        protected void viewChange(List<Address> mbrs) {
            // close all entries for sender which are not in the new view (stops mq-handler runner thread)
            for(Map.Entry<Address,Entry> e: map.entrySet()) {
                Address key=e.getKey();
                if(!mbrs.contains(key)) {
                    Entry val=e.getValue();
                    Util.close(val);
                }
            }
            // remove all senders that are not in the new view
            map.keySet().retainAll(mbrs);
            map.values().forEach(Entry::trimToInitialCapacity);
        }

        public String toString() {
            return map.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue())).collect(Collectors.joining("\n"));
        }
    }


    protected class Entry implements Closeable {
        protected final MessageBatch           batch;      // grabs queued msgs from msg_queue and passes them up the stack
        protected final BlockingQueue<Message> mq;         // Producers (receiver threads) add messages to this queue
        protected final Runner                 mq_handler; // consumes messages/batches from mq and sends them up
        protected final Address                dest, sender;
        protected final AsciiString            cluster_name;
        protected final LongAdder              queued_msgs=new LongAdder();
        protected final boolean                loopback;


        protected Entry(Address sender, Address dest, AsciiString cluster_name) {
            this.sender=sender;
            this.dest=dest;
            this.cluster_name=cluster_name;
            batch=new MessageBatch(batch_capacity).dest(dest).sender(sender).clusterName(cluster_name)
              .multicast(dest == null).mode(MessageBatch.Mode.REG); // only regular messages are queued
            batch.array().increment(128);

            // the queue blocks the runner (mq-handler) when empty, but discards new messages when full
            // (they will get retransmitted anyway)
            mq=new ConcurrentBlockingRingBuffer<>(queue_capacity, true, false);
            mq_handler=new Runner(tp.getThreadFactory(),
                                  String.format("mq-handler-%s-%s-%s", cluster_name, sender, dest == null? "mcast" : "ucast"),
                                  this::run, null);
            loopback=Objects.equals(sender, tp.addr());
        }

        public Entry reset() {
            queued_msgs.reset();
            return this;
        }

        public Entry start() {
            mq_handler.start();
            return this;
        }

        public void stop(){
            mq_handler.stop();
        }

        @Override
        public void close() throws IOException {
            stop();
        }

        public Entry trimToInitialCapacity() {
            batch.array().trimTo(batch_capacity);
            return this;
        }

        protected boolean process(Message msg) {
            if(mq.offer(msg))
                queued_msgs.increment();
            return true;
        }

        protected boolean process(MessageBatch batch) {
            int added=0;
            for(Message msg: batch) {
                if(mq.offer(msg) == false)
                    break;
                added++;
            }
            queued_msgs.add(added);
            return true;
        }

        protected void run() {
            try {
                batch.reset();
                batch.add(mq.take());
                int size=mq.size();
                if(size > 0) {
                    FastArray<Message> array=batch.array();
                    int cap=array.capacity(), index=array.index();
                    if(index + size > cap)
                        array.resize(index + size);
                    mq.drainTo(array);
                }

                if(!batch.multicast()) {
                    // due to an incorrect (e.g. late) view change, the cached batch's destination might be
                    // different from our local address. If this is the case, change the cached batch's dest address
                    if(tp.unicastDestMismatch(batch.dest())) {
                        Address d=tp.addr();
                        if(d != null)
                            batch.dest(d);
                    }
                }
                // https://issues.redhat.com/browse/JGRP-2958
                if(batch.size() == 1) {
                    Message msg=batch.first();
                    tp.passMessageUp(msg, !this.loopback, msg.dest() == null, !this.loopback);
                }
                else
                    tp.passBatchUp(batch, !this.loopback, !this.loopback);
            }
            catch(InterruptedException iex) {
            }
            catch(Throwable t) {
                // Will not throw an exception, e.g. an OOME raised by log.error() above: don't terminate with
                // entry.adders > 0, or else no further messages from that sender would ever be delivered:
                // https://redhat.atlassian.net/browse/JGRP-3032.
                // NPE due to null 'log' is impossible (log is guaranteed to be non-bull)
                log.failSafeError("failed processing batch", t);
            }
        }

        // unsynchronized on batch but who cares
        public String toString() {
            return String.format("mq.size=%,d mq.cap: %,d batch.cap=%,d queued msgs=%,d", mq.size(),
                                 ((ConcurrentBlockingRingBuffer<?>)mq).capacity(), batch.capacity(), queued_msgs.sum());
        }
    }


}

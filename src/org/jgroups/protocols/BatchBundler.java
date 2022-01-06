package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.NullAddress;
import org.jgroups.View;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.jgroups.protocols.TP.MSG_OVERHEAD;

/**
 * Bundler based on {@link BATCH}. Batches messages, keeping a {@link #max_size} for every destination.
 * When the accumulated size of the messages for a given destination P would exceed max_bytes, a MessageBatch is
 * created and sent to P.<br/>
 * Additionally, a timer runs every {@link #flush_interval} milliseconds, sending messages whose size hasn't yet
 * reached max_size.<br/>
 * Contrary to {@link TransferQueueBundler}, which maintains a max_size for all messages, {@link BatchBundler} maintains
 * it for every destination separately. This causes batches to be fuller than with {@link TransferQueueBundler}:
 * assuming 4 members, everyone sending to everyone else, and max_size = 60000: with TransferQueueBundler, a batch is
 * sent with ~15'000 bytes of messages (60'000/4), but with BatchBundler, it has ~60'000 bytes. Fuller batches means
 * more amortization of costs of handling single messages.
 * @author Bela Ban
 * @since 5.2
 */
@Experimental
public class BatchBundler extends NoBundler {

    /**
     * Maximum number of bytes for messages to be queued until they are sent.
     * This value needs to be smaller than the largest datagram packet size in case of UDP
     */
    @Property(name="max_size", type=AttributeType.BYTES,
      description="Maximum number of bytes for messages to be queued until they are sent")
    protected int                    max_size=64000;

    @Property(description="Max interval (millis) at which the queued messages are sent")
    protected long                   flush_interval=100;

    @Property(description="The maximum number of messages per batch")
    public int                       max_batch_size = 1000;

    @ManagedAttribute(description="Local address")
    protected volatile Address       local_addr;

    @ManagedAttribute(description="Number of messages sent in BatchMessages",type=AttributeType.SCALAR)
    protected long                   num_msgs_sent;

    @ManagedAttribute(description="Number of BatchMessages sent",type=AttributeType.SCALAR)
    protected long                   num_ebs_sent;

    @ManagedAttribute(description="Number of BatchMessages sent because the queue was full",type=AttributeType.SCALAR)
    protected long                   num_ebs_sent_due_to_full_queue;

    @ManagedAttribute(description="Number of BatchMessages sent because the max number of messages has been " +
      "reached (max_batch_size)", type=AttributeType.SCALAR)
    protected long                   num_ebs_sent_due_to_max_number_of_msgs;

    @ManagedAttribute(description="Number of BatchMessages sent because the timeout kicked in",
      type=AttributeType.SCALAR)
    protected long                   num_ebs_sent_due_to_timeout;

    protected ConcurrentMap<Address,Buffer> msgMap = Util.createConcurrentMap();

    protected final NullAddress      nullAddress = new NullAddress();
    protected TimeScheduler          timer;
    protected volatile boolean       running;
    protected Future<?>              flush_task;


    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        if(num_ebs_sent == 0 || num_msgs_sent == 0) return 0.0;
        return num_msgs_sent / (double)num_ebs_sent;
    }

    @Override
    public void resetStats() {
        num_msgs_sent=0;
        num_ebs_sent=0;
        num_ebs_sent_due_to_full_queue=0;
        num_ebs_sent_due_to_timeout=0;
        num_ebs_sent_due_to_max_number_of_msgs=0;
    }

    public void viewChange(View view) {
        List<Address> mbrs=view.getMembers();
        if(mbrs == null) return;

        mbrs.stream().filter(dest -> !msgMap.containsKey(dest))
          .forEach(dest -> msgMap.putIfAbsent(dest, new Buffer(dest)));

        // remove members that left
        //msgMap.keySet().retainAll(mbrs);
        // Tries to send remaining messages so could potentially block, might not be necessary?
        // Potentially can forward messages out of order as remove and close are not synced but it isn't in view anyway
        msgMap.keySet().stream().filter(dest -> !mbrs.contains(dest) && !(dest instanceof NullAddress)).forEach(dest -> {
            Buffer removed = msgMap.remove(dest);
            removed.close();
        });
    }


    public void init(TP transport) {
        super.init(transport);
        // msgMap.putIfAbsent(nullAddress, new BatchBuffer(nullAddress));
    }

    public void start() {
        timer=transport.getTimer();
        if(timer == null)
            throw new RuntimeException("timer is null");
        local_addr=Objects.requireNonNull(transport.getAddress());
        running=true;
        startFlushTask();
    }

    public void stop() {
        running=false;
        stopFlushTask();
    }

    public void send(Message msg) throws Exception {
        if (msg.isFlagSet(Message.Flag.OOB)) {
            super.send(msg);
            return;
        }

        if (msg.getSrc() == null)
            msg.setSrc(local_addr);
        // Ignore messages from other senders due to BatchMessage compression
        if (!Objects.equals(msg.getSrc(), local_addr)) {
            super.send(msg);
            return;
        }

        Address dest = msg.dest() == null ? nullAddress : msg.dest();
        Buffer ebbuffer = msgMap.get(dest);
        if (ebbuffer == null)
            ebbuffer=msgMap.computeIfAbsent(dest, k -> new Buffer(dest));
        boolean add_successful = ebbuffer.addMessage(msg);

        if (!add_successful) {
            super.send(msg);
        }
    }



    public int size() {
        return 0;
    }

    public int getQueueSize() {
        return 0;
    }

    public int getCapacity() {
        return max_batch_size;
    }

    public int getMaxSize() {
        return max_size;
    }

    public Bundler setMaxSize(int s) {
        this.max_size=s;
        return this;
    }

    protected void startFlushTask() {
        if(flush_task == null || flush_task.isDone())
            flush_task=timer.scheduleWithFixedDelay(new FlushTask(), 0, flush_interval, TimeUnit.MILLISECONDS, true);
    }

    protected void stopFlushTask() {
        if(flush_task != null) {
            flush_task.cancel(true);
            flush_task=null;
        }
    }

    protected void _send(Message msg, ByteArrayDataOutputStream out) {
        try {
            sendSingleMessage(msg, out);
        }
        catch(Exception e) {
            log.error("%s: failed sending message: %s", local_addr, e);
        }
    }

    protected class FlushTask implements Runnable {
        public void run() {
            flush();
        }

        public String toString() {
            return BatchBundler.class.getSimpleName() + ": FlushTask (interval=" + flush_interval + " ms)";
        }
    }

    public void flush() {
        msgMap.forEach((k,v) -> v.sendBatch(true));
    }

    protected class Buffer {
        private final Address          dest;
        private final Message[]        msgs;
        private int                    index;
        private boolean                closed;
        private long                   total_bytes;
        private final ByteArrayDataOutputStream output;

        protected Buffer(Address address) {
            this.dest=address;
            this.msgs = new Message[max_batch_size];
            this.index = 0;
            output=new ByteArrayDataOutputStream(max_size + MSG_OVERHEAD);
        }

        protected synchronized boolean addMessage(Message msg) {
            if (closed) {
                return false;
            }

            int msg_bytes = msg.getLength();
            if(total_bytes + msg_bytes > transport.getBundler().getMaxSize()) {
                num_ebs_sent_due_to_full_queue++;
                sendBatch(false);
            }

            msgs[index++] = msg;
            total_bytes += msg_bytes;
            if (index == msgs.length) {
                num_ebs_sent_due_to_max_number_of_msgs++;
                sendBatch(false);
            }
            return true;
        }

        protected synchronized void sendBatch(boolean due_to_timeout) {
            if (index == 0) {
                return;
            }
            if (index == 1) {
                _send(msgs[0], output);
                msgs[0] = null;
                index = 0;
                total_bytes = 0;
                num_msgs_sent++;
                return;
            }

            Address ebdest = dest instanceof NullAddress ? null : dest;
            sendMessageList(ebdest, local_addr, msgs, index);
           // msgs = new Message[max_batch_size];
            num_msgs_sent+=index;
            num_ebs_sent++;
            if(due_to_timeout)
                num_ebs_sent_due_to_timeout++;
            index=0;
            total_bytes = 0;
        }

        protected void sendMessageList(final Address dest, final Address src, final Message[] list, int length) {
            try {
                output.position(0);
                Util.writeMessageList(dest, src, transport.cluster_name.chars(), list, 0,
                                      length, output, dest == null, transport.getId());
                transport.doSend(output.buffer(), 0, output.position(), dest);
            }
            catch(Throwable e) {
                log.trace(Util.getMessage("FailureSendingMsgBundle"), transport.getAddress(), e);
            }
        }

        protected synchronized void close() {
            this.closed = true;
            sendBatch(false);
        }
    }


}

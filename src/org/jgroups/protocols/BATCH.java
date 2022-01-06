package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Batches messages near the top of the stack.  This reduces the work done on the IO thread and reduces overhead,
 * greatly increasing throughput for smaller message sizes (<1k in test configurations).  Also reduces the amount of
 * header data by having one header for each batch.
 * Conceptually, down messages are buffered then put in a wrapper message, so lower protocols only interact with the
 * wrapper.  On the receiving end, the batch is unwrapped when it reaches this protocol and then forwarded to higher
 * protocols as individual messages in a loop.
 * @author Chris Johnson
 * @since 5.x
 */
@Experimental
@MBean(description="Protocol just below flow control that wraps messages to improve throughput with small messages.")
public class BATCH extends Protocol {

    @Property(description="Max interval (millis) at which the queued messages are sent")
    protected long                   flush_interval=100;

    @Property(description="The maximum number of messages per batch")
    public int                       max_batch_size = 100;

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

    protected final NullAddress      nullAddress = new NullAddress();
    protected TimeScheduler          timer;
    protected volatile boolean       running;
    protected Future<?>              flush_task;
    protected Map<Address,Buffer>    msgMap = Util.createConcurrentMap();
    protected static BatchHeader     HEADER= new BatchHeader();


    @ManagedAttribute(description="Average number of messages in an BatchMessage")
    public double avgBatchSize() {
        if(num_ebs_sent == 0 || num_msgs_sent == 0) return 0.0;
        return num_msgs_sent / (double)num_ebs_sent;
    }

    public void init() throws Exception {
        msgMap.putIfAbsent(nullAddress, new Buffer(nullAddress));
    }

    public void resetStats() {
        super.resetStats();
        num_msgs_sent=0;
        num_ebs_sent=0;
        num_ebs_sent_due_to_full_queue=0;
        num_ebs_sent_due_to_timeout=0;
        num_ebs_sent_due_to_max_number_of_msgs=0;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View v=evt.getArg();
                handleViewChange(v.getMembers());
                break;
        }
        return down_prot.down(evt); // this could potentially use the lower protocol's thread which may block
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleViewChange(((View)evt.getArg()).getMembers());
                break;
        }
        return up_prot.up(evt);
    }

    protected void handleViewChange(List<Address> mbrs) {
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

    public Object down(Message msg) {
        if (msg.isFlagSet(Message.Flag.OOB))
            return down_prot.down(msg);
        if (msg.getSrc() == null)
            msg.setSrc(local_addr);
        // Ignore messages from other senders due to BatchMessage compression
        if (!Objects.equals(msg.getSrc(), local_addr))
            return down_prot.down(msg);

        Address dest = msg.dest() == null ? nullAddress : msg.dest();
        Buffer ebbuffer = msgMap.computeIfAbsent(dest, k -> new Buffer(dest));
        boolean add_successful = ebbuffer.addMessage(msg);

        if (!add_successful)
            return down_prot.down(msg);
        return msg;
    }

    public Object up(Message msg) {
        if(msg.getHeader(getId()) == null)
            return up_prot.up(msg);

        BatchMessage comp = (BatchMessage) msg;
        for(Message bundledMsg: comp) {
            bundledMsg.setDest(comp.getDest());
            if (bundledMsg.getSrc() == null)
                bundledMsg.setSrc(comp.getSrc());
        }
        MessageBatch batch=new MessageBatch();
        batch.set(comp.getDest(), comp.getSrc(), comp.getMessages());
        if(!batch.isEmpty())
            up_prot.up(batch);
        return null;
    }

    public void up(MessageBatch batch) {
        int len=0;

        for(Message msg: batch)
            if(msg instanceof BatchMessage)
                len+=((BatchMessage)msg).getNumberOfMessages();

        if(len > 0) {
            // remove BatchMessages and add their contents to a new batch
            MessageBatch mb=new MessageBatch(len+1).setDest(batch.dest()).setSender(batch.getSender());
            for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
                Message m=it.next();
                if(m instanceof BatchMessage) {
                    BatchMessage ebm=(BatchMessage)m;
                    it.remove();
                    mb.add(ebm.getMessages(), ebm.getNumberOfMessages());
                }
            }
            if(!mb.isEmpty())
                up_prot.up(mb);
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    public void start() throws Exception {
        timer=getTransport().getTimer();
        if(timer == null)
            throw new Exception("timer is null");
        running=true;
        startFlushTask();
    }

    public void stop() {
        running=false;
        stopFlushTask();
    }

    public void startFlushTask() {
        if(flush_task == null || flush_task.isDone())
            flush_task=timer.scheduleWithFixedDelay(new BATCH.FlushTask(), 0, flush_interval, TimeUnit.MILLISECONDS, true);
    }

    public void stopFlushTask() {
        if(flush_task != null) {
            flush_task.cancel(true);
            flush_task=null;
        }
    }

    protected class FlushTask implements Runnable {
        public void run() {
            flush();
        }

        public String toString() {
            return BATCH.class.getSimpleName() + ": FlushTask (interval=" + flush_interval + " ms)";
        }
    }

    public void flush() {
        msgMap.forEach((k,v) -> v.sendBatch(true));
    }

    protected class Buffer {
        private final Address    dest;
        private Message[]        msgs;
        private int              index;
        private boolean          closed;
        private long             total_bytes;

        protected Buffer(Address address) {
            this.dest=address;
            this.msgs = new Message[max_batch_size];
            this.index = 0;
        }

        protected synchronized boolean addMessage(Message msg) {
            if (closed) {
                return false;
            }

            int msg_bytes = msg.getLength();
            if(total_bytes + msg_bytes > getTransport().getBundler().getMaxSize()) {
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
                down_prot.down(msgs[0]);
                msgs[0] = null;
                index = 0;
                total_bytes = 0;
                num_msgs_sent++;
                return;
            }

            Address ebdest = dest instanceof NullAddress ? null : dest;
            Message comp = new BatchMessage(ebdest, local_addr, msgs, index)
              .putHeader(id, HEADER)
              .setSrc(local_addr);
            msgs = new Message[max_batch_size];
            num_msgs_sent+=index;
            num_ebs_sent++;
            if(due_to_timeout)
                num_ebs_sent_due_to_timeout++;
            index=0;
            total_bytes = 0;
            // Could send down out of synchronize, but that could make batches hit nakack out of order
            down_prot.down(comp);
        }

        protected synchronized void close() {
            this.closed = true;
            sendBatch(false);
        }
    }

    public static class BatchHeader extends Header {

        public BatchHeader() {
        }

        public short                      getMagicId()                               {return 95;}
        public Supplier<? extends Header> create()                                   {return BatchHeader::new;}
        @Override public int              serializedSize()                           {return 0;}
        @Override public void             writeTo(DataOutput out) throws IOException {}
        @Override public void             readFrom(DataInput in) throws IOException  {}
        public String                     toString()                                 {return "BatchHeader";}
    }

}

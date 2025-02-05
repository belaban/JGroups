package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * Protocol measuring delivery times. Can be used in the up or down direction
 * JIRA: https://issues.redhat.com/browse/JGRP-2101
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Measures message delivery times")
public class TIME extends Protocol {
    @ManagedAttribute(description="Average delivery time (in microseconds) for single messages")
    protected final AverageMinMax up_delivery_msgs=new AverageMinMax().unit(TimeUnit.NANOSECONDS);

    @ManagedAttribute(description="Average delivery time (in microseconds) for message batches")
    protected final AverageMinMax up_delivery_batches=new AverageMinMax().unit(TimeUnit.NANOSECONDS);

    @ManagedAttribute(description="Average size of received message batches")
    protected final AverageMinMax avg_up_batch_size=new AverageMinMax();

    @ManagedAttribute(description="Average down delivery time (in microseconds)")
    protected final AverageMinMax down_delivery=new AverageMinMax().unit(TimeUnit.NANOSECONDS);

    @Property(description="Enables or disables measuring times of messages sent down")
    protected boolean             down_msgs=true;

    @Property(description="Enables or disables measuring times of message batches received from below")
    protected boolean             up_batches=true;

    @Property(description="Enables or disables measuring times of messages received from below. Attribute " +
      "up_batches has to be true, or else up_msgs will be ignored")
    protected boolean             up_msgs=true;

    @Property(description="The number of (regular plus OOB) batches passed up to the application",type=SCALAR)
    protected final LongAdder     num_batches=new LongAdder();

    @Property(description="The number of OOB batches passed up to the application",type=SCALAR)
    protected final LongAdder     num_batches_oob=new LongAdder();

    @Property(description="The number of regular batches passed up to the application",type=SCALAR)
    protected final LongAdder     num_batches_reg=new LongAdder();

    @Property(description="The number of (regular plus OOB) messages passed down by the application",type=SCALAR)
    protected final LongAdder     num_down_msgs=new LongAdder();

    @Property(description="The number of OOB messages passed down by the application",type=SCALAR)
    protected final LongAdder     num_down_msgs_oob=new LongAdder();

    @Property(description="The number of regular messages passed down by the application",type=SCALAR)
    protected final LongAdder     num_down_msgs_reg=new LongAdder();

    @Property(description="The number of (regular and OOB) messages passed up to the application",type=SCALAR)
    protected final LongAdder     num_up_msgs=new LongAdder();

    @Property(description="The number of OOB messages passed up to the application",type=SCALAR)
    protected final LongAdder     num_up_msgs_oob=new LongAdder();

    @Property(description="The number of regular messages passed up to the application",type=SCALAR)
    protected final LongAdder     num_up_msgs_reg=new LongAdder();

    public void resetStats() {
        down_delivery.clear();
        up_delivery_msgs.clear();
        up_delivery_batches.clear();
        avg_up_batch_size.clear();
        num_batches.reset();
        num_batches_oob.reset();
        num_batches_reg.reset();
        num_down_msgs.reset();
        num_down_msgs_oob.reset();
        num_down_msgs_reg.reset();
        num_up_msgs.reset();
        num_up_msgs_oob.reset();
        num_up_msgs_reg.reset();
    }


    public Object down(Message msg) {
        if(!down_msgs)
            return down_prot.down(msg);
        long start=System.nanoTime();
        try {
            return down_prot.down(msg);
        }
        finally {
            long time=System.nanoTime() - start;
            down_delivery.add(time);
            num_down_msgs.increment();
            if(msg.isFlagSet(OOB))
                num_down_msgs_oob.increment();
            else
                num_down_msgs_reg.increment();
        }
    }

    public Object up(Message msg) {
        if(!up_msgs)
            return up_prot.up(msg);

        long start=System.nanoTime();
        try {
            return up_prot.up(msg);
        }
        finally {
            long time=System.nanoTime() - start;
            up_delivery_msgs.add(time);
            num_up_msgs.increment();
            if(msg.isFlagSet(OOB))
                num_up_msgs_oob.increment();
            else
                num_up_msgs_reg.increment();
        }
    }


    /**
     * Dividing the delivery time for a batch by the batch size is problematic, e.g. if a batch of 5 is
     * received at time 0 and the 5 messages are delivered at times 20, 40, 60, 80 and 100, then the total
     * time is 100, divided by 5 would be 20 per message.<br/>
     * However, this is incorrect as it ignores the waiting times for the individual messages: e.g. message 3 gets
     * delayed for 60 until it is processed.<br/>
     * The correct average delivery time per message is therefore (20+40+60+80+100)/5 = 60.
     * <br/>
     * The above computation is not correct: we don't know whether a protocol looks at all messages: it may even
     * remove some (making 'size' incorrect)!<br/>
     * Also, we don't know whether messages in a batch are processed in order, on by one, or whether they are processed
     * in parallel. Therefore, times for individual messages will not be computed.
     */
    public void up(MessageBatch batch) {
        if(!up_batches) {
            up_prot.up(batch);
            return;
        }
        int size=batch.size();
        long start=System.nanoTime();
        try {
            up_prot.up(batch);
        }
        finally {
            long time=System.nanoTime() - start;
            up_delivery_batches.add(time);
            avg_up_batch_size.add(size);
            num_batches.increment();
            if(batch.mode() == MessageBatch.Mode.OOB)
                num_batches_oob.increment();
            else
                num_batches_reg.increment();
        }
    }


}

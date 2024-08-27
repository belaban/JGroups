package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;

import java.util.concurrent.TimeUnit;

/**
 * Protocol measuring delivery times. Can be used in the up or down direction
 * JIRA: https://issues.redhat.com/browse/JGRP-2101
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Measures message delivery times")
public class TIME extends Protocol {
    @ManagedAttribute(description="Average delivery time (in microseconds) for single messages")
    protected final AverageMinMax up_delivery_msgs=new AverageMinMax().unit(TimeUnit.MICROSECONDS);

    @ManagedAttribute(description="Average delivery time (in microseconds) for message batches")
    protected final AverageMinMax up_delivery_batches=new AverageMinMax().unit(TimeUnit.MICROSECONDS);

    @ManagedAttribute(description="Average size of received message batches")
    protected final AverageMinMax avg_up_batch_size=new AverageMinMax();

    @ManagedAttribute(description="Average down delivery time (in microseconds)")
    protected final AverageMinMax down_delivery=new AverageMinMax().unit(TimeUnit.MICROSECONDS);

    @Property(description="Enables or disables measuring times of messages sent down")
    protected boolean             down_msgs;

    @Property(description="Enables or disables measuring times of message batches received from below")
    protected boolean             up_batches=true;

    @Property(description="Enables or disables measuring times of messages received from below. Attribute " +
      "up_batches has to be true, or else up_msgs will be ignored")
    protected boolean             up_msgs;

    /*@Property(description="When enabled, the delay until a message in a batch is processed gets added to the " +
      "up-processing time of a message. Otherwise the up-processing time of a message is the up-processing " +
      "time of a batch divided by the batch size")
    protected boolean include_delay=true;*/


    public void resetStats() {
        down_delivery.clear();
        up_delivery_msgs.clear();
        up_delivery_batches.clear();
        avg_up_batch_size.clear();
    }


    public Object down(Message msg) {
        if(!down_msgs)
            return down_prot.down(msg);
        long start=System.nanoTime();
        try {
            return down_prot.down(msg);
        }
        finally {
            long time=(System.nanoTime() - start) / 1000; // us
            add(down_delivery, time);
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
            long time=(System.nanoTime() - start) / 1000; // us
            add(up_delivery_msgs, time);
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
            long time=(System.nanoTime() - start) / 1000; // us
            add(up_delivery_batches, time);
            avg_up_batch_size.add(size);
        }
    }

    protected static double avgTimePerMessageIncludingDelay(int num, long time) {
        double avg_per_msg=(double)time / num, total=0;
        for(int i=0; i < num; i++)
            total+=(avg_per_msg * i) + avg_per_msg;
        return total/num;
    }

    protected static void add(final AverageMinMax avg, long num) {
        if(num > 0) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized(avg) {
                avg.add(num);
            }
        }
    }

}

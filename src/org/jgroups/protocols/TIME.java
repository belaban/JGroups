package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.concurrent.TimeUnit;

/**
 * Protocol measuring delivery times. Can be used in the up or down direction
 * JIRA: https://issues.jboss.org/browse/JGRP-2101
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Measures message delivery times")
public class TIME extends Protocol {
    protected final AverageMinMax up_delivery=new AverageMinMax(), down_delivery=new AverageMinMax();

    @Property(description="Enables or disables measuring times in the up direction")
    protected boolean up=true;

    @Property(description="Enables or disables measuring times in the down direction")
    protected boolean down;

    @ManagedAttribute(description="Average down delivery time (in microseconds). This is computed as the average " +
      "delivery time for sending a messages down, until the call returns",type=AttributeType.TIME,unit=TimeUnit.MICROSECONDS)
    public double getAvgDownDeliveryTime() {
        return down_delivery.average();
    }

    @ManagedAttribute(description="Average delivery time (in microseconds). This is computed as the average " +
      "delivery time for single messages, plus the delivery time for batches up the stack, until the call returns",
      type=AttributeType.TIME,unit=TimeUnit.MICROSECONDS)
    public double getAvgUpDeliveryTime() {
        return up_delivery.average();
    }


    public void resetStats() {
        down_delivery.clear();
        up_delivery.clear();
    }


    public Object down(Message msg) {
        if(!down)
            return down_prot.down(msg);
        long start=Util.micros();
        try {
            return down_prot.down(msg);
        }
        finally {
            long time=Util.micros()-start;
            down_delivery.add(time);
        }
    }

    public Object up(Message msg) {
        if(!up)
            return up_prot.up(msg);
        long start=Util.micros();
        try {
            return up_prot.up(msg);
        }
        finally {
            long time=Util.micros()-start;
            up_delivery.add(time);
        }
    }

    // Dividing the delivery time for a batch by the batch size is problematic, e.g. if a batch of 5 is
    // received at time 0 and the 5 messages are delivered at times 20, 40, 60, 80 and 100, then the total
    // time is 100, divided by 5 is 20 per message. However, this is incorrect as it ignores the waiting
    // times for the individual messages: e.g. message 3 has to wait for 60 until it gets processed.
    // The correct average delivery times would be (20+40+60+80+100)/5 = 60.
    // However, we don't know in up(MessageBatch) when the individual messages are delivered, so we here we
    // can only compute the delivery time of the entire batch
    public void up(MessageBatch batch) {
        if(!up) {
            up_prot.up(batch);
            return;
        }

        // int batch_size=batch.size();
        long start=Util.micros();
        try {
            up_prot.up(batch);
        }
        finally {
            long time=Util.micros()-start;
            //if(batch_size > 1)
              //  time/=batch_size; // cannot use batch.size() as message might have been removed from the batch!
            up_delivery.add(time);
        }
    }
}

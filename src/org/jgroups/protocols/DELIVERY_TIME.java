package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.stack.Protocol;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

/**
 * Protocol measuring delivery times. Should be placed towards the top of the stack.
 * JIRA: https://issues.jboss.org/browse/JGRP-2101
 * @author Bela Ban
 * @since  4.0
 */
@MBean(description="Measures message delivery times")
public class DELIVERY_TIME extends Protocol {
    protected AverageMinMax delivery_times=new AverageMinMax();

    @ManagedAttribute(description="Average delivery time (in microseconds). This is computed as the average " +
      "delivery time for single messages, plus the delivery time for batches")
    public double getAvgDeliveryTimeUs() {
        return delivery_times.average();
    }

    public void resetStats() {
        delivery_times.clear();
    }

    public Object up(Message msg) {
        long start=Util.micros();
        try {
            return up_prot.up(msg);
        }
        finally {
            long time=Util.micros()-start;
            delivery_times.add(time);
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
        // int batch_size=batch.size();
        long start=Util.micros();
        try {
            up_prot.up(batch);
        }
        finally {
            long time=Util.micros()-start;
            //if(batch_size > 1)
              //  time/=batch_size; // cannot use batch.size() as message might have been removed from the batch!
            delivery_times.add(time);
        }
    }
}

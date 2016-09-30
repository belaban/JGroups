package org.jgroups.protocols;

import org.jgroups.Event;
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

    @ManagedAttribute(description="Average delivery time (in microseconds). This is computed as the average delivery " +
      "time for single messages, plus the delivery time for batches divided by batch size")
    public double getAvgDeliveryTime() {
        return delivery_times.average();
    }

    public void resetStats() {
        delivery_times.clear();
    }

    public Object up(Event evt) {
        if(evt.getType() != Event.MSG)
            return up_prot.up(evt);
        long start=Util.micros();
        try {
            return up_prot.up(evt);
        }
        finally {
            long time=Util.micros()-start;
            delivery_times.add(time);
        }
    }

    public void up(MessageBatch batch) {
        int batch_size=batch.size();
        long start=Util.micros();
        try {
            up_prot.up(batch);
        }
        finally {
            long time=Util.micros()-start;
            if(batch_size > 1)
                time/=batch.size();
            delivery_times.add(time);
        }
    }
}

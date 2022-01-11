package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of Random Early Drop: messages are discarded when the bundler's queue in the transport nears exhaustion.
 * See Floyd and Van Jacobsen's paper for details.
 * @author Bela Ban
 * @since  5.0.0, 4.2.2
 */
@MBean(description="Implementation of Random Early Drop: messages are discarded when the bundler's queue in " +
  "the transport nears exhaustion")
public class RED extends Protocol {

    @Property(description="If false, all messages are passed down. Will be set to false if the bundler " +
      "returns a queue size of -1")
    protected boolean         enabled=true;

    @ManagedAttribute(description="The capacity of the queue (assumed to be constant)")
    protected int             queue_capacity;

    @Property(description="The min threshold (percentage between 0 and 1.0) below which no message is dropped")
    protected double          min_threshold=0.5;
    protected long            min;

    @Property(description="The max threshold (percentage between min_threshold and 1.0) above which all messages are dropped")
    protected double          max_threshold=1.0;
    protected long            max;

    @ManagedAttribute(description="The average number of elements in the bundler's queue. Computed as " +
      "o * (1 - 2^-wf) + c * (2^-wf) where o is the old average, c the current queue size amd wf the weight_factor")
    protected double          avg_queue_size;

    @Property(description="The weight used to compute the average queue size. The higher the value is, the less the " +
      "current queue size is taken into account. E.g. with 2, 25% of the current queue size and 75% of the old " +
      "average is taken to compute the new average. In other words: with a high value, the average will take " +
      "longer to reflect the current queue size.")
    protected double          weight_factor=2;

    protected final LongAdder dropped_msgs=new LongAdder(); // dropped messages
    protected final LongAdder total_msgs=new LongAdder();   // total messages looked at

    protected Bundler         bundler;
    protected final Lock      lock=new ReentrantLock();
    protected long            span=max-min; // diff between max and min
    protected double          weight=Math.pow(2, -weight_factor);


    public boolean isEnabled()           {return enabled;}
    public RED     setEnabled(boolean e) {enabled=e; return this;}
    public double  getMinThreshold()     {return min_threshold;}



    @ManagedAttribute(description="The number of dropped messages",type=AttributeType.SCALAR)
    public long    getDroppedMessages() {return dropped_msgs.sum();}

    @ManagedAttribute(description="Total number of messages processed",type=AttributeType.SCALAR)
    public long    getTotalMessages()   {return total_msgs.sum();}

    @ManagedAttribute(description="Percentage of all messages that were dropped")
    public double  getDropRate()        {return dropped_msgs.sum() / (double)total_msgs.sum();}


    public void start() throws Exception {
        super.start();
        bundler=getTransport().getBundler();
        enabled=bundler != null && bundler.getQueueSize() >= 0;
        if(enabled) {
            queue_capacity=getTransport().getBundler().getCapacity();
            min=(long)(queue_capacity * checkRange(min_threshold, 0, 1, "min_threshold"));
            max=(long)(queue_capacity * checkRange(max_threshold, 0, 1, "max_threshold"));
            span=max-min;
            weight=Math.pow(2, -weight_factor);
        }
    }

    public void resetStats() {
        super.resetStats();
        avg_queue_size=0;
        dropped_msgs.reset();
        total_msgs.reset();
    }

    public Object down(Message msg) {
        if(enabled) {
            int current_queue_size=bundler.getQueueSize();
            double avg;
            lock.lock();
            try {
                avg=avg_queue_size=computeAverage(avg_queue_size, current_queue_size);
            }
            finally {
                lock.unlock();
            }
            total_msgs.increment();
            // don't drop if avg <= min, drop if avg >= max, and drop with a probability p if avg is between min and max
            boolean drop=!(avg <= min) && (avg >= max || drop(avg));
            if(drop) {
                dropped_msgs.increment();
                return null;
            }
        }
        return down_prot.down(msg);
    }

    public void up(MessageBatch batch) {
        up_prot.up(batch);
    }

    public String toString() {
        return String.format("enabled=%b, queue capacity=%d, min=%d, max=%d, avg-queue-size=%.2f, " +
                               "total=%d dropped=%d (%d%%)", enabled, queue_capacity, min, max, avg_queue_size,
                             total_msgs.sum(), dropped_msgs.sum(), (int)(getDropRate()*100.0));
    }

    protected double computeAverage(double old_avg, int new_queue_size) {
        return old_avg * (1 - weight) + new_queue_size * weight;
    }

    /** Computes a probability P with which the message should get dropped. min_threshold < avg < max_threshold.
     * Probability increases linearly with min moving toward max */
    protected double computeDropProbability(double avg) {
        return Math.min(1, (avg-min) / span);
    }

    protected boolean drop(double avg) {
        // message will be dropped with probability p
        double p=computeDropProbability(avg);
        return Util.tossWeightedCoin(p); // returns true if message should be dropped, false otherwise
    }

    protected static double checkRange(double val, double min, double max, String name) {
        if(val < min || val > max)
            throw new IllegalArgumentException(String.format("%s (%.2f) needs to be in range [%.2f..%.2f]", name, val, min, max));
        return val;
    }

   /* public static void main(String[] args) {
        RED red=new RED();
        for(int i=0; i <= 1030; i++) {
            double p=red.computeDropProbability(i);
            System.out.printf("i=%d, drop-p=%.2f\n", i, p);
        }
    }*/
}

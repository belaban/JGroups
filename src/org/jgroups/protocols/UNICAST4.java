package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Buffer;
import org.jgroups.util.FixedBuffer;

import java.util.function.IntBinaryOperator;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * New unicast protocol based on fixed-size xmit windows and message ACKs<p>
 * Details: https://issues.redhat.com/browse/JGRP-2843
 * @author Bela Ban
 * @since  5.4
 */
public class UNICAST4 extends ReliableUnicast {
    @Property(description="Size of the send/receive buffers, in messages",writable=false)
    protected int capacity=2048;

    @Property(description="Number of ACKs to skip before one is sent. For example, a value of 500 means that only " +
      "every 500th ACk is sent; all others are dropped. If not set, defaulted to capacity/4",type=SCALAR)
    protected int ack_threshold;

    protected final IntBinaryOperator add_acks=(current_acks_sent, acks_to_be_sent) -> {
        if(current_acks_sent+acks_to_be_sent >= ack_threshold)
            return 0;
        else
            return current_acks_sent + acks_to_be_sent;
    };

    @ManagedAttribute(description="Number of times sender threads were blocked on a full send window",type=SCALAR)
    public long getNumBlockings() {
        long total=0;
        for(Entry e: send_table.values()) {
            FixedBuffer<Message> buf=(FixedBuffer<Message>)e.buf();
            total+=buf.numBlockings();
        }
        return total;
    }

    @ManagedAttribute(description="Average time blocked")
    public AverageMinMax getAvgTimeBlocked() {
        AverageMinMax first=null;
        for(Entry e: send_table.values()) {
            FixedBuffer<Message> buf=(FixedBuffer<Message>)e.buf();
            AverageMinMax avg=buf.avgTimeBlocked();
            if(first == null)
                first=avg;
            else
                first.merge(avg);
        }
        return first;
    }

    /**
     * Changes the capacity of all buffers, basically by creating new buffers and copying the messages from the
     * old ones.
     * This method is only supposed to be used by perf testing, so DON'T USE!
     */
    @ManagedOperation
    public void changeCapacity(int new_capacity) {
        if(new_capacity == this.capacity)
            return;
        send_table.values().stream().map(Entry::buf)
          .forEach(buf -> ((FixedBuffer<Message>)buf).changeCapacity(new_capacity));
        recv_table.values().stream().map(Entry::buf)
          .forEach(buf -> ((FixedBuffer<Message>)buf).changeCapacity(new_capacity));
        this.capacity=new_capacity;
        this.ack_threshold=capacity / 4;
    }

    @Override
    protected Buffer<Message> createBuffer(long seqno) {
        return new FixedBuffer<>(capacity, seqno);
    }

    public int                capacity()          {return capacity;}
    public UNICAST4           capacity(int c)     {capacity=c; return this;}
    public int                ackThreshold()      {return ack_threshold;}
    public UNICAST4           ackThreshold(int t) {ack_threshold=t; return this;}

    @Override
    public void init() throws Exception {
        super.init();
        if(ack_threshold <= 0) {
            ack_threshold=capacity / 4;
            log.debug("defaulted ack_threshold to %d", ack_threshold);
        }
    }

    @Override
    protected boolean needToSendAck(Entry e, int num_acks) {
        return e.update(num_acks, add_acks);
    }

}

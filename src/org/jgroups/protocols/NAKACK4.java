package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.AckTable;
import org.jgroups.util.AverageMinMax;
import org.jgroups.util.Buffer;
import org.jgroups.util.Buffer.Options;
import org.jgroups.util.FixedBuffer;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * New multicast protocols based on fixed-size xmit windows and message ACKs<rb/>
 * Details: https://issues.redhat.com/browse/JGRP-2780
 * @author Bela Ban
 * @since  5.4
 */
public class NAKACK4 extends ReliableMulticast {
    protected final AckTable       ack_table=new AckTable();
    protected static final Options SEND_OPTIONS=new Options().block(true);

    @Property(description="Size of the send/receive buffers, in messages",writable=false)
    protected int                  capacity=1024;

    @ManagedAttribute(description="Number of ACKs received",type=SCALAR)
    protected final LongAdder      acks_received=new LongAdder();

    @ManagedAttribute(description="Number of times sender threads were blocked on a full send window",type=SCALAR)
    public long getNumBlockings() {
        FixedBuffer<Message> buf=(FixedBuffer<Message>)sendBuf();
        return buf != null? buf.numBlockings() : -1;
    }

    @ManagedAttribute(description="Average time blocked")
    public AverageMinMax getAvgTimeBlocked() {
        FixedBuffer<Message> buf=(FixedBuffer<Message>)sendBuf();
        return buf != null? buf.avgTimeBlocked() : null;
    }

    @Override
    protected Buffer<Message> createXmitWindow(long initial_seqno) {
        return new FixedBuffer<>(capacity, initial_seqno);
    }

    @Override
    protected Options sendOptions() {return SEND_OPTIONS;}
    protected int     capacity()    {return capacity;}

    @ManagedAttribute(description="The minimum of all ACKs",type=SCALAR)
    public long       acksMin() {return ack_table.min();}

    @Override
    public void resetStats() {
        super.resetStats();
        acks_received.reset();
        Buffer<Message> buf=sendBuf();
        if(buf != null)
            buf.resetStats();
    }

    @Override
    public void destroy() {
        super.destroy();
        ack_table.clear();
    }

    @ManagedOperation(description="Prints the ACKs received from members")
    public String printAckTable() {return ack_table.toString();}

    @Override
    protected void adjustReceivers(List<Address> members) {
        super.adjustReceivers(members);
        ack_table.adjust(members);
    }

    @Override
    protected void handleAck(Address sender, long ack) {
        Buffer<Message> buf=sendBuf();
        if(buf == null) {
            log.warn("%s: local send buffer is null", local_addr);
            return;
        }
        acks_received.increment();
        long old_min=ack_table.min(), new_min=ack_table.ack(sender, ack);
        if(new_min > old_min)
            buf.purge(new_min); // unblocks senders waiting for space to become available
    }
}

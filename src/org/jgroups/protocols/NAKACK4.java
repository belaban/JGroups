package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Preview;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntBinaryOperator;
import java.util.function.Predicate;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * New multicast protocol based on fixed-size xmit windows and message ACKs<br/>
 * Details: https://issues.redhat.com/browse/JGRP-2780
 * @author Bela Ban
 * @since  5.4
 */
@Preview
public class NAKACK4 extends ReliableMulticast {
    protected final AckTable          ack_table=new AckTable();

    @Property(description="Size of the send/receive buffers, in messages",writable=false)
    protected int                     capacity=16384;

    @Property(description="Number of ACKs to skip before one is sent. For example, a value of 500 means that only " +
      "every 500th ACk is sent; all others are dropped. If not set, defaulted to capacity/4",type=SCALAR)
    protected int                     ack_threshold;

    @ManagedAttribute(description="Number of ACKs received",type=SCALAR)
    protected final LongAdder         acks_received=new LongAdder();

    protected final IntBinaryOperator add_acks=(current_acks_sent, acks_to_be_sent) -> {
        if(current_acks_sent+acks_to_be_sent >= ack_threshold)
            return 0;
        else
            return current_acks_sent + acks_to_be_sent;
    };

    public int               capacity()                {return capacity;}
    public NAKACK4           capacity(int c)           {capacity=c; return this;}
    public int               ackThreshold()            {return ack_threshold;}
    public NAKACK4           ackThreshold(int t)       {ack_threshold=t; return this;}

    @ManagedAttribute(type = SCALAR)
    public long getNumUnackedMessages() {
        long minAck=ack_table.min();
        return minAck > 0 ? seqno.get() - minAck : 0;
    }

    public long getNumUnackedMessages(Address dest) {
        long minAck=ack_table.min(dest);
        return minAck > 0 ? seqno.get() - minAck : 0;
    }

    @ManagedAttribute(description="Number of times sender threads were blocked on a full send window",type=SCALAR)
    public long getNumBlockings() {
        long retval=0;
        for(Entry e: xmit_table.values()) {
            if(e.buf() instanceof FixedBuffer)
                retval+=((FixedBuffer<?>)e.buf()).numBlockings();
        }
        return retval;
    }

    @ManagedAttribute(description="The number of received messages dropped due to full capacity of the buffer")
    public long getNumDroppedMessages() {
        long retval=0;
        for(Entry e: xmit_table.values()) {
            if(e.buf() instanceof FixedBuffer)
                retval+=((FixedBuffer<?>)e.buf()).numDroppedMessages();
        }
        return retval;
    }

    @ManagedAttribute(description="Average time blocked")
    public AverageMinMax getAvgTimeBlocked() {
        AverageMinMax avg=new AverageMinMax(1024).unit(TimeUnit.NANOSECONDS);
        for(Entry e: xmit_table.values()) {
            Buffer<Message> buf=e.buf();
            if(buf instanceof FixedBuffer) {
                AverageMinMax tmp=((FixedBuffer<Message>)buf).avgTimeBlocked();
                avg.merge(tmp);
            }
        }
        return avg;
    }

    @Override
    protected Buffer<Message> createXmitWindow(long initial_seqno) {
        return new FixedBuffer<>(capacity, initial_seqno);
    }

    @Override
    public boolean sendBufferCanBlock() {
        return true;
    }

    @Override
    public void resetStats() {
        super.resetStats();
        acks_received.reset();
        for(Entry e: xmit_table.values()) {
            Buffer<Message> buf=e.buf();
            buf.resetStats();
        }
    }

    @Override
    public void init() throws Exception {
        super.init();
        if(ack_threshold <= 0) {
            ack_threshold=capacity / 4;
            log.debug("defaulted ack_threshold to %d", ack_threshold);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        ack_table.clear();
    }

    @ManagedOperation(description="Prints the ACKs received from members")
    public String printAckTable() {return "\n" + ack_table;}

    @ManagedOperation(description="Sends ACKs immediately for all receive buffers")
    public void sendAcks() {
        sendAcks(true);
    }

    @ManagedOperation(description="Sends ACKs immediately for all receive buffers")
    public void sendPendingAcks() {
        sendAcks(false);
    }

    protected void sendAcks(boolean always_send) {
        for(Map.Entry<Address,Entry> entry: xmit_table.entrySet()) {
            Address         target=entry.getKey(); // target to send retransmit requests to
            Entry           val=entry.getValue();
            Buffer<Message> win=val != null? val.buf() : null;
            if(win != null && (always_send || needToSendAck(val))) // needToSendAck() resets send_ack to false
                sendAck(target, win);
        }
    }

    /**
     * Changes the capacity of the buffer, basically by creating a new buffer and copying the messages from the old one.
     * This method is only supposed to be used by perf testing, so DON'T USE!
     */
    @ManagedOperation
    public void changeCapacity(int new_capacity) {
        if(new_capacity == this.capacity)
            return;
        xmit_table.values().stream().map(Entry::buf)
          .forEach(buf -> ((FixedBuffer<Message>)buf).changeCapacity(new_capacity));
        this.capacity=new_capacity;
    }

    @Override
    protected void adjustReceivers(List<Address> members) {
        super.adjustReceivers(members);
        long old_min=ack_table.min();
        ack_table.adjust(members);
        long new_min=ack_table.min();
        if(new_min > old_min) {
            Buffer<Message> buf=sendBuf();
            if(buf == null)
                log.warn("%s: local send buffer is null", local_addr);
            else
                buf.purge(new_min); // unblocks senders waiting for space to become available
        }
    }

    @Override
    protected void reset() {
        FixedBuffer<Message> buf=(FixedBuffer<Message>)sendBuf();
        Util.close(buf);
        super.reset();
    }

    @Override
    protected void stable(Digest digest) {
        log.warn("%s: ignoring stable event %s", local_addr, digest);
    }

    @Override
    protected void handleAck(Address sender, long ack) {
        Buffer<Message> buf=sendBuf();
        if(buf == null) {
            log.warn("%s: local send buffer is null", local_addr);
            return;
        }
        if(is_trace)
            log.trace("%s <-- %s: ACK(%d)", local_addr, sender, ack);
        acks_received.increment();
        // don't create an entry if missing: https://issues.redhat.com/browse/JGRP-2904
        long[] rc=ack_table.ack(sender, ack, false);
        if(rc == null) {
            log.warn("%s: received ACK(%,d) from %s, but ack-table doesn't have an entry", local_addr, ack, sender);
            return;
        }
        long old_min=rc[0], new_min=rc[1];
        if(new_min > old_min)
            buf.purge(new_min); // unblocks senders waiting for space to become available
    }

    @Override
    protected boolean needToSendAck(Entry e) {
        return e.needToSendAck();
    }

    @Override
    protected boolean needToSendAck(Entry e, int num_acks) {
        return e.update(num_acks, add_acks);
    }

    @Override
    protected void sendAck(Address to, Buffer<Message> win) {
        long hd=win.highestDelivered();
        if(is_trace)
            log.trace("%s --> %s: ACK(%d)", local_addr, to, hd);
        down_prot.down(new EmptyMessage(to).putHeader(id, NakAckHeader.createAckHeader(hd)).setFlag(OOB));
    }

    @Override
    protected boolean addToSendBuffer(Buffer<Message> win, long seq, Message msg, Predicate<Message> filter) {
        return win.add(seq, msg, filter, true);
    }
}

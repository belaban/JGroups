package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.EmptyMessage;
import org.jgroups.Message;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.*;
import org.jgroups.util.Buffer.Options;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.function.IntBinaryOperator;

import static org.jgroups.Message.Flag.OOB;
import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
import static org.jgroups.conf.AttributeType.SCALAR;

/**
 * New multicast protocols based on fixed-size xmit windows and message ACKs<rb/>
 * Details: https://issues.redhat.com/browse/JGRP-2780
 * @author Bela Ban
 * @since  5.4
 */
public class NAKACK4 extends ReliableMulticast {
    protected final AckTable          ack_table=new AckTable();
    protected static final Options    SEND_OPTIONS=new Options().block(true);

    @Property(description="Size of the send/receive buffers, in messages",writable=false)
    protected int                     capacity=2048;

    @Property(description="Increment seqno and send a message atomically. Reduces retransmissions. " +
      "Description in doc/design/NAKACK4.txt ('misc')")
    protected boolean                 send_atomically;

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
    public boolean           sendAtomically()          {return send_atomically;}
    public NAKACK4           sendAtomically(boolean f) {send_atomically=f; return this;}
    public int               ackThreshold()            {return ack_threshold;}
    public NAKACK4           ackThreshold(int t)       {ack_threshold=t; return this;}
    @Override public Options sendOptions()             {return SEND_OPTIONS;}

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
    public void resetStats() {
        super.resetStats();
        acks_received.reset();
        Buffer<Message> buf=sendBuf();
        if(buf != null)
            buf.resetStats();
    }

    @Override
    public void init() throws Exception {
        super.init();
        if(ack_threshold <= 0) {
            ack_threshold=capacity / 4;
            log.info("defaulted ack_threshold to %d", ack_threshold);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        ack_table.clear();
    }

    @ManagedOperation(description="Prints the ACKs received from members")
    public String printAckTable() {return ack_table.toString();}

    @ManagedOperation(description="Sends ACKs immediately for entries which are marked as pending")
    public void sendPendingAcks() {
        for(Map.Entry<Address,Entry> entry: xmit_table.entrySet()) {
            Address         target=entry.getKey(); // target to send retransmit requests to
            Entry           val=entry.getValue();
            Buffer<Message> win=val != null? val.buf() : null;
            if(win != null && needToSendAck(val)) // needToSendAck() resets send_ack to false
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
        ack_table.adjust(members);
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
        long[] rc=ack_table.ack(sender, ack);
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
    protected void send(Message msg, Buffer<Message> win) {
        if(!send_atomically) {
            super.send(msg, win);
            return;
        }
        boolean dont_loopback_set=msg.isFlagSet(DONT_LOOPBACK);
        long msg_id=0;

        // As described in doc/design/NAKACK4 ("misc"): if we hold the lock while (1) getting the seqno for a message,
        // (2) adding it to the send window and (3) sending it (so it is sent by the transport in that order),
        // messages should be received in order and therefore not require retransmissions.
        // Passing the message down should not block with TransferQueueBundler (default), as drop_when_fule==true
        Lock lock=win.lock();
        lock.lock();
        try {
            msg_id=seqno.incrementAndGet();
            msg.putHeader(this.id, NakAckHeader.createMessageHeader(msg_id));
            win.add(msg_id, msg, dont_loopback_set? remove_filter : null, sendOptions());
            if(dont_loopback_set)
                win.purge(win.highestDelivered());
            down_prot.down(msg); // if this fails, since msg is in sent_msgs, it can be retransmitted
        }
        finally {
            lock.unlock();
        }
        if(is_trace)
            log.trace("%s --> [all]: #%d", local_addr, msg_id);
    }
}

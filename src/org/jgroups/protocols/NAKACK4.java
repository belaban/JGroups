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
import java.util.concurrent.locks.Lock;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;
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
    protected int                  capacity=8192;

    @Property(description="Increment seqno and send a message atomically. Reduces retransmissions. " +
      "Description in doc/design/NAKACK4.txt ('misc')")
    protected boolean              send_atomically=true;

    @ManagedAttribute(description="Number of ACKs received",type=SCALAR)
    protected final LongAdder      acks_received=new LongAdder();

    protected int               capacity()                {return capacity;}
    protected boolean           sendAtomically()          {return send_atomically;}
    protected NAKACK4           sendAtomically(boolean f) {send_atomically=f; return this;}
    @Override protected Options sendOptions()             {return SEND_OPTIONS;}

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
            win.add(msg_id, msg, dont_loopback_set? dont_loopback_filter : null, sendOptions());
            down_prot.down(msg); // if this fails, since msg is in sent_msgs, it can be retransmitted
        }
        finally {
            lock.unlock();
        }
        if(is_trace)
            log.trace("%s --> [all]: #%d", local_addr, msg_id);
    }
}

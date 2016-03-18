
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of total order protocol using a sequencer.
 * Consult <a href="https://github.com/belaban/JGroups/blob/master/doc/design/SEQUENCER.txt">SEQUENCER.txt</a> for details
 * @author Bela Ban
 */
@MBean(description="Implementation of total order protocol using a sequencer")
public class SEQUENCER extends Protocol {
    protected Address                           local_addr;
    protected volatile Address                  coord;
    protected volatile View                     view;
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);


    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final NavigableMap<Long,Message>  forward_table=new ConcurrentSkipListMap<>();

    protected final Lock                        send_lock=new ReentrantLock();

    protected final Condition                   send_cond=send_lock.newCondition();

    /** When ack_mode is set, we need to wait for an ack for each forwarded message until we can send the next one */
    protected volatile boolean                  ack_mode=true;

    /** Set when we block all sending threads to resend all messages from forward_table */
    protected volatile boolean                  flushing=false;

    protected volatile boolean                  running=true;

    /** Keeps track of the threads sending messages */
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    // Maintains received seqnos, so we can weed out dupes
    protected final ConcurrentMap<Address,BoundedHashMap<Long,Long>> delivery_table=Util.createConcurrentMap();

    protected volatile Flusher                  flusher;

    /** Used for each resent message to wait until the message has been received */
    protected final Promise<Long>               ack_promise=new Promise<>();



    @Property(description="Size of the set to store received seqnos (for duplicate checking)")
    protected int  delivery_table_max_size=2000;

    @Property(description="Number of acks needed before going from ack-mode to normal mode. " +
      "0 disables this, which means that ack-mode is always on")
    protected int  threshold=10;

    protected int  num_acks=0;

    protected long forwarded_msgs=0;
    protected long bcast_msgs=0;
    protected long received_forwards=0;
    protected long received_bcasts=0;
    protected long delivered_bcasts=0;

    @ManagedAttribute
    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    @ManagedAttribute
    public long getForwarded() {return forwarded_msgs;}
    @ManagedAttribute
    public long getBroadcast() {return bcast_msgs;}
    @ManagedAttribute
    public long getReceivedForwards() {return received_forwards;}
    @ManagedAttribute
    public long getReceivedBroadcasts() {return received_bcasts;}

    @ManagedAttribute(description="Number of messages in the forward-table")
    public int getForwardTableSize() {return forward_table.size();}

    public void setThreshold(int new_threshold) {this.threshold=new_threshold;}

    public void setDeliveryTableMaxSize(int size) {delivery_table_max_size=size;}

    @ManagedOperation
    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=delivered_bcasts=0L;
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("forwarded",forwarded_msgs);
        m.put("broadcast",bcast_msgs);
        m.put("received_forwards", received_forwards);
        m.put("received_bcasts",   received_bcasts);
        m.put("delivered_bcasts",  delivered_bcasts);
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    public void start() throws Exception {
        super.start();
        running=true;
        ack_mode=true;
    }

    public void stop() {
        running=false;
        unblockAll();
        stopFlusher();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                if(flushing)
                    block();

                // A seqno is not used to establish ordering, but only to weed out duplicates; next_seqno doesn't need
                // to increase monotonically, but only to be unique (https://issues.jboss.org/browse/JGRP-1461) !
                long next_seqno=seqno.incrementAndGet();
                in_flight_sends.incrementAndGet();
                try {
                    SequencerHeader hdr=new SequencerHeader(is_coord? SequencerHeader.BCAST : SequencerHeader.WRAPPED_BCAST, next_seqno);
                    msg.putHeader(this.id, hdr);
                    if(log.isTraceEnabled())
                        log.trace("[" + local_addr + "]: forwarding " + local_addr + "::" + seqno + " to coord " + coord);

                    // We always forward messages to the coordinator, even if we're the coordinator. Having the coord
                    // send its messages directly led to starvation of messages from other members. MPerf perf went up
                    // from 20MB/sec/node to 50MB/sec/node with this change !
                    forwardToCoord(next_seqno, msg);
                }
                catch(Exception ex) {
                    log.error(Util.getMessage("FailedSendingMessage"), ex);
                }
                finally {
                    in_flight_sends.decrementAndGet();
                }
                return null; // don't pass down

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }




    public Object up(Event evt) {
        Message msg;
        SequencerHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB))
                    break;
                hdr=(SequencerHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up

                switch(hdr.type) {
                    case SequencerHeader.FORWARD:
                    case SequencerHeader.FLUSH:
                        if(!is_coord) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": non-coord; dropping FORWARD request from " + msg.getSrc());
                            return null;
                        }
                        Address sender=msg.getSrc();
                        if(view != null && !view.containsMember(sender)) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": dropping FORWARD request from non-member " + sender +
                                            "; view=" + view);
                            return null;
                        }

                        broadcast(msg, true, msg.getSrc(), hdr.seqno, hdr.type == SequencerHeader.FLUSH); // do copy the message
                        received_forwards++;
                        break;

                    case SequencerHeader.BCAST:
                        deliver(msg, evt, hdr);
                        received_bcasts++;
                        break;

                    case SequencerHeader.WRAPPED_BCAST:
                        unwrapAndDeliver(msg, hdr.flush_ack);  // unwrap the original message (in the payload) and deliver it
                        received_bcasts++;
                        break;
                }
                return null;

            case Event.VIEW_CHANGE:
                Object retval=up_prot.up(evt);
                handleViewChange((View)evt.getArg());
                return retval;

            case Event.TMP_VIEW:
                handleTmpView((View)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            // simplistic implementation
            try {
                up(new Event(Event.MSG, msg));
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedPassingUpMessage"), t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /* --------------------------------- Private Methods ----------------------------------- */

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;

        delivery_table.keySet().retainAll(mbrs);

        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(coord_changed && new_coord != null) {
            stopFlusher();
            startFlusher(new_coord); // needs to be done in the background, to prevent blocking if down() would block
        }
    }

    protected void flush(final Address new_coord) throws InterruptedException {
        // wait until all threads currently sending messages have returned (new threads after flushing=true) will block
        // flushing is set to true in startFlusher()
        while(flushing && running) {
            if(in_flight_sends.get() == 0)
                break;
            Thread.sleep(100);
        }

        send_lock.lockInterruptibly();
        try {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": coord changed from " + coord + " to " + new_coord);
            coord=new_coord;
            is_coord=local_addr != null && local_addr.equals(coord);
            flushMessagesInForwardTable();
        }
        finally {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing completed");
            flushing=false;
            ack_mode=true; // go to ack-mode after flushing
            num_acks=0;
            send_cond.signalAll();
            send_lock.unlock();
        }
    }


    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.get(0);
        if(!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord))
            handleViewChange(v);
    }


    /**
     * Sends all messages currently in forward_table to the new coordinator (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.<br/>
     * Access to the forward_table doesn't need to be synchronized as there won't be any insertions during flushing
     * (all down-threads are blocked)
     */
    protected void flushMessagesInForwardTable() {
        if(is_coord) {
            for(Map.Entry<Long,Message> entry: forward_table.entrySet()) {
                Long key=entry.getKey();
                Message msg=entry.getValue();
                Buffer buf;
                try {
                    buf=Util.streamableToBuffer(msg);
                }
                catch(Exception e) {
                    log.error(Util.getMessage("FlushingBroadcastingFailed"), e);
                    continue;
                }

                SequencerHeader hdr=new SequencerHeader(SequencerHeader.WRAPPED_BCAST, key);
                Message forward_msg=new Message(null).setBuffer(buf).putHeader(this.id, hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (broadcasting) " + local_addr + "::" + key);
                down_prot.down(new Event(Event.MSG, forward_msg));
            }
            return;
        }

        // for forwarded messages, we need to receive the forwarded message from the coordinator, to prevent this case:
        // - V1={A,B,C}
        // - A crashes
        // - C installs V2={B,C}
        // - C forwards messages 3 and 4 to B (the new coord)
        // - B drops 3 because its view is still V1
        // - B installs V2
        // - B receives message 4 and broadcasts it
        // ==> C's message 4 is delivered *before* message 3 !
        // ==> By resending 3 until it is received, then resending 4 until it is received, we make sure this won't happen
        // (see https://issues.jboss.org/browse/JGRP-1449)
        while(flushing && running && !forward_table.isEmpty()) {
            Map.Entry<Long,Message> entry=forward_table.firstEntry();
            final Long key=entry.getKey();
            Message    msg=entry.getValue();
            Buffer     buf;

            try {
                buf=Util.streamableToBuffer(msg);
            }
            catch(Exception e) {
                log.error(Util.getMessage("FlushingBroadcastingFailed"), e);
                continue;
            }

            while(flushing && running && !forward_table.isEmpty()) {
                SequencerHeader hdr=new SequencerHeader(SequencerHeader.FLUSH, key);
                Message forward_msg=new Message(coord).setBuffer(buf).putHeader(this.id,hdr).setFlag(Message.Flag.DONT_BUNDLE);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": flushing (forwarding) " + local_addr + "::" + key + " to coord " + coord);
                ack_promise.reset();
                down_prot.down(new Event(Event.MSG, forward_msg));
                Long ack=ack_promise.getResult(500);
                if((ack != null && ack.equals(key)) || !forward_table.containsKey(key))
                    break;
            }
        }
    }


    protected void forwardToCoord(long seqno, Message msg) {
        if(is_coord) {
            forward(msg, seqno, false);
            return;
        }

        if(!running || flushing) {
            forward_table.put(seqno, msg);
            return;
        }

        if(!ack_mode) {
            forward_table.put(seqno, msg);
            forward(msg, seqno, false);
            return;
        }

        send_lock.lock();
        try {
            forward_table.put(seqno, msg);
            while(running && !flushing) {
                ack_promise.reset();
                forward(msg, seqno, true);
                if(!ack_mode || !running || flushing)
                    break;
                Long ack=ack_promise.getResult(500);
                if((ack != null && ack.equals(seqno)) || !forward_table.containsKey(seqno))
                    break;
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    protected void forward(final Message msg, long seqno, boolean flush) {
        Address target=coord;
        if(target == null)
            return;
        byte type=flush? SequencerHeader.FLUSH : SequencerHeader.FORWARD;
        try {
            SequencerHeader hdr=new SequencerHeader(type, seqno);
            Message forward_msg=new Message(target).setBuffer(Util.streamableToBuffer(msg)).putHeader(this.id,hdr);
            down_prot.down(new Event(Event.MSG, forward_msg));
            forwarded_msgs++;
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailedForwardingMessageTo") + msg.getDest(), ex);
        }
    }

    protected void broadcast(final Message msg, boolean copy, Address original_sender, long seqno, boolean resend) {
        Message bcast_msg=null;

        if(!copy) {
            bcast_msg=msg; // no need to add a header, message already has one
        }
        else {
            SequencerHeader new_hdr=new SequencerHeader(SequencerHeader.WRAPPED_BCAST, seqno);
            bcast_msg=new Message(null, msg.getRawBuffer(), msg.getOffset(), msg.getLength()).putHeader(this.id, new_hdr);
            if(resend) {
                new_hdr.flush_ack=true;
                bcast_msg.setFlag(Message.Flag.DONT_BUNDLE);
            }
        }

        if(log.isTraceEnabled())
            log.trace(local_addr + ": broadcasting " + original_sender + "::" + seqno);

        down_prot.down(new Event(Event.MSG,bcast_msg));
        bcast_msgs++;
    }



    /**
     * Unmarshal the original message (in the payload) and then pass it up (unless already delivered)
     * @param msg
     */
    protected void unwrapAndDeliver(final Message msg, boolean flush_ack) {
        try {
            Message msg_to_deliver=Util.streamableFromBuffer(Message.class, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            SequencerHeader hdr=(SequencerHeader)msg_to_deliver.getHeader(this.id);
            if(flush_ack)
                hdr.flush_ack=true;
            deliver(msg_to_deliver, new Event(Event.MSG, msg_to_deliver), hdr);
        }
        catch(Exception ex) {
            log.error(Util.getMessage("FailureUnmarshallingBuffer"), ex);
        }
    }


    protected void deliver(Message msg, Event evt, SequencerHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + "::" + hdr.getSeqno());
            return;
        }
        long msg_seqno=hdr.getSeqno();
        if(sender.equals(local_addr)) {
            forward_table.remove(msg_seqno);
            if(hdr.flush_ack) {
                ack_promise.setResult(msg_seqno);
                if(ack_mode && !flushing && threshold > 0 && ++num_acks >= threshold) {
                    ack_mode=false;
                    num_acks=0;
                }
            }
        }
        if(!canDeliver(sender, msg_seqno)) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": dropped duplicate message " + sender + "::" + msg_seqno);
            return;
        }
        if(log.isTraceEnabled())
            log.trace(local_addr + ": delivering " + sender + "::" + msg_seqno);
        up_prot.up(evt);
        delivered_bcasts++;
    }


    /**
     * Checks if seqno has already been received from sender. This weeds out duplicates.
     * Note that this method is never called concurrently for the same sender, as the sender in NAKACK will always be
     * the coordinator.
     */
    protected boolean canDeliver(Address sender, long seqno) {
        BoundedHashMap<Long,Long> seqno_set=delivery_table.get(sender);
        if(seqno_set == null) {
            seqno_set=new BoundedHashMap<>(delivery_table_max_size);
            BoundedHashMap<Long,Long> existing=delivery_table.put(sender,seqno_set);
            if(existing != null)
                seqno_set=existing;
        }
        return seqno_set.add(seqno, seqno);
    }

    protected void block() {
        send_lock.lock();
        try {
            while(flushing && running) {
                try {
                    send_cond.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            send_lock.unlock();
        }
    }

    protected void unblockAll() {
        flushing=false;
        send_lock.lock();
        try {
            send_cond.signalAll();
            ack_promise.setResult(null);
        }
        finally {
            send_lock.unlock();
        }
    }

    protected synchronized void startFlusher(final Address new_coord) {
        if(flusher == null || !flusher.isAlive()) {
            if(log.isTraceEnabled())
                log.trace(local_addr + ": flushing started");
            // causes subsequent message sends (broadcasts and forwards) to block (https://issues.jboss.org/browse/JGRP-1495)
            flushing=true;
            
            flusher=new Flusher(new_coord);
            flusher.setName("Flusher");
            flusher.start();
        }
    }

    protected void stopFlusher() {
        flushing=false;
        Thread tmp=flusher;

        while(tmp != null && tmp.isAlive()) {
            tmp.interrupt();
            ack_promise.setResult(null);
            try {
                tmp.join();
            }
            catch(InterruptedException e) {
            }
        }
    }

/* ----------------------------- End of Private Methods -------------------------------- */

    protected class Flusher extends Thread {
        protected final Address new_coord;

        public Flusher(Address new_coord) {
            this.new_coord=new_coord;
        }

        public void run() {
            try {
                flush(new_coord);
            }
            catch (InterruptedException e) {
            }
        }
    }




    public static class SequencerHeader extends Header {
        protected static final byte FORWARD       = 1;
        protected static final byte FLUSH         = 2;
        protected static final byte BCAST         = 3;
        protected static final byte WRAPPED_BCAST = 4;

        protected byte    type=-1;
        protected long    seqno=-1;
        protected boolean flush_ack;

        public SequencerHeader() {
        }

        public SequencerHeader(byte type) {
            this.type=type;
        }

        public SequencerHeader(byte type, long seqno) {
            this(type);
            this.seqno=seqno;
        }

        public long getSeqno() {
            return seqno;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(seqno >= 0)
                sb.append(" seqno=" + seqno);
            if(flush_ack)
                sb.append(" (flush_ack)");
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case FORWARD:        return "FORWARD";
                case FLUSH:          return "FLUSH";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                default:             return "n/a";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(seqno,out);
            out.writeBoolean(flush_ack);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Bits.readLong(in);
            flush_ack=in.readBoolean();
        }

        public int size() {
            return Global.BYTE_SIZE + Bits.size(seqno) + Global.BYTE_SIZE; // type + seqno + flush_ack
        }

    }

}
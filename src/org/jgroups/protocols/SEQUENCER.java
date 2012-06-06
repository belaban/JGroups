
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
    protected volatile boolean                  is_coord=false;
    protected final AtomicLong                  seqno=new AtomicLong(0);

    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final NavigableMap<Long,byte[]>   forward_table=new ConcurrentSkipListMap<Long,byte[]>();

    protected final Lock                        resend_lock=new ReentrantLock();

    protected final Condition                   resend_cond=resend_lock.newCondition();

    protected volatile boolean                  resending=false;

    // keeps track of broadcasts or forwards that are currently in progress
    protected final AtomicInteger               in_flight_sends=new AtomicInteger(0);

    // Maintains receives seqnos, so we can weed out dupes
    protected final ConcurrentMap<Address,NavigableSet<Long>> delivery_table=Util.createConcurrentMap();

    protected volatile Resender                 resender;


    @Property(description="Size of the set to store received seqnos (for duplicate checking)")
    protected int  delivery_table_max_size=2000;

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

    @ManagedOperation
    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=delivered_bcasts=0L;
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();
        m.put("forwarded",         forwarded_msgs);
        m.put("broadcast",         bcast_msgs);
        m.put("received_forwards", received_forwards);
        m.put("received_bcasts",   received_bcasts);
        m.put("delivered_bcasts",  delivered_bcasts);
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    public void stop() {
        unblockAll();
        stopResender();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.NO_TOTAL_ORDER) || msg.isFlagSet(Message.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                if(resending)
                    block();

                // A seqno is not used to establish ordering, but only to weed out duplicates; next_seqno doesn't need
                // to increase monotonically, but only to be unique (https://issues.jboss.org/browse/JGRP-1461) !
                long next_seqno=seqno.incrementAndGet();
                in_flight_sends.incrementAndGet();
                try {
                    SequencerHeader hdr=new SequencerHeader(is_coord? SequencerHeader.BCAST : SequencerHeader.WRAPPED_BCAST, next_seqno);
                    msg.putHeader(this.id, hdr);
                    if(is_coord)
                        broadcast(msg, false, msg.getSrc(), next_seqno); // don't copy, just use the message passed as argument
                    else {
                        byte[] marshalled_msg=Util.objectToByteBuffer(msg);
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": forwarding " + local_addr + "::" + seqno + " to coord " + coord);
                        forwardToCoord(marshalled_msg, next_seqno);
                    }
                }
                catch(Exception ex) {
                    log.error("failed marshalling message", ex);
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
                if(msg.isFlagSet(Message.NO_TOTAL_ORDER) || msg.isFlagSet(Message.OOB))
                    break;
                hdr=(SequencerHeader)msg.getHeader(this.id);
                if(hdr == null)
                    break; // pass up

                switch(hdr.type) {
                    case SequencerHeader.FORWARD:
                        if(!is_coord) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": non-coord; dropping FORWARD request from " + msg.getSrc());
                            return null;
                        }
                        broadcast(msg, true, msg.getSrc(), hdr.seqno); // do copy the message
                        received_forwards++;
                        return null;

                    case SequencerHeader.BCAST:
                        deliver(msg, evt, hdr);
                        received_bcasts++;
                        return null;

                    case SequencerHeader.WRAPPED_BCAST:
                        unwrapAndDeliver(msg);  // unwrap the original message (in the payload) and deliver it
                        received_bcasts++;
                        return null;
                }
                break;

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



    /* --------------------------------- Private Methods ----------------------------------- */

    protected void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        delivery_table.keySet().retainAll(mbrs);

        Address existing_coord=coord, new_coord=mbrs.get(0);
        boolean coord_changed=existing_coord == null || !existing_coord.equals(new_coord);
        if(!coord_changed)
            return;

        stopResender();
        startResender(new_coord);
    }

    protected void resend(final Address new_coord) {
        resend_lock.lock();
        try {
            resending=true;  // causes subsequent message sends (broadcasts and forwards) to block

            // wait for all threads to complete in-flight message sending
            while(resending) {
                if(in_flight_sends.get() == 0)
                    break;
                Util.sleep(100);
            }

            setCoord(new_coord);

            // needs to be done in the background, to not block if down() would block
            resendMessagesInForwardTable(); // maybe optimize in the future: broadcast directly if coord
        }
        finally {
            resending=false;
            resend_cond.signalAll();
            resend_lock.unlock();
        }
    }

    private void setCoord(Address new_coord) {
        coord=new_coord;
        is_coord=local_addr != null && local_addr.equals(coord);
    }


    // If we're becoming coordinator, we need to handle TMP_VIEW as
    // an immediate change of view. See JGRP-1452.
    private void handleTmpView(View v) {
        List<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;

        Address new_coord=mbrs.iterator().next();
        if (!new_coord.equals(coord) && local_addr != null && local_addr.equals(new_coord)) {
            handleViewChange(v);
        }
    }


    /**
     * Sends all messages currently in forward_table to the new coordinator (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.<br/>
     * Access to the forward_table doesn't need to be synchronized as there won't be any insertions during resending
     * (all down-threads are blocked)
     */
    protected void resendMessagesInForwardTable() {
        if(is_coord) {
            for(Map.Entry<Long,byte[]> entry: forward_table.entrySet()) {
                Long key=entry.getKey();
                byte[] val=entry.getValue();

                Message forward_msg=new Message(null, val);
                SequencerHeader hdr=new SequencerHeader(SequencerHeader.WRAPPED_BCAST, key);
                forward_msg.putHeader(this.id,hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": resending (broadcasting) " + local_addr + "::" + key);
                down_prot.down(new Event(Event.MSG, forward_msg));
            }
            return;
        }

        // for forwarded messages, we need to receive the forwarded message from the coordinator, to prvent this case:
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

        while(resending && !forward_table.isEmpty()) {
            Map.Entry<Long,byte[]> entry=forward_table.firstEntry();
            final Long key=entry.getKey();
            byte[] val=entry.getValue();

            boolean sent=false;
            while(!sent && resending && !forward_table.isEmpty()) {
                Message forward_msg=new Message(coord, val);
                SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, key);
                forward_msg.putHeader(this.id,hdr);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": resending (forwarding) " + local_addr + "::" + key + " to coord " + coord);
                down_prot.down(new Event(Event.MSG, forward_msg));

                long sleep_time=10;
                for(int i=0; i < 5; i++) {
                    if(!forward_table.containsKey(key)) {
                        sent=true;
                        break;
                    }
                    Util.sleep(sleep_time);
                    sleep_time=Math.min(sleep_time * 2, 1000);
                }
            }
        }

    }


    protected void forwardToCoord(final byte[] marshalled_msg, long seqno) {
        forward_table.put(seqno, marshalled_msg);
        Message forward_msg=new Message(coord, marshalled_msg);
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, seqno);
        forward_msg.putHeader(this.id,hdr);
        down_prot.down(new Event(Event.MSG, forward_msg));
        forwarded_msgs++;
    }
    

    protected void broadcast(final Message msg, boolean copy, Address original_sender, long seqno) {
        Message bcast_msg=null;

        if(!copy) {
            bcast_msg=msg; // no need to add a header, message already has one
        }
        else {
            bcast_msg=new Message(null, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            SequencerHeader new_hdr=new SequencerHeader(SequencerHeader.WRAPPED_BCAST, seqno);
            bcast_msg.putHeader(this.id, new_hdr);
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
    protected void unwrapAndDeliver(final Message msg) {
        try {
            Message msg_to_deliver=(Message)Util.objectFromByteBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            SequencerHeader hdr=(SequencerHeader)msg_to_deliver.getHeader(this.id);
            deliver(msg_to_deliver, new Event(Event.MSG, msg_to_deliver), hdr);
        }
        catch(Exception ex) {
            log.error("failure unmarshalling buffer", ex);
        }
    }


    protected void deliver(Message msg, Event evt, SequencerHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error(local_addr + ": sender is null, cannot deliver " + sender + "::" + hdr.getSeqno());
            return;
        }
        long msg_seqno=hdr.getSeqno();
        if(sender.equals(local_addr))
            forward_table.remove(msg_seqno);
        if(!canDeliver(sender, msg_seqno)) {
            if(log.isWarnEnabled())
                log.warn(local_addr + ": dropped message " + sender + "::" + msg_seqno);
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
        NavigableSet<Long> seqno_set=delivery_table.get(sender);
        if(seqno_set == null) {
            seqno_set=new ConcurrentSkipListSet<Long>();
            NavigableSet<Long> existing=delivery_table.put(sender,seqno_set);
            if(existing != null)
                seqno_set=existing;
        }

        boolean added=seqno_set.add(seqno);
        int size=seqno_set.size();
        if(size > delivery_table_max_size) {
            // trim the seqno_set to delivery_table_max_size elements by removing the first N seqnos
            for(int i=0; i < size - delivery_table_max_size; i++) {
                if(seqno_set.pollFirst() == null)
                    break;
            }
        }
        return added;
    }

    protected void block() {
        resend_lock.lock();
        try {
            while(resending) {
                try {
                    resend_cond.await();
                }
                catch(InterruptedException e) {
                }
            }
        }
        finally {
            resend_lock.unlock();
        }
    }

    protected void unblockAll() {
        resend_lock.lock();
        try {
            resending=false;
            resend_cond.signalAll();
        }
        finally {
            resend_lock.unlock();
        }
    }

    protected void startResender(final Address new_coord) {
        if(resender == null || !resender.isAlive()) {
            resender=new Resender(new_coord);
            resender.setName("Resender");
            resender.start();
        }
    }

    protected void stopResender() {
        Thread tmp=resender;
        if(tmp != null &&tmp.isAlive()) {
            unblockAll();
            tmp.interrupt();
        }
    }

/* ----------------------------- End of Private Methods -------------------------------- */

    protected class Resender extends Thread {
        protected volatile boolean    running=false;
        protected final Address       new_coord;

        public Resender(Address new_coord) {
            this.new_coord=new_coord;
        }

        public void run() {
            resend(new_coord);
        }
    }





    public static class SequencerHeader extends Header {
        protected static final byte FORWARD       = 1;
        protected static final byte BCAST         = 2;
        protected static final byte WRAPPED_BCAST = 3;

        protected byte    type=-1;
        protected long    seqno=-1;

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
            return sb.toString();
        }

        protected final String printType() {
            switch(type) {
                case FORWARD:        return "FORWARD";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                default:             return "n/a";
            }
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeLong(seqno, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            seqno=Util.readLong(in);
        }

        public int size() {
            return Global.BYTE_SIZE + Util.size(seqno); // type + seqno
        }

    }


}
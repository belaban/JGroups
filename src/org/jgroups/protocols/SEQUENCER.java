
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Implementation of total order protocol using a sequencer.
 * Consult <a href="https://github.com/belaban/JGroups/blob/master/doc/design/SEQUENCER.txt">SEQUENCER.txt</a> for details
 * @author Bela Ban
 */
@MBean(description="Implementation of total order protocol using a sequencer")
public class SEQUENCER extends Protocol {
    protected Address                           local_addr=null, coord=null;
    protected final Collection<Address>         members=new ArrayList<Address>();
    protected volatile boolean                  is_coord=false;
    protected long                              seqno=1;

    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     *  Needs to be sorted so we resend them in the right order
     */
    protected final Map<Long,byte[]>            forward_table=new TreeMap<Long,byte[]>();

    // Maintains the next seqno to be delivered, so we can weed out dupes
    protected final Map<Address,Long>           delivery_table=Util.createHashMap();

    protected final Lock                        seqno_lock=new ReentrantLock();


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

    @ManagedOperation(description="Prints the next seqnos to be received for all senders")
    public String printDeliveryTable() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,Long> entry: delivery_table.entrySet())
        sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        return sb.toString();
    }



    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                if(msg.getDest() != null || msg.isFlagSet(Message.NO_TOTAL_ORDER) || msg.isFlagSet(Message.OOB))
                    break;

                if(msg.getSrc() == null)
                    msg.setSrc(local_addr);

                // We have to wrap the sending / forwarding in a lock, or else messages can get reordered
                // before they reach UNICAST or NAKACK (https://issues.jboss.org/browse/JGRP-1461) !
                seqno_lock.lock();
                try {
                    long next_seqno=seqno;
                    SequencerHeader hdr=new SequencerHeader(is_coord? SequencerHeader.BCAST : SequencerHeader.WRAPPED_BCAST, next_seqno);
                    msg.putHeader(this.id, hdr);

                    if(is_coord) {
                        broadcast(msg, false, msg.getSrc(), next_seqno); // don't copy, just use the message passed as argument
                    }
                    else {
                        byte[] marshalled_msg=null;
                        try {
                            marshalled_msg=Util.objectToByteBuffer(msg);
                            if(log.isTraceEnabled())
                                log.trace(local_addr + ": forwarding " + local_addr + "::" + seqno + " to coord " + coord);
                            forwardToCoord(marshalled_msg, next_seqno);
                        }
                        catch(Exception ex) {
                            log.error("failed marshalling message", ex);
                            return null;
                        }
                    }
                    seqno++;
                }
                finally {
                    seqno_lock.unlock();
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
        boolean coord_changed=false;

        synchronized(this) {
            members.clear();
            members.addAll(mbrs);
            Address prev_coord=coord;
            coord=mbrs.iterator().next();
            is_coord=local_addr != null && local_addr.equals(coord);
            coord_changed=prev_coord != null && !prev_coord.equals(coord);
        }

        delivery_table.keySet().retainAll(mbrs);

        if(coord_changed) {
            resendMessagesInForwardTable(); // maybe optimize in the future: broadcast directly if coord
        }
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
     * from being inserted until we're done, that's why there's synchronization.
     */
    protected void resendMessagesInForwardTable() {
        Map<Long,byte[]> copy;
        synchronized(forward_table) {
            copy=new TreeMap<Long,byte[]>(forward_table);
        }
        for(Map.Entry<Long,byte[]> entry: copy.entrySet()) {
            Long key=entry.getKey();
            byte[] val=entry.getValue();

            Message forward_msg=new Message(coord, null, val);
            SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, key);
            forward_msg.putHeader(this.id, hdr);

            if(log.isTraceEnabled())
                log.trace(local_addr + ": resending " + local_addr + "::" + key + " to coord " + coord);
            down_prot.down(new Event(Event.MSG, forward_msg));
        }
    }


    protected void forwardToCoord(final byte[] marshalled_msg, long seqno) {
        synchronized(forward_table) {
            forward_table.put(seqno, marshalled_msg);
        }
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
                log.error(local_addr + ": sender is null, cannot deliver " + sender + "::" + hdr.getSeqno() + " (expected=" + getNextToDeliver(sender) + ")");
            return;
        }
        long msg_seqno=hdr.getSeqno();
        if(sender.equals(local_addr)) {
            synchronized(forward_table) {
                forward_table.remove(msg_seqno);
            }
        }
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
     * Checks if seqno is == the next expected seqno. If one doesn't exist yet, it will be created. This weeds out
     * duplicates. Note that we'll only get monotonically increasing seqnos, so if we get A5, A6, A7, we know that 5 is
     * the first seqno broadcast by A.<p/>
     * Note that this method is never called concurrently for the same sender, as the sender in NAKACK will always be
     * the coordinator.
     */
    protected boolean canDeliver(Address sender, long seqno) {
        Long next_to_deliver=delivery_table.get(sender);
        if(next_to_deliver == null) {
            delivery_table.put(sender,seqno + 1);
            return true;
        }
        if(next_to_deliver == seqno) {
            delivery_table.put(sender,seqno + 1);
            return true;
        }
        return false;
    }

    protected long getNextToDeliver(Address sender) {
        Long next_to_deliver=delivery_table.get(sender);
        return next_to_deliver != null? next_to_deliver : -1;
    }

/* ----------------------------- End of Private Methods -------------------------------- */





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
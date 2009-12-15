
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.SeqnoTable;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Implementation of total order protocol using a sequencer. Consult doc/design/SEQUENCER.txt for details
 * @author Bela Ban
 * @version $Id: SEQUENCER.java,v 1.33 2009/12/15 12:49:26 belaban Exp $
 */
@Experimental
@MBean(description="Implementation of total order protocol using a sequencer")
public class SEQUENCER extends Protocol {
    private Address                    local_addr=null, coord=null;
    private final Collection<Address>  members=new ArrayList<Address>();
    private volatile boolean           is_coord=false;
    private AtomicLong                 seqno=new AtomicLong(0);

    /** Maintains messages forwarded to the coord which which no ack has been received yet.
     * Needs to be sorted so we resend them in the right order
     */
    private final Map<Long,byte[]> forward_table=new TreeMap<Long,byte[]>();

    /** Map<Address, seqno>: maintains the highest seqnos seen for a given member */
    private final SeqnoTable received_table=new SeqnoTable(0);

    private long forwarded_msgs=0;
    private long bcast_msgs=0;
    private long received_forwards=0;
    private long received_bcasts=0;

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

    @ManagedOperation
    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=0L;
    }

    @ManagedOperation
    public Map<String,Object> dumpStats() {
        Map<String,Object> m=super.dumpStats();       
        m.put("forwarded", new Long(forwarded_msgs));
        m.put("broadcast", new Long(bcast_msgs));
        m.put("received_forwards", new Long(received_forwards));
        m.put("received_bcasts", new Long(received_bcasts));
        return m;
    }

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }    


    
    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest == null || dest.isMulticastAddress()) { // only handle multicasts
                    long next_seqno=seqno.getAndIncrement();
                    if(is_coord) {
                        SequencerHeader hdr=new SequencerHeader(SequencerHeader.BCAST, local_addr, next_seqno);
                        msg.putHeader(name, hdr);
                        broadcast(msg, false); // don't copy, just use the message passed as argument
                    }
                    else {
                        // SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, local_addr, next_seqno);
                        // msg.putHeader(name, hdr);
                        forwardToCoord(msg, next_seqno);
                    }
                    return null; // don't pass down
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
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
                hdr=(SequencerHeader)msg.getHeader(name);
                if(hdr == null)
                    break; // pass up

                switch(hdr.type) {
                    case SequencerHeader.FORWARD:
                        if(!is_coord) {
                            if(log.isErrorEnabled())
                                log.error(local_addr + ": non-coord; dropping FORWARD request from " + msg.getSrc());
                            return null;
                        }
                        broadcast(msg, true); // do copy the message
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

            case Event.SUSPECT:
                handleSuspect((Address)evt.getArg());
                break;
        }

        return up_prot.up(evt);
    }



    /* --------------------------------- Private Methods ----------------------------------- */

    private void handleViewChange(View v) {
        Vector<Address> mbrs=v.getMembers();
        if(mbrs.isEmpty()) return;
        boolean coord_changed=false;

        synchronized(this) {
            members.clear();
            members.addAll(mbrs);
            Address prev_coord=coord;
            coord=mbrs.firstElement();
            is_coord=local_addr != null && local_addr.equals(coord);
            coord_changed=prev_coord != null && !prev_coord.equals(coord);
        }

        if(coord_changed) {
            resendMessagesInForwardTable(); // maybe optimize in the future: broadcast directly if coord
        }
        // remove left members from received_table
        received_table.retainAll(mbrs);
    }

    private void handleSuspect(Address suspected_mbr) {
        boolean coord_changed=false;

        if(suspected_mbr == null)
            return;
        
        synchronized(this) {
            List<Address> non_suspected_mbrs=new ArrayList<Address>(members);
            non_suspected_mbrs.remove(suspected_mbr);
            if(!non_suspected_mbrs.isEmpty()) {
                Address prev_coord=coord;
                coord=non_suspected_mbrs.get(0);
                is_coord=local_addr != null && local_addr.equals(coord);
                coord_changed=prev_coord != null && !prev_coord.equals(coord);
            }
        }
        if(coord_changed) {
            resendMessagesInForwardTable(); // maybe optimize in the future: broadcast directly if coord
        }
    }

    /**
     * Sends all messages currently in forward_table to the new coordinator (changing the dest field).
     * This needs to be done, so the underlying reliable unicast protocol (e.g. UNICAST) adds these messages
     * to its retransmission mechanism<br/>
     * Note that we need to resend the messages in order of their seqnos ! We also need to prevent other message
     * from being inserted until we're done, that's why there's synchronization.
     */
    private void resendMessagesInForwardTable() {
        Map<Long,byte[]> copy;
        synchronized(forward_table) {
            copy=new TreeMap<Long,byte[]>(forward_table);
        }
        for(Map.Entry<Long,byte[]> entry: copy.entrySet()) {
            Long key=entry.getKey();
            byte[] val=entry.getValue();

            Message forward_msg=new Message(coord, null, val);
            SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, local_addr, key);
            forward_msg.putHeader(name, hdr);

            if (log.isTraceEnabled()) {
                log.trace("resending msg " + local_addr + "::" + key + " to coord (" + coord + ")");
            }
            down_prot.down(new Event(Event.MSG, forward_msg));
        }
    }


    private void forwardToCoord(final Message msg, long seqno) {
        msg.setSrc(local_addr);
        if(log.isTraceEnabled())
            log.trace("forwarding msg " + msg + " (seqno " + seqno + ") to coord (" + coord + ")");

        byte[] marshalled_msg;
        try {
            marshalled_msg=Util.objectToByteBuffer(msg);
            synchronized(forward_table) {
                forward_table.put(seqno, marshalled_msg);
            }
            Message forward_msg=new Message(coord, null, marshalled_msg);
            SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, local_addr, seqno);
            forward_msg.putHeader(name, hdr);
            down_prot.down(new Event(Event.MSG, forward_msg));
            forwarded_msgs++;
        }
        catch(Exception e) {
            log.error("failed marshalling message", e);
        }
    }

    private void broadcast(final Message msg, boolean copy) {
        Message bcast_msg=null;
        final SequencerHeader hdr=(SequencerHeader)msg.getHeader(name);

        if(!copy) {
            bcast_msg=msg; // no need to add a header, message already has one
        }
        else {
            bcast_msg=new Message(null, local_addr, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            SequencerHeader new_hdr=new SequencerHeader(SequencerHeader.WRAPPED_BCAST, hdr.getOriginalSender(), hdr.getSeqno());
            bcast_msg.putHeader(name, new_hdr);
        }

        if(log.isTraceEnabled())
            log.trace("broadcasting msg " + bcast_msg + " (seqno " + hdr.getSeqno() + ")");

        down_prot.down(new Event(Event.MSG, bcast_msg));
        bcast_msgs++;
    }

    /**
     * Unmarshal the original message (in the payload) and then pass it up (unless already delivered)
     * @param msg
     */
    private void unwrapAndDeliver(final Message msg) {
        try {
            SequencerHeader hdr=(SequencerHeader)msg.getHeader(name);
            Message msg_to_deliver=(Message)Util.objectFromByteBuffer(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            long msg_seqno=hdr.getSeqno();
            if(!canDeliver(msg_to_deliver.getSrc(), msg_seqno))
                return;
            if(log.isTraceEnabled())
                log.trace("delivering msg " + msg_to_deliver + " (seqno " + msg_seqno +
                        "), original sender " + msg_to_deliver.getSrc());

            up_prot.up(new Event(Event.MSG, msg_to_deliver));
        }
        catch(Exception e) {
            log.error("failure unmarshalling buffer", e);
        }
    }


    private void deliver(Message msg, Event evt, SequencerHeader hdr) {
        Address sender=msg.getSrc();
        if(sender == null) {
            if(log.isErrorEnabled())
                log.error("sender is null, cannot deliver msg " + msg);
            return;
        }
        long msg_seqno=hdr.getSeqno();
        if(!canDeliver(sender, msg_seqno))
            return;
        if(log.isTraceEnabled())
            log.trace("delivering msg " + msg + " (seqno " + msg_seqno + "), sender " + sender);
        up_prot.up(evt);
    }


    private boolean canDeliver(Address sender, long seqno) {
        // this is the ack for the message sent by myself
        if(sender.equals(local_addr)) {
            synchronized(forward_table) {
                forward_table.remove(seqno);
            }
        }

        // if msg was already delivered, discard it
        boolean added=received_table.add(sender, seqno);
        if(!added) {
            if(log.isWarnEnabled())
                log.warn("seqno (" + sender + "::" + seqno + " has already been received " +
                        "(highest received=" + received_table.getHighestReceived(sender) +
                        "); discarding duplicate message");
        }
        return added;
    }

/* ----------------------------- End of Private Methods -------------------------------- */





    public static class SequencerHeader extends Header implements Streamable {
        private static final byte FORWARD       = 1;
        private static final byte BCAST         = 2;
        private static final byte WRAPPED_BCAST = 3;
        private static final long serialVersionUID=6181860771697205253L;

        byte    type=-1;
        /** the original sender's address and a seqno */
        ViewId  tag=null;


        public SequencerHeader() {
        }

        public SequencerHeader(byte type, Address original_sender, long seqno) {
            this.type=type;
            this.tag=new ViewId(original_sender, seqno);
        }

        public Address getOriginalSender() {
            return tag != null? tag.getCoordAddress() : null;
        }

        public long getSeqno() {
            return tag != null? tag.getId() : -1;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder(64);
            sb.append(printType());
            if(tag != null)
                sb.append(" (tag=").append(tag).append(")");
            return sb.toString();
        }

        private final String printType() {
            switch(type) {
                case FORWARD:        return "FORWARD";
                case BCAST:          return "BCAST";
                case WRAPPED_BCAST:  return "WRAPPED_BCAST";
                default:             return "n/a";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
            out.writeObject(tag);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
            tag=(ViewId)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
            Util.writeStreamable(tag, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
            tag=(ViewId)Util.readStreamable(ViewId.class, in);
        }

        public int size() {
            int size=Global.BYTE_SIZE *2; // type + presence byte
            if(tag != null)
                size+=tag.serializedSize();
            return size;
        }

    }


}
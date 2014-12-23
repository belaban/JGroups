package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Bits;
import org.jgroups.util.ForwardQueue;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Forwards a message to the current coordinator. When the coordinator changes, forwards all pending messages to
 * the new coordinator. Only looks at unicast messages.<p/>
 * Note that the ordering of messages sent in parallel to the resending of messages is currently (Sept 2012) undefined:
 * when resending messages 1-4, and concurrently sending (new) messages 5 and 6, then FORWARD_TO_COORD only guarantees
 * that messages [1,2,3,4] are delivered in that order and messages [5,6] are delivered in that order, too, but
 * there are no guarantees regarding the ordering between [1,2,3,4] and [5,6], e.g. a receiver could deliver
 * 1,5,2,3,6,4. <p/>
 * This is currently not an issue, as the main consumer of FORWARD_TO_COORD is RELAY2, which is used by Infinispan to
 * invoke sync or async RPCs across sites. In the former case, a unicast #2 will not be sent until unicast #1 is either
 * ack'ed or times out.<p/>
 * In a future version, ordering may be provided. Note though that OOB or UNRELIABLE messages don't need to be ordered.
 * @author Bela Ban
 * @since 3.2
 */
@MBean(description="Forwards unicast messages to the current coordinator")
public class FORWARD_TO_COORD extends Protocol {

    @Property(description="The delay (in ms) to wait until we resend a message to member P after P told us that " +
      "it isn't the coordinator. Thsi can happen when we see P as new coordinator, but P hasn't yet installed the view " +
      "which makes it coordinator (perhaps due to a slight delay)",deprecatedMessage="not used anymore, will be ignored")
    @Deprecated
    protected long                    resend_delay=500;

    /** the address of the current coordinator, all msgs are forwarded to it */
    protected volatile Address        coord=null;

    protected volatile Address        local_addr;

    /** ID to be used to identify forwarded messages. Wrap-around shouldn't be an issue. */
    protected final AtomicLong        current_id=new AtomicLong(0);

    protected final ForwardQueue      fwd_queue=new ForwardQueue(log);

    /** Set when NOT_COORD message has been received. This will trigger a fwd_queue flush even if the coord didn't change */
    protected volatile boolean        received_not_coord;




    public FORWARD_TO_COORD() {
    }


    @ManagedAttribute(description="Number of messages for which no ack has been received yet")
    public int           getForwardTableSize()  {return fwd_queue.size();}
    @ManagedAttribute(description="Total number of all seqnos maintained for all receivers")
    public int           getDeliveryTableSize() {return fwd_queue.deliveryTableSize();}
    public List<Integer> providedUpServices()   {return Arrays.asList(Event.FORWARD_TO_COORD);}

    public void start() throws Exception {
        super.start();
        received_not_coord=false;
        fwd_queue.setUpProt(up_prot);
        fwd_queue.setDownProt(down_prot);
        fwd_queue.start();
        log.setLevel("trace");
    }

    public void stop() {
        super.stop();
        fwd_queue.stop();
        coord=null;
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.FORWARD_TO_COORD:
                Address target=coord;
                if(target == null)
                    throw new IllegalStateException("coord is null; dropping message");
                Message msg=(Message)evt.getArg();
                long msg_id=getNextId();
                ForwardHeader hdr=new ForwardHeader(ForwardHeader.MSG, msg_id);
                msg.putHeader(id, hdr);
                msg.setDest(target);
                if(log.isTraceEnabled())
                    log.trace(local_addr + ": forwarding message with id=" + msg_id + " to current coordinator " + target);
                fwd_queue.send(msg_id, msg);
                return null; // FORWARD_TO_COORD is not passed down any further

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                fwd_queue.setLocalAddr(local_addr);
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                log.warn(local_addr + ": received message (up)");
                Message msg=(Message)evt.getArg();
                ForwardHeader hdr=(ForwardHeader)msg.getHeader(id);
                if(hdr == null)
                    break;
                long msg_id=hdr.getId();
                Address sender=msg.getSrc();
                switch(hdr.getType()) {
                    case ForwardHeader.MSG:
                        if(local_addr != null && !local_addr.equals(coord)) {
                            // I'm not the coord
                            if(log.isWarnEnabled())
                                log.warn(local_addr + ": received a message with id=" + msg_id + " from " + sender +
                                           ", but I'm not coordinator (" + coord + " is); dropping the message");
                            sendNotCoord(sender, msg_id);
                            return null;
                        }
                        try {
                            if(log.isTraceEnabled())
                                log.trace(local_addr + ": received a message with id=" + msg_id + " from " + sender);
                            fwd_queue.receive(msg_id, msg);
                            return null;
                        }
                        finally {
                            sendAck(sender, msg_id);
                        }
                    case ForwardHeader.ACK:
                        fwd_queue.ack(msg_id);
                        if(log.isTraceEnabled())
                            log.trace(local_addr + ": received an ack from " + sender + " for " + msg_id);
                        return null;
                    case ForwardHeader.NOT_COORD:
                        if(!received_not_coord)
                            received_not_coord=true;
                        return null;
                }
                break;
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }


    
    protected long getNextId() {return current_id.incrementAndGet();}

    protected void handleViewChange(View view) {
        Address new_coord=Util.getCoordinator(view);
        boolean coord_changed=coord == null || !coord.equals(new_coord);

        if(coord_changed || received_not_coord) {
            if(received_not_coord)
                received_not_coord=false;
            // resend all messages to the new coordinator (make copies)
            fwd_queue.flush(new_coord, view.getMembers());
            coord=new_coord;
        }
    }

    protected void sendAck(Address target, long ack_id) {
        send(target,ack_id,ForwardHeader.ACK);
    }

    protected void sendNotCoord(Address target, long ack_id) {
        send(target,ack_id,ForwardHeader.NOT_COORD);
    }

    protected void send(Address target, long ack_id, byte type) {
        down_prot.down(new Event(Event.MSG, new Message(target).putHeader(id, new ForwardHeader(type, ack_id))));
    }





    protected static class ForwardHeader extends Header {
        protected static final byte MSG       = 1; // attached to the messages to the coord
        protected static final byte ACK       = 2; // sent back by the coord
        protected static final byte NOT_COORD = 3; // sent by the coord when it is *not* the coord

        protected byte type;
        protected long id;


        public ForwardHeader() {
        }

        public ForwardHeader(byte type, long id) {
            this.type=type;
            this.id=id;
        }

        public long getId()   {return id;}
        public byte getType() {return type;}
        public int  size()    {return Global.BYTE_SIZE + Bits.size(id);}

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Bits.writeLong(id,out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            id=Bits.readLong(in);
        }

        public String toString() {
            return typeToString(type) + "(" + id + ")";
        }

        protected static String typeToString(byte type) {
            switch(type) {
                case MSG:       return "MSG";
                case ACK:       return "ACK";
                case NOT_COORD: return "NOT_COORD";
                default:        return "n/a";
            }
        }
    }
}

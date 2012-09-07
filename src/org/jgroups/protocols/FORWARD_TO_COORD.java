package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Forwards a message to the current coordinator. When the coordinator changes, forwards all pending messages to
 * the new coordinator. Only looks at unicast messages.
 * @author Bela Ban
 * @since 3.2
 */
@MBean(description="Forwards unicast messages to the current coordinator")
public class FORWARD_TO_COORD extends Protocol {
    protected final Map<Long,Message> msgs=new HashMap<Long,Message>();

    protected volatile Address        coord=null; // the address of the current coordinator, all msgs are forwarded to it

    protected volatile Address        local_addr;

    /** ID to be used to identify messages. Short.MAX_VALUE (ca 32K plus 32K negative) should be enough, and wrap-around
     * shouldn't be an issue. */
    protected long                    current_id=0;


    public FORWARD_TO_COORD() {
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.FORWARD_TO_COORD:
                Message msg=(Message)evt.getArg();
                Long tmp_id=getNextId();
                ForwardHeader hdr=new ForwardHeader(ForwardHeader.MSG,tmp_id);
                msg.putHeader(id, hdr);
                msg.setDest(coord);
                synchronized(msgs) {
                    msgs.put(tmp_id, msg);
                }
                return down_prot.down(new Event(Event.MSG, msg)); // FORWARD_TO_COORD is not pass down any further
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
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                ForwardHeader hdr=(ForwardHeader)msg.getHeader(id);
                if(hdr == null)
                    break;
                long tmp_id=hdr.getId();
                Address sender=msg.getSrc();
                switch(hdr.getType()) {
                    case ForwardHeader.MSG:
                        if(local_addr != null && !local_addr.equals(coord)) {
                            // I'm not the coord
                            sendNotCoord(sender, id);
                            return null;
                        }

                        try {
                            return up_prot.up(evt);
                        }
                        finally {
                            sendAck(sender, tmp_id);
                        }
                    case ForwardHeader.ACK:
                        synchronized(msgs) {
                            msgs.remove(tmp_id);
                        }
                        return null;
                    case ForwardHeader.NOT_COORD:
                        Message resend;
                        synchronized(msgs) {
                            resend=msgs.get(tmp_id);
                        }

                        if(resend != null) {
                            Message copy=resend.copy();
                            copy.setDest(coord);
                            down_prot.down(new Event(Event.MSG, resend));
                        }

                        return null;
                }
                break;
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }



    public void stop() {
        super.stop();
        msgs.clear();
        coord=null;
    }


    @ManagedAttribute(description="Number of messages for which no ack has been received yet")
    public int getPendingMessages() {
        synchronized(msgs) {
            return msgs.size();
        }
    }

    
    protected synchronized long getNextId() {return current_id++;}


    protected void handleViewChange(View view) {
        Address new_coord=Util.getCoordinator(view);
        boolean coord_changed=coord == null || !coord.equals(new_coord);
        coord=new_coord;

        if(coord_changed) {
            // resend all messages to the new coordinator (make copies)
            final Collection<Message> pending_msgs;
            synchronized(msgs) {
                pending_msgs=new LinkedList<Message>(msgs.values());
            }

            if(!pending_msgs.isEmpty()) {
                if(log.isTraceEnabled())
                    log.trace("sending " + pending_msgs.size() + " messages to new coordinator " + coord);
                for(Message msg: pending_msgs) {
                    Message copy=msg.copy();
                    copy.setDest(coord);
                    // the header with the ID is already attached (when sending the message the first time)
                    try {
                        down_prot.down(new Event(Event.MSG, copy)); // todo: do this in a separate task ?
                    }
                    catch(Throwable t) {
                        log.warn("failed resending message", t);
                    }
                }
            }
        }
    }

    protected void sendAck(Address target, long ack_id) {
        send(target,ack_id,ForwardHeader.ACK);
    }

    protected void sendNotCoord(Address target, long ack_id) {
        send(target,ack_id,ForwardHeader.NOT_COORD);
    }

    protected void send(Address target, long ack_id, byte type) {
        Message msg=new Message(target);
        ForwardHeader hdr=new ForwardHeader(type, ack_id);
        msg.putHeader(id, hdr);
        down_prot.down(new Event(Event.MSG, msg));
    }





    protected static class ForwardHeader extends Header {
        protected static final byte MSG       = 1; // attached to the messages to the coord
        protected static final byte ACK       = 2; // sent back by the coord
        protected static final byte NOT_COORD = 3; // sent by the coord when it is *not* the coord

        public ForwardHeader() {
        }

        public ForwardHeader(byte type, long id) {
            this.type=type;
            this.id=id;
        }

        protected byte type;
        protected long id;

        public long getId()   {return id;}
        public byte getType() {return type;}
        public int  size()    {return Global.BYTE_SIZE + Util.size(id);}

        public void writeTo(DataOutput out) throws Exception {
            out.writeByte(type);
            Util.writeLong(id, out);
        }

        public void readFrom(DataInput in) throws Exception {
            type=in.readByte();
            id=Util.readLong(in);
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

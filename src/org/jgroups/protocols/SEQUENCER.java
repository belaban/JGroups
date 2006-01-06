
package org.jgroups.protocols;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;
import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;


/**
 * Implementation of total order protocol using a sequencer. Consult doc/SEQUENCER.txt for details
 * @author Bela Ban
 * @version $Id: SEQUENCER.java,v 1.6 2006/01/06 08:44:54 belaban Exp $
 */
public class SEQUENCER extends Protocol {
    private Address                 local_addr=null, coord=null;
    static final String             name="SEQUENCER";
    private boolean                 is_coord=false;
    private final SynchronizedLong  seqno=new SynchronizedLong(0);

    private long forwarded_msgs=0;
    private long bcast_msgs=0;
    private long received_forwards=0;
    private long received_bcasts=0;

    public boolean isCoordinator() {return is_coord;}
    public Address getCoordinator() {return coord;}
    public Address getLocalAddress() {return local_addr;}
    public String getName() {return name;}
    public long getForwarded() {return forwarded_msgs;}
    public long getBroadcast() {return bcast_msgs;}
    public long getReceivedForwards() {return received_forwards;}
    public long getReceivedBroadcasts() {return received_bcasts;}

    public void resetStats() {
        forwarded_msgs=bcast_msgs=received_forwards=received_bcasts=0L;
    }

    public Map dumpStats() {
        Map m=super.dumpStats();
        if(m == null)
            m=new HashMap();
        m.put("forwarded", new Long(forwarded_msgs));
        m.put("broadcast", new Long(bcast_msgs));
        m.put("received_forwards", new Long(received_forwards));
        m.put("received_bcasts", new Long(received_bcasts));
        return m;
    }

    public String printStats() {
        return dumpStats().toString();
    }


    public boolean setProperties(Properties props) {
        super.setProperties(props);

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    private final long nextSeqno() {
        return seqno.increment();
    }


    public void down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                if(dest == null || dest.isMulticastAddress()) {
                    if(!is_coord) {
                        forwardToCoord(msg);
                    }
                    else {
                        broadcast(msg, local_addr);
                    }
                    return; // don't pass down
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }
        passDown(evt);
    }




    public void up(Event evt) {
        Message msg;
        SequencerHeader hdr;

        switch(evt.getType()) {

            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;

            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(SequencerHeader)msg.getHeader(name);
                if(hdr == null)
                    break;

                switch(hdr.type) {
                    case SequencerHeader.FORWARD:
                        broadcast(msg, msg.getSrc());
                        received_forwards++;
                        return;
                    case SequencerHeader.BCAST:
                        deliver(msg, hdr);
                        received_bcasts++;
                        return;
                }
                break;

            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }

        passUp(evt);
    }



    /* --------------------------------- Private Methods ----------------------------------- */

    private void handleViewChange(View v) {
        Vector members=v.getMembers();
        if(members.size() > 0) {
            coord=(Address)members.firstElement();
            is_coord=local_addr != null && local_addr.equals(coord);
        }
    }


    private void forwardToCoord(Message msg) {
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD, local_addr, nextSeqno());
        msg.putHeader(name, hdr);
        msg.setDest(coord);  // we change the message dest from multicast to unicast (to coord)
        passDown(new Event(Event.MSG, msg));
        forwarded_msgs++;
    }

    private void broadcast(Message msg, Address sender) {
        SequencerHeader hdr=(SequencerHeader)msg.getHeader(name);
        if(hdr == null) {
            hdr=new SequencerHeader(SequencerHeader.BCAST, sender, nextSeqno());
            msg.putHeader(name, hdr);
        }
        else {
            hdr.type=SequencerHeader.BCAST; // we change the type of header, but leave the tag intact
        }
        msg.setDest(null); // mcast
        msg.setSrc(local_addr); // the coord is sending it - this will be replaced with sender in deliver()
        passDown(new Event(Event.MSG, msg));
        bcast_msgs++;
    }

    /**
     * We copy the message in order to change the sender's address. If we did this on the original message,
     * retransmission would likely run into problems, and possibly also stability (STABLE) of messages
     * @param msg
     * @param hdr
     */
    private void deliver(Message msg, SequencerHeader hdr) {
        if(hdr.getOriginalSender() != null) {
            Message tmp=msg.copy(true);
            tmp.setSrc(hdr.getOriginalSender());
            passUp(new Event(Event.MSG, tmp));
        }
    }

    /* ----------------------------- End of Private Methods -------------------------------- */





    public static class SequencerHeader extends Header implements Streamable {
        static final byte FORWARD = 1;
        static final byte BCAST   = 2;

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
            StringBuffer sb=new StringBuffer(64);
            sb.append(printType());
            if(tag != null)
                sb.append(" (tag=").append(tag).append(")");
            return sb.toString();
        }

        private final String printType() {
            switch(type) {
                case FORWARD: return "FORWARD";
                case BCAST:   return "DATA";
                default:      return "n/a";
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

        public long size() {
            long size=Global.BYTE_SIZE *2; // type + presence byte
            if(tag != null)
                size+=tag.serializedSize();
            return size;
        }

    }


}
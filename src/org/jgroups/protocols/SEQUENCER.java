
package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Properties;
import java.util.Vector;


/**
 * Implementation of total order protocol using a sequencer. Consult doc/SEQUENCER.txt for details
 * @author Bela Ban
 * @version $Id: SEQUENCER.java,v 1.2 2005/12/30 16:35:42 belaban Exp $
 */
public class SEQUENCER extends Protocol {
    private Address     local_addr=null, coord=null;
    static final String name="SEQUENCER";
    private boolean     is_coord=false;


    public String getName() {
        return name;
    }


    public boolean setProperties(Properties props) {
        super.setProperties(props);

        if(props.size() > 0) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
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
                        return;
                    case SequencerHeader.DATA:
                        deliver(msg, hdr);
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
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.FORWARD);
        msg.putHeader(name, hdr);
        msg.setDest(coord);  // we change the message dest from multicast to unicast (to coord)
        passDown(new Event(Event.MSG, msg));
    }

    private void broadcast(Message msg, Address sender) {
        SequencerHeader hdr=new SequencerHeader(SequencerHeader.DATA, sender);
        msg.setDest(null); // mcast
        msg.setSrc(local_addr); // the coord is sending it - this will be replaced with sender in deliver()
        msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, msg));
    }

    private void deliver(Message msg, SequencerHeader hdr) {
        if(hdr.from != null) {
            msg.setSrc(hdr.from);
            passUp(new Event(Event.MSG, msg));
        }

    }

    /* ----------------------------- End of Private Methods -------------------------------- */





    public static class SequencerHeader extends Header implements Streamable {
        static final short FORWARD = 1;
        static final short DATA    = 2;

        short   type=-1;
        Address from=null;     // member who wants to verify that suspected_mbr is dead


        public SequencerHeader() {
        } // used for externalization

        SequencerHeader(short type) {
            this.type=type;
        }

        SequencerHeader(short type, Address from) {
            this(type);
            this.from=from;
        }

        public String toString() {
            StringBuffer sb=new StringBuffer(64);
            sb.append(printType());
            if(from != null)
                sb.append(" (from=").append(from).append(")");
            return sb.toString();
        }

        public String printType() {
            switch(type) {
                case FORWARD: return "FORWARD";
                case DATA:    return "DATA";
                default:      return "n/a";
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeShort(type);
            out.writeObject(from);
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readShort();
            from=(Address)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeShort(type);
            Util.writeAddress(from, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readShort();
            from=Util.readAddress(in);
        }

    }


}
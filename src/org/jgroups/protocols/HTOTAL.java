// $Id: HTOTAL.java,v 1.3 2005/08/11 12:43:47 belaban Exp $

package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Properties;
import java.util.Vector;


/**
 * Implementation of UTO-TCP as designed by EPFL. Implements chaining algorithm: each sender sends the message
 * to a coordinator who then forwards it to its neighbor on the right, who then forwards it to its neighbor to the right
 * etc.
 * @author Bela Ban
 * @version $Id: HTOTAL.java,v 1.3 2005/08/11 12:43:47 belaban Exp $
 */
public class HTOTAL extends Protocol {
    Address coord=null;
    Address neighbor=null; // to whom do we forward the message (member to the right, or null if we're at the tail)
    Address local_addr=null;
    Vector  mbrs=new Vector();
    boolean is_coord=false;
    private boolean use_multipoint_forwarding=false;


    public static class HTotalHeader extends Header implements Streamable {
        Address dest, src;
        boolean forward=true;

        public HTotalHeader() {
        }

        public HTotalHeader(Address dest, Address src) {
            this.dest=dest;
            this.src=src;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(dest);
            out.writeObject(src);
            out.writeBoolean(forward);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dest=(Address)in.readObject();
            src=(Address)in.readObject();
            forward=in.readBoolean();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeAddress(dest, out);
            Util.writeAddress(src, out);
            out.writeBoolean(forward);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            dest=Util.readAddress(in);
            src=Util.readAddress(in);
            forward=in.readBoolean();
        }

        public String toString() {
            return "dest=" + dest + ", src=" + src + ", forward=" + forward;
        }
    }

    public HTOTAL() {
    }

    public String getName() {
        return "HTOTAL";
    }

     public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("use_multipoint_forwarding");
        if(str != null) {
            use_multipoint_forwarding=Boolean.valueOf(str).booleanValue();
            props.remove("use_multipoint_forwarding");
        }

        if(props.size() > 0) {
            log.error("TCP.setProperties(): the following properties are not recognized: " + props);

            return false;
        }
        return true;
    }

    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.VIEW_CHANGE:
            determineCoordinatorAndNextMember((View)evt.getArg());
            break;
        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            if(dest == null || dest.isMulticastAddress()) { // only process multipoint messages
                if(coord == null)
                    log.error("coordinator is null, cannot send message to coordinator");
                else
                    forwardTo(coord, msg);
                return; // handled here, don't pass down by default
            }
            break;
        }
        passDown(evt);
    }

    public void up(Event evt) {
        switch(evt.getType()) {
        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
        case Event.VIEW_CHANGE:
            determineCoordinatorAndNextMember((View)evt.getArg());
            break;
        case Event.MSG:
            Message msg=(Message)evt.getArg();
            HTotalHeader hdr=(HTotalHeader)msg.getHeader(getName());

            if(hdr == null)
                break;  // probably a unicast message, just pass it up

            if(hdr.forward) {
                Message copy=msg.copy();
                if(use_multipoint_forwarding) {
                    copy.setDest(null);
                    passDown(new Event(Event.MSG, copy));
                }
                else {
                    if(neighbor != null) {
                        forwardTo(neighbor, copy);
                    }
                }
            }

            msg.setDest(hdr.dest); // set destination to be the original destination
            msg.setSrc(hdr.src);   // set sender to be the original sender (important for retransmission requests)

            passUp(evt); // <-- we modify msg directly inside evt
            return;
        }
        passUp(evt);
    }

    private void forwardTo(Address destination, Message msg) {
        HTotalHeader hdr=(HTotalHeader)msg.getHeader(getName());

        if(hdr == null) {
            hdr=new HTotalHeader(msg.getDest(), local_addr);
            msg.putHeader(getName(), hdr);
        }
        msg.setDest(destination);
        if(trace)
            log.trace("forwarding message to " + destination + ", hdr=" + hdr);
        passDown(new Event(Event.MSG, msg));
    }


    private void determineCoordinatorAndNextMember(View v) {
        Object tmp;
        Address retval=null;

        mbrs.clear();
        mbrs.addAll(v.getMembers());

        coord=(Address)(mbrs != null && mbrs.size() > 0? mbrs.firstElement() : null);
        is_coord=coord != null && local_addr != null && coord.equals(local_addr);

        if(mbrs == null || mbrs.size() < 2 || local_addr == null)
            neighbor=null;
        else {
            for(int i=0; i < mbrs.size(); i++) {
                tmp=mbrs.elementAt(i);
                if(local_addr.equals(tmp)) {
                    if(i + 1 >= mbrs.size())
                        retval=null; // we don't wrap, last member is null
                    else
                        retval=(Address)mbrs.elementAt(i + 1);
                    break;
                }
            }
        }
        neighbor=retval;
        if(trace)
            log.trace("coord=" + coord + ", neighbor=" + neighbor);
    }


}

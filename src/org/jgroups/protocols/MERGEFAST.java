package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.Experimental;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Vector;

/**
 * The coordinator attaches a small header to each (or every nth) message. If another coordinator <em>in the
 * same group</em> sees the message, it will initiate the merge protocol immediately by sending a MERGE
 * event up the stack.
 * @author Bela Ban, Aug 25 2003
 */
@Experimental
public class MERGEFAST extends Protocol {
    Address       local_addr=null;
    boolean       is_coord=false;
    static final String  name="MERGEFAST";

    public String getName() {
        return name;
    }


    public Object down(Event evt) {
        if(is_coord == true && evt.getType() == Event.MSG && local_addr != null) {
            Message msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            if(dest == null || dest.isMulticastAddress()) {
                msg.putHeader(getName(), new MergefastHeader(local_addr));
            }
        }

        if(evt.getType() == Event.VIEW_CHANGE) {
            handleViewChange((View)evt.getArg());
        }

        return down_prot.down(evt);
    }



    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.MSG:
                if(is_coord == false) // only handle message if we are coordinator
                    break;
                Message msg=(Message)evt.getArg();
                MergefastHeader hdr=(MergefastHeader)msg.getHeader(name);
                up_prot.up(evt);
                if(hdr != null && local_addr != null) {
                    Address other_coord=hdr.coord;
                    if(!local_addr.equals(other_coord)) {
                        sendUpMerge(new Address[]{local_addr, other_coord});
                    }
                }
                return null; // event was already passed up
            case Event.VIEW_CHANGE:
                handleViewChange((View)evt.getArg());
                break;
        }
        return up_prot.up(evt);
    }


    void handleViewChange(View v) {
        Vector mbrs;
        if(local_addr == null)
            return;
        mbrs=v.getMembers();
        is_coord=mbrs != null && !mbrs.isEmpty() && local_addr.equals(mbrs.firstElement());
    }

    /**
     * @todo avoid sending up too many MERGE events.
     */
    void sendUpMerge(Address[] addresses) {
        Vector v=new Vector(11);
        for(int i=0; i < addresses.length; i++) {
            Address addr=addresses[i];
            v.add(addr);
        }
        up_prot.up(new Event(Event.MERGE, v));
    }


    public static class MergefastHeader extends Header {
        Address coord=null;

        public MergefastHeader() {
        }

        public MergefastHeader(Address coord) {
            this.coord=coord;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(coord);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            coord=(Address)in.readObject();
        }

    }

}

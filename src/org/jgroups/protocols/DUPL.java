package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Address;

/** Duplicates outgoing or incoming messages by copying them
 * @author Bela Ban
 * @version $Id: DUPL.java,v 1.2 2008/06/06 08:06:26 belaban Exp $
 */
public class DUPL extends Protocol {

    private static enum Direction {UP,DOWN};


    @Property @ManagedAttribute(description="Number of copies of each incoming message (0=no copies)",writable=true)
    protected short incoming_copies=1;

    @Property @ManagedAttribute(description="Number of copies of each outgoing message (0=no copies)",writable=true)
    protected short outgoing_copies=1;

    @Property @ManagedAttribute(description="Whether or not to copy unicast messages",writable=true)
    protected boolean copy_unicast_msgs=true;

    @Property @ManagedAttribute(description="Whether or not to copy multicast messages",writable=true)
    protected boolean copy_multicast_msgs=true;

    public String getName() {
        return "DUPL";
    }


    public Object down(Event evt) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && outgoing_copies > 0;
        if(!copy)
            return down_prot.down(evt);

        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                copy(msg, outgoing_copies, Direction.DOWN);
                break;
        }

        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && incoming_copies > 0;
        if(!copy)
            return up_prot.up(evt);

        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                copy(msg, incoming_copies, Direction.UP);
                break;
        }

        return up_prot.up(evt);
    }

    private void copy(Message msg, int num_copies, Direction direction) {
        Address dest=msg.getDest();
        boolean multicast=dest == null || dest.isMulticastAddress();
        if((multicast && copy_multicast_msgs) ||  (!multicast && copy_unicast_msgs)) {
            for(int i=0; i < num_copies; i++) {
                Message copy=msg.copy(true);
                switch(direction) {
                    case UP:
                        up_prot.up(new Event(Event.MSG, copy));
                        break;
                    case DOWN:
                        down_prot.down(new Event(Event.MSG, copy));
                        break;
                }
            }
        }
    }
}

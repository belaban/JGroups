package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Address;

import java.util.Properties;

/** Duplicates outgoing or incoming messages by copying them
 * @author Bela Ban
 * @version $Id: DUPL.java,v 1.3.2.2 2008/06/09 09:41:19 belaban Exp $
 */
public class DUPL extends Protocol {
    private static enum Direction {UP,DOWN};

    protected int incoming_copies=1;

    protected int outgoing_copies=1;

    protected boolean copy_unicast_msgs=true;

    protected boolean copy_multicast_msgs=true;


    public DUPL() {
    }

    public DUPL(boolean copy_multicast_msgs, boolean copy_unicast_msgs, int incoming_copies, int outgoing_copies) {
        this.copy_multicast_msgs=copy_multicast_msgs;
        this.copy_unicast_msgs=copy_unicast_msgs;
        this.incoming_copies=incoming_copies;
        this.outgoing_copies=outgoing_copies;
    }

    public String getName() {
        return "DUPL";
    }

    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("incoming_copies");
        if(str != null) {
            incoming_copies=Integer.parseInt(str);
            props.remove("incoming_copies");
        }

        str=props.getProperty("outgoing_copies");
        if(str != null) {
            outgoing_copies=Integer.parseInt(str);
            props.remove("outgoing_copies");
        }

        str=props.getProperty("copy_unicast_msgs");
        if(str != null) {
            copy_unicast_msgs=Boolean.parseBoolean(str);
            props.remove("copy_unicast_msgs");
        }

        str=props.getProperty("copy_multicast_msgs");
        if(str != null) {
            copy_multicast_msgs=Boolean.parseBoolean(str);
            props.remove("copy_multicast_msgs");
        }

        if(!props.isEmpty()) {
            log.error("the following properties are not recognized: " + props);
            return false;
        }
        return true;
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

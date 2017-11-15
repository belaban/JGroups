package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import java.util.ArrayList;
import java.util.List;

/** Duplicates outgoing or incoming messages by copying them
 * @author Bela Ban
 */
public class DUPL extends Protocol {
    protected enum Direction {UP,DOWN};

    @Property(description="Number of copies of each incoming message (0=no copies)")
    protected int incoming_copies=1;

    @Property(description="Number of copies of each outgoing message (0=no copies)")
    protected int outgoing_copies=1;

    @Property(description="Whether or not to copy unicast messages")
    protected boolean copy_unicast_msgs=true;

    @Property(description="Whether or not to copy multicast messages")
    protected boolean copy_multicast_msgs=true;


    public DUPL() {
    }

    public DUPL(boolean copy_multicast_msgs, boolean copy_unicast_msgs, int incoming_copies, int outgoing_copies) {
        this.copy_multicast_msgs=copy_multicast_msgs;
        this.copy_unicast_msgs=copy_unicast_msgs;
        this.incoming_copies=incoming_copies;
        this.outgoing_copies=outgoing_copies;
    }


    public int getIncomingCopies() {
        return incoming_copies;
    }

    public void setIncomingCopies(int incoming_copies) {
        this.incoming_copies=incoming_copies;
    }

    public int getOutgoingCopies() {
        return outgoing_copies;
    }

    public void setOutgoingCopies(int outgoing_copies) {
        this.outgoing_copies=outgoing_copies;
    }

    public boolean isCopyUnicastMsgs() {
        return copy_unicast_msgs;
    }

    public void setCopyUnicastMsgs(boolean copy_unicast_msgs) {
        this.copy_unicast_msgs=copy_unicast_msgs;
    }

    public boolean isCopyMulticastMsgs() {
        return copy_multicast_msgs;
    }

    public void setCopyMulticastMsgs(boolean copy_multicast_msgs) {
        this.copy_multicast_msgs=copy_multicast_msgs;
    }

    public Object down(Message msg) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && outgoing_copies > 0;
        if(!copy)
            return down_prot.down(msg);

        copy(msg, outgoing_copies, Direction.DOWN);
        return down_prot.down(msg);
    }

    public Object up(Event evt) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && incoming_copies > 0;
        if(!copy)
            return up_prot.up(evt);
        return up_prot.up(evt);
    }


    public Object up(Message msg) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && incoming_copies > 0;
        if(!copy)
            return up_prot.up(msg);

        copy(msg, incoming_copies, Direction.UP);
        return up_prot.up(msg);
    }

    public void up(MessageBatch batch) {
        boolean copy=(copy_multicast_msgs || copy_unicast_msgs) && incoming_copies > 0;
        if(copy) {
            List<Message> copies=new ArrayList<>();
            for(Message msg: batch) {
                Address dest=msg.getDest();
                boolean multicast=dest == null;
                if((multicast && copy_multicast_msgs) ||  (!multicast && copy_unicast_msgs)) {
                    for(int i=0; i < incoming_copies; i++)
                        copies.add(msg.copy(true, true));
                }
            }
            copies.forEach(batch::add);
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    private void copy(Message msg, int num_copies, Direction direction) {
        Address dest=msg.getDest();
        boolean multicast=dest == null;
        if((multicast && copy_multicast_msgs) ||  (!multicast && copy_unicast_msgs)) {
            for(int i=0; i < num_copies; i++) {
                Message copy=msg.copy(true, true);
                switch(direction) {
                    case UP:
                        up_prot.up(copy);
                        break;
                    case DOWN:
                        down_prot.down(copy);
                        break;
                }
            }
        }
    }
}

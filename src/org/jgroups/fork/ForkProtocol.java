package org.jgroups.fork;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.FORK;
import org.jgroups.stack.Protocol;

import java.util.Map;

/**
 * Acts as bottom protool of a fork-stack. Knows about the fork-stack-id and inserts it into the ForkHeader of messages
 * sent down the stack
 * @author Bela Ban
 * @since  3.4
 */
public class ForkProtocol extends Protocol {
    protected final String       fork_stack_id;

    public ForkProtocol(String fork_stack_id) {
        this.fork_stack_id=fork_stack_id;
    }


    public Map<String,Object> dumpStats() {
        return null;
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                FORK.ForkHeader hdr=(FORK.ForkHeader)msg.getHeader(FORK.ID);
                if(hdr == null)
                    msg.putHeader(FORK.ID, hdr=new FORK.ForkHeader(fork_stack_id, null));
                else
                    hdr.setForkStackId(fork_stack_id);
                break;
            case Event.SET_LOCAL_ADDRESS:
            case Event.VIEW_CHANGE:
            case Event.CONNECT:
            case Event.CONNECT_USE_FLUSH:
            case Event.CONNECT_WITH_STATE_TRANSFER:
            case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
            case Event.DISCONNECT:
                return null; // don't propagate further down, this is only important for the main stack
        }
        return down_prot.down(evt);
    }
}

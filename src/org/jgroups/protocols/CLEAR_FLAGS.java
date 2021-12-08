package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

/**
 * Protocol which clears the given flags in the down direction for all messages
 * @author Bela Ban
 * @since  4.0.4
 */
public class CLEAR_FLAGS extends Protocol {
    @Property(description="clear OOB flags")
    protected boolean oob=true;

    @Override
    public Object down(Message msg) {
        if(oob)
            msg.clearFlag(Message.Flag.OOB);
        return down_prot.down(msg);
    }
}

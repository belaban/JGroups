package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.AbstractProtocol;

/**
 * Discards a message whose sequence number (in the payload, as a Long) matches seqno 2 times,
 * before passing it down. Used for unit testing
 * of OOB messages
 * @author Bela Ban
 */
@Unsupported
public class DISCARD_PAYLOAD extends AbstractProtocol {
    @Property protected long seqno=3; // drop 3
    @Property protected long duplicate=4; // duplicate 4 (one time)
    protected int            num_discards=0;

    public DISCARD_PAYLOAD() {
    }

    public Object down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            if(msg.getLength() > 0) {
                try {
                    Long payload=(Long)msg.getObject();
                    if(payload != null) {
                        if(payload == seqno) {
                            synchronized(this) {
                                if(num_discards < 3) {
                                    num_discards++;
                                    return null;
                                }
                            }
                        }
                        if(payload == duplicate) { // inject a duplicate message
                            super.down(evt); // pass it down, will passed down a second time by the default down_prot.down(evt)
                        }
                    }
                }
                catch(Throwable t) {
                    ;
                }
            }
        }
        return down_prot.down(evt);
    }
}

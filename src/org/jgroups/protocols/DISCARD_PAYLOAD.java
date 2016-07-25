package org.jgroups.protocols;

import org.jgroups.Message;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;

/**
 * Discards a message whose sequence number (in the payload, as a Long) matches seqno 2 times,
 * before passing it down. Used for unit testing
 * of OOB messages
 * @author Bela Ban
 */
@Unsupported
public class DISCARD_PAYLOAD extends Protocol {
    @Property protected long seqno=3; // drop 3
    @Property protected long duplicate=4; // duplicate 4 (one time)
    protected int            num_discards=0;

    public DISCARD_PAYLOAD() {
    }

    public Object down(Message msg) {
        if(msg.getLength() > 0) {
            try {
                Long payload=msg.getObject();
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
                        super.down(msg); // pass it down, will passed down a second time by the default down_prot.down(evt)
                    }
                }
            }
            catch(Throwable t) {
                ;
            }
        }
        return down_prot.down(msg);
    }
}

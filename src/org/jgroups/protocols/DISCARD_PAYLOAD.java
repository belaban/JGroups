package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.stack.Protocol;

import java.util.Properties;

/**
 * Discards a message whose sequence number (in the payload, as a Long) matches seqno 2 times,
 * before passing it up. Used for unit testing
 * of OOB messages
 * @author Bela Ban
 * @version $Id: DISCARD_PAYLOAD.java,v 1.6 2007/01/12 14:19:40 belaban Exp $
 */
public class DISCARD_PAYLOAD extends Protocol {
    long seqno=3; // drop 3
    long duplicate=4; // duplicate 4 (one time)
    int num_discards=0;

    public DISCARD_PAYLOAD() {
    }

    public String getName() {
        return "DISCARD_PAYLOAD";
    }

    public boolean setProperties(Properties props) {
        String     str;

        super.setProperties(props);

        str=props.getProperty("seqno");
        if(str != null) {
            seqno=Long.parseLong(str);
            props.remove("seqno");
        }

        str=props.getProperty("duplicate");
        if(str != null) {
            duplicate=Long.parseLong(str);
            props.remove("duplicate");
        }

        if(!props.isEmpty()) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public Object up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            if(msg.getLength() > 0) {
                try {
                    Long payload=(Long)msg.getObject();
                    if(payload != null) {
                        long val=payload.longValue();

                        if(val == seqno) {
                            synchronized(this) {
                                if(num_discards < 3) {
                                    num_discards++;
                                    return null;
                                }
                            }
                        }
                        if(val == duplicate) { // inject a duplicate message
                            super.up(evt); // pass it up, will passed up a second time by the default up_prot.up(evt)
                        }
                    }
                }
                catch(Throwable t) {
                    ;
                }
            }
        }
        return up_prot.up(evt);
    }
}

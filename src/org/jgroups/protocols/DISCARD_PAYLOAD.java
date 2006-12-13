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
 * @version $Id: DISCARD_PAYLOAD.java,v 1.2 2006/12/13 11:29:20 belaban Exp $
 */
public class DISCARD_PAYLOAD extends Protocol {
    long seqno=3;
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

        if(props.size() > 0) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }


    public void up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            if(msg.getLength() > 0) {
                try {
                    Long payload=(Long)msg.getObject();
                    if(payload != null && payload.longValue() == seqno) {
                        synchronized(this) {
                            if(num_discards < 3) {
                                num_discards++;
                                return;
                            }
                        }
                    }
                }
                catch(Throwable t) {
                    ;
                }
            }
        }
        super.up(evt);
    }
}

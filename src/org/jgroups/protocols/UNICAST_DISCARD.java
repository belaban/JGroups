package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.Address;

import java.util.Properties;

/**
 * Discards a UNICAST message whose sequence number (in the payload, as a Long) matches seqno 2 times,
 * before passing it up. Used for unit testing
 * of OOB messages
 * @author Bela Ban
 * @version $Id: UNICAST_DISCARD.java,v 1.2 2006/12/13 09:03:39 belaban Exp $
 */
public class UNICAST_DISCARD extends Protocol {
    long seqno=3;
    int num_discards=0;

    public UNICAST_DISCARD() {
    }

    public String getName() {
        return "UNICAST_DISCARD";
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
            Address dest=msg.getDest();
            if(dest != null && !dest.isMulticastAddress()) {
                Long payload=(Long)msg.getObject();
                if(payload != null && payload.longValue() == seqno) {
                    synchronized(this) {
                        if(num_discards < 3) {
                            System.out.println("num_discards=" + num_discards + ", discarding");
                            num_discards++;
                            return;
                        }
                        else {
                            System.out.println("num_discards=" + num_discards + ", passing up");
                        }
                    }
                }
            }
        }
        super.up(evt);
    }
}

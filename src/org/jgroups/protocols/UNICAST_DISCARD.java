package org.jgroups.protocols;

import org.jgroups.stack.Protocol;
import org.jgroups.Event;
import org.jgroups.Message;

import java.util.Properties;

/**
 * Discards a UNICAST message whose sequence number matches seqno 2 times, before passing it up. Used for unit testing
 * of OOB messages
 * @author Bela Ban
 * @version $Id: UNICAST_DISCARD.java,v 1.1 2006/12/13 08:00:25 belaban Exp $
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
            UNICAST.UnicastHeader hdr=(UNICAST.UnicastHeader)msg.getHeader("UNICAST");
            if(hdr != null) {
                if(hdr.seqno == seqno && num_discards++ < 3) {
                    return;
                }
            }
        }
        super.up(evt);
    }
}

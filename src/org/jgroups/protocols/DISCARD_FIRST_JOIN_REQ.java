package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;

/**
 * Discards 2 JOIN-REQs then accepts 1, then discards 2 more and so on
 * @author Bela Ban
 * @version $Id: DISCARD_FIRST_JOIN_REQ.java,v 1.1 2007/11/16 12:50:53 belaban Exp $
 */
public class DISCARD_FIRST_JOIN_REQ extends Protocol {
    int cnt=1;

    public String getName() {
        return "DISCARD_FIRST_JOIN_REQ";
    }


    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                GMS.GmsHeader hdr=(GMS.GmsHeader)msg.getHeader("GMS");
                if(hdr != null) {
                    switch(hdr.getType()) {
                        case GMS.GmsHeader.JOIN_REQ:
                        case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                            if(cnt++ >= 3) {
                                cnt=0;
                                System.out.println("accepted JOIN-REQ from " + hdr.getMember());
                                return up_prot.up(evt);
                            }
                            else {
                                System.out.println("discarded JOIN-REQ from " + hdr.getMember());
                                return null;
                            }
                    }
                }
                break;
        }
        return up_prot.up(evt);
    }
}

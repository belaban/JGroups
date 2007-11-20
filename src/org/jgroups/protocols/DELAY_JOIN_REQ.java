package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Date;
import java.util.Properties;

/**
 * Discards 2 JOIN-REQs then accepts 1, then discards 2 more and so on
 * @author Bela Ban
 * @version $Id: DELAY_JOIN_REQ.java,v 1.2.2.2 2007/11/20 08:37:24 belaban Exp $
 */
public class DELAY_JOIN_REQ extends Protocol {
    private long delay=4000;

    public String getName() {
        return "DELAY_JOIN_REQ";
    }

    public boolean setProperties(Properties props) {
        String     str;

        super.setProperties(props);

        str=props.getProperty("delay");
        if(str != null) {
            delay=Long.parseLong(str);
            props.remove("delay");
        }

        if(!props.isEmpty()) {
            log.error("these properties are not recognized: " + props);
            return false;
        }
        return true;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay=delay;
    }

    public Object up(final Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                final GMS.GmsHeader hdr=(GMS.GmsHeader)msg.getHeader("GMS");
                if(hdr != null) {
                    switch(hdr.getType()) {
                        case GMS.GmsHeader.JOIN_REQ:
                        case GMS.GmsHeader.JOIN_REQ_WITH_STATE_TRANSFER:
                            System.out.println(new Date() + ": delaying JOIN-REQ by " + delay + " ms");
                            Thread thread=new Thread() {
                                public void run() {
                                    Util.sleep(delay);
                                    System.out.println(new Date() + ": sending up delayed JOIN-REQ by " + hdr.getMember());
                                    up_prot.up(evt);
                                }
                            };
                            thread.start();
                            return null;
                    }
                }
                break;
        }
        return up_prot.up(evt);
    }
}

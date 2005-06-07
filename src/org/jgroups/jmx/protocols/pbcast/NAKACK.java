package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: NAKACK.java,v 1.2 2005/06/07 13:28:28 belaban Exp $
 */
public class NAKACK extends Protocol implements NAKACKMBean {
    org.jgroups.protocols.pbcast.NAKACK p;

    public NAKACK() {
    }

    public NAKACK(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.pbcast.NAKACK)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.pbcast.NAKACK)p;
    }

    public long getXmitRequestsReceived() {
        return p.getXmitRequestsReceived();
    }

    public long getXmitRequestsSent() {
        return p.getXmitRequestsSent();
    }

    public long getXmitResponsesReceived() {
        return p.getXmitResponsesReceived();
    }

    public long getXmitResponsesSent() {
        return p.getXmitResponsesSent();
    }
}

package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: PING.java,v 1.1 2005/06/08 15:17:30 belaban Exp $
 */
public class PING extends Discovery implements PINGMBean {
    org.jgroups.protocols.PING p;

    public PING() {
    }

    public PING(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.PING)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.PING)p;
    }
}

package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;

/**
 * @author Bela Ban
 * @version $Id: UDP.java,v 1.3 2005/08/17 07:32:29 belaban Exp $
 */
public class UDP extends org.jgroups.jmx.protocols.TP implements UDPMBean {
    org.jgroups.protocols.UDP udp;

    public UDP() {
    }

    public UDP(Protocol p) {
        super(p);
        udp=(org.jgroups.protocols.UDP)p;
    }

    public void attachProtocol(Protocol p) {
        super.attachProtocol(p);
        udp=(org.jgroups.protocols.UDP)p;
    }
}

package org.jgroups.jmx.protocols;

import org.jgroups.stack.Protocol;

/**
 * @author Bela Ban
 * @version $Id: UDP.java,v 1.1 2005/06/03 08:49:17 belaban Exp $
 */
public class UDP extends org.jgroups.jmx.Protocol implements UDPMBean {
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

    public long getNumMessagesSent() {
        return udp.getNumMessagesSent();
    }

    public long getNumMessagesReceived() {
        return udp.getNumMessagesReceived();
    }

    public long getNumBytesSent() {
        return udp.getNumBytesSent();
    }

    public long getNumBytesReceived() {
        return udp.getNumBytesReceived();
    }
}

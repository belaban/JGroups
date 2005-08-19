package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: UNICAST.java,v 1.4 2005/08/19 12:26:09 belaban Exp $
 */
public class UNICAST extends Protocol implements UNICASTMBean {
    org.jgroups.protocols.UNICAST p;

    public UNICAST() {
    }

    public UNICAST(org.jgroups.stack.Protocol p) {
        super(p);
        this.p=(org.jgroups.protocols.UNICAST)p;
    }

    public void attachProtocol(org.jgroups.stack.Protocol p) {
        super.attachProtocol(p);
        this.p=(org.jgroups.protocols.UNICAST)p;
    }

    public String getLocalAddress() {
        return p.getLocalAddress();
    }

    public String getMembers() {
        return p.getMembers();
    }

    public boolean isLoopback() {
        return p.isLoopback();
    }

    public void setLoopback(boolean loopback) {
        p.setLoopback(loopback);
    }

    public String printConnections() {
        return p.printConnections();
    }

    public long getNumMessagesSent() {
        return p.getNumMessagesSent();
    }

    public long getNumMessagesReceived() {
        return p.getNumMessagesReceived();
    }

    public long getNumBytesSent() {
        return p.getNumBytesSent();
    }

    public long getNumBytesReceived() {
        return p.getNumBytesReceived();
    }

    public long getNumAcksSent() {
        return p.getNumAcksSent();
    }

    public long getNumAcksReceived() {
        return p.getNumAcksReceived();
    }

    public long getNumberOfRetransmitRequestsReceived() {
        return p.getNumberOfRetransmitRequestsReceived();
    }

}

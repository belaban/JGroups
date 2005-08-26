package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: UNICAST.java,v 1.7 2005/08/26 14:19:09 belaban Exp $
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


    public String printConnections() {
        return p.printConnections();
    }

    public long getMessagesSent() {
        return p.getNumMessagesSent();
    }

    public long getMessagesReceived() {
        return p.getNumMessagesReceived();
    }

    public long getBytesSent() {
        return p.getNumBytesSent();
    }

    public long getBytesReceived() {
        return p.getNumBytesReceived();
    }

    public long getAcksSent() {
        return p.getNumAcksSent();
    }

    public long getAcksReceived() {
        return p.getNumAcksReceived();
    }

    public long getXmitRequestsReceived() {
        return p.getNumberOfRetransmitRequestsReceived();
    }

    public int getUnackedMessages() {
        return p.getNumberOfUnackedMessages();
    }

}

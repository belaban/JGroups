package org.jgroups.jmx.protocols;

import org.jgroups.jmx.Protocol;

/**
 * @author Bela Ban
 * @version $Id: UNICAST.java,v 1.8.4.3 2009/09/18 08:03:30 belaban Exp $
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

    public long getNumRetransmissions() {
        return p.getNumberOfRetransmissions();
    }

    public int getNumUnackedMessages() {
        return p.getNumberOfUnackedMessages();
    }

    public String getUnackedMessages() {
        return p.printUnackedMessages();
}

    public int getNumberOfMessagesInReceiveWindows() {
        return p.getNumberOfMessagesInReceiveWindows();
    }

    public int getAgeOutCacheSize() {
        return p.getAgeOutCacheSize();
    }

    public String printAgeOutCache() {
        return p.printAgeOutCache();
    }

}

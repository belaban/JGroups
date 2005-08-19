package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.4 2005/08/19 12:26:09 belaban Exp $
 */
public interface UNICASTMBean extends ProtocolMBean {
    String getLocalAddress();
    String getMembers();
    boolean isLoopback();
    void setLoopback(boolean loopback);
    String printConnections();
    long getNumMessagesSent();
    long getNumMessagesReceived();
    long getNumBytesSent();
    long getNumBytesReceived();
    long getNumAcksSent();
    long getNumAcksReceived();
    long getNumberOfRetransmitRequestsReceived();
}

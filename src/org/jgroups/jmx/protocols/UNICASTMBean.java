package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.6 2005/08/25 10:08:33 belaban Exp $
 */
public interface UNICASTMBean extends ProtocolMBean {
    String getLocalAddress();
    String getMembers();
    String printConnections();
    long getNumMessagesSent();
    long getNumMessagesReceived();
    long getNumBytesSent();
    long getNumBytesReceived();
    long getNumAcksSent();
    long getNumAcksReceived();
    long getNumberOfRetransmitRequestsReceived();
    int getNumberOfUnackedMessages();
}

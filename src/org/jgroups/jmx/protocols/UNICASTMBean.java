package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.3 2005/08/18 12:23:26 belaban Exp $
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
}

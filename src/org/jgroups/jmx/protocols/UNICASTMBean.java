package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.7 2005/08/26 14:19:09 belaban Exp $
 */
public interface UNICASTMBean extends ProtocolMBean {
    String getLocalAddress();
    String getMembers();
    String printConnections();
    long getMessagesSent();
    long getMessagesReceived();
    long getBytesSent();
    long getBytesReceived();
    long getAcksSent();
    long getAcksReceived();
    long getXmitRequestsReceived();
    int getUnackedMessages();
}

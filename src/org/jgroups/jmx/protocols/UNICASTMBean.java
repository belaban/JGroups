package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.9 2008/03/10 07:24:36 belaban Exp $
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
    int getNumUnackedMessages();
    String printUnackedMessages();
    int getNumberOfMessagesInReceiveWindows();
}

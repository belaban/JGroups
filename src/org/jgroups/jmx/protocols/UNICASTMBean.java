package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.8.4.1 2009/09/10 11:52:57 belaban Exp $
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
    long getNumRetransmissions();
    int getNumUnackedMessages();
    String getUnackedMessages();
    int getNumberOfMessagesInReceiveWindows();
}

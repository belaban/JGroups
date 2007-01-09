package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: SFCMBean.java,v 1.2 2007/01/09 10:19:22 belaban Exp $
 */
public interface SFCMBean extends ProtocolMBean {
    long getMaxCredits();
    long getCredits();
    long getBytesSent();
    long getBlockings();
    long getCreditRequestsReceived();
    long getCreditRequestsSent();
    long getReplenishmentsReceived();
    long getReplenishmentsSent();
    long getTotalBlockingTime();
    double getAverageBlockingTime();
    String printBlockingTimes();
    String printReceived();
    String printPendingCreditors();
    String printPendingRequesters();
    void unblock();
}

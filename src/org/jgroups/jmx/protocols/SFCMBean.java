package org.jgroups.jmx.protocols;

import org.jgroups.Address;
import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: SFCMBean.java,v 1.2.2.2 2007/04/18 03:18:43 bstansberry Exp $
 */
public interface SFCMBean extends ProtocolMBean {
    long getMulticastMaxCredits();
    long getUnicastMaxCredits();
    long getMulticastCredits();
    long getBytesSent();
    long getBlockings();
    long getCreditRequestsReceived();
    long getCreditRequestsSent();
    long getReplenishmentsReceived();
    long getReplenishmentsSent();
    long getTotalBlockingTime();
    double getAverageBlockingTime();
    long getCredits(Address peer);
    long getBytesSent(Address peer);
    long getBlockings(Address peer);
    long getCreditRequestsReceived(Address peer);
    long getCreditRequestsSent(Address peer);
    long getReplenishmentsReceived(Address peer);
    long getReplenishmentsSent(Address peer);
    long getTotalBlockingTime(Address peer);
    double getAverageBlockingTime(Address peer);
    String printBlockingTimes();
    String printReceived();
    String printPendingCreditors();
    String printPendingRequesters();
    void unblock();
}

package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: SFCMBean.java,v 1.1 2007/01/09 09:05:46 belaban Exp $
 */
public interface SFCMBean extends ProtocolMBean {
    long getMaxCredits();
    long getCredits();
    long getBlockings();
    long getReplenishments();
    long getTotalBlockingTime();
    double getAverageBlockingTime();
    String printBlockingTimes();
    String printReceived();
    String printPendingCreditors();
    String printPendingRequesters();
    void unblock();
}

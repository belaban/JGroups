package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FCMBean.java,v 1.3 2005/07/01 12:40:29 belaban Exp $
 */
public interface FCMBean extends ProtocolMBean {
    long getMaxCredits();
    void setMaxCredits(long max_credits);
    double getMinThreshold();
    void setMinThreshold(double min_threshold);
    long getMinCredits();
    void setMinCredits(long min_credits);
    boolean isBlocked();
    int getNumberOfBlockings();
    long getTotalTimeBlocked();
    double getAverageTimeBlocked();
    int getNumberOfReplenishmentsReceived();
    String printSenderCredits();
    String printReceiverCredits();
    String printCredits();
    String showLastBlockingTimes();
    void unblock();
}
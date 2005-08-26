package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FCMBean.java,v 1.6 2005/08/26 14:19:09 belaban Exp $
 */
public interface FCMBean extends ProtocolMBean {
    long getMaxCredits();
    void setMaxCredits(long max_credits);
    double getMinThreshold();
    void setMinThreshold(double min_threshold);
    long getMinCredits();
    void setMinCredits(long min_credits);
    boolean isBlocked();
    int getBlockings();
    long getTotalTimeBlocked();
    long getMaxBlockTime();
    void setMaxBlockTime(long t);
    double getAverageTimeBlocked();
    int getCreditRequestsReceived();
    int getCreditRequestsSent();
    int getCreditResponsesReceived();
    int getCreditResponsesSent();
    String printSenderCredits();
    String printReceiverCredits();
    String printCredits();
    String showLastBlockingTimes();
    void unblock();
}
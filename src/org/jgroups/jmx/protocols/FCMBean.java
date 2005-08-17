package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FCMBean.java,v 1.5 2005/08/17 06:07:41 belaban Exp $
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
    long getMaxBlockTime();
    void setMaxBlockTime(long t);
    double getAverageTimeBlocked();
    int getNumberOfCreditRequestsReceived();
    int getNumberOfCreditRequestsSent();
    int getNumberOfCreditResponsesReceived();
    int getNumberOfCreditResponsesSent();
    String printSenderCredits();
    String printReceiverCredits();
    String printCredits();
    String showLastBlockingTimes();
    void unblock();
}
package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FC2MBean.java,v 1.1.2.1 2007/04/18 05:18:09 bstansberry Exp $
 */
public interface FC2MBean extends ProtocolMBean {
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
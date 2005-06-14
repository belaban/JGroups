package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: FCMBean.java,v 1.2 2005/06/14 09:51:08 belaban Exp $
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
    int getNumberOfReplenishmentsReceived();
    String printSenderCredits();
    String printReceiverCredits();
    String printCredits();
    void unblock();
}
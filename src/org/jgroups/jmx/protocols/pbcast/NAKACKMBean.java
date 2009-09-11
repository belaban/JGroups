package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: NAKACKMBean.java,v 1.13.2.2 2009/09/11 12:09:18 belaban Exp $
 */
public interface NAKACKMBean extends ProtocolMBean {
    int getGcLag();
    void setGcLag(int gc_lag);
    boolean isUseMcastXmit();
    void setUseMcastXmit(boolean use_mcast_xmit);
    boolean isXmitFromRandomMember();
    void setXmitFromRandomMember(boolean xmit_from_random_member);
    boolean isDiscardDeliveredMsgs();
    void setDiscardDeliveredMsgs(boolean discard_delivered_msgs);
    int getMaxXmitBufSize();
    void setMaxXmitBufSize(int max_xmit_buf_size);

    /**
     *
     * @return
     * @deprecated removed in 2.6
     */
    long getMaxXmitSize();

    /**
     *
     * @param max_xmit_size
     * @deprecated removed in 2.6
     */
    void setMaxXmitSize(long max_xmit_size);
    int getXmitTableSize();
    long getXmitRequestsReceived();
    long getXmitRequestsSent();
    long getXmitResponsesReceived();
    long getXmitResponsesSent();
    long getMissingMessagesReceived();
    int getPendingRetransmissionRequests();
    long getUndeliveredMessages();
    String printXmitTable();
    String printMessages();
    String printStabilityMessages();
    String printMergeHistory();
    String printRetransmissionAvgs();
    String printRetransmissionTimes();
    String printSmoothedRetransmissionAvgs();
    String printLossRates();
    double getTotalAvgXmitTime();
    double getTotalAvgSmoothedXmitTime();
    int getAverageLossRate();
    int getAverageSmoothedLossRate();
}

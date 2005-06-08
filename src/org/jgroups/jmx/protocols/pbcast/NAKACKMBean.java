package org.jgroups.jmx.protocols.pbcast;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: NAKACKMBean.java,v 1.3 2005/06/08 12:36:41 belaban Exp $
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
    long getMaxXmitSize();
    void setMaxXmitSize(long max_xmit_size);

    long getXmitRequestsReceived();
    long getXmitRequestsSent();
    long getXmitResponsesReceived();
    long getXmitResponsesSent();
    long getMissingMessagesReceived();
    String printSentMessages();
    String printMessages();
}

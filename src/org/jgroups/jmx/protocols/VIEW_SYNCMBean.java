package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: VIEW_SYNCMBean.java,v 1.2.14.1 2008/03/12 09:01:42 belaban Exp $
 */
public interface VIEW_SYNCMBean extends ProtocolMBean {
    long getAverageSendInterval();
    void setAverageSendInterval(long send_interval);
    int getNumViewsSent();
    int getNumViewRequestsSent();
    int getNumViewResponsesSeen();
    int getNumViewsNonLocal();
    int getNumViewsEqual();
    int getNumViewsLess();
    long getLastViewRequestSent();
    int getNumViewsAdjusted();
    void sendViewRequest();
    // void sendFakeViewForTestingOnly();
}
